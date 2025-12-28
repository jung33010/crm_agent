import json
from datetime import datetime

import pandas as pd

from src.common.logger import get_logger
from src.common.storage import load_latest_table, save_table

log = get_logger("build_features")


def now_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_json_list(xs) -> str:
    """
    리스트를 JSON 문자열로 저장(Parquet/DB 적재 호환성 좋음)
    """
    return json.dumps(list(xs), ensure_ascii=False)


def _unique_preserve_order(seq):
    out = []
    for x in seq:
        if x is None:
            continue
        x = str(x).strip()
        if not x:
            continue
        if x not in out:
            out.append(x)
    return out


def build_category_aggregates(category_map: pd.DataFrame) -> pd.DataFrame:
    """
    category_map(prod_sn별 다중 row)을 products에 붙일 집계 형태로 변환.
    산출:
      - category_paths_all_json: ["스킨케어>클렌징>클렌징 폼", ...]
      - category_names_all_json: ["스킨케어","클렌징","클렌징 폼", ...] (depth1~3 flatten unique)
      - category_depth1_primary / category_depth2_primary / category_depth3_primary: 대표(첫 번째 path 기준)
    """
    required = {"prod_sn", "category_path", "category_depth1", "category_depth2", "category_depth3"}
    missing = required - set(category_map.columns)
    if missing:
        raise RuntimeError(f"category_map missing columns: {sorted(missing)}")

    # prod_sn 정리
    cm = category_map.copy()
    cm = cm.dropna(subset=["prod_sn"])
    cm["prod_sn"] = cm["prod_sn"].astype(int)

    # path/depth 문자열 정리
    for c in ["category_path", "category_depth1", "category_depth2", "category_depth3"]:
        cm[c] = cm[c].astype("string")

    # prod_sn별로 정렬 기준(있으면 collected_at 우선)
    if "collected_at" in cm.columns:
        cm["collected_at"] = cm["collected_at"].astype("string")
        cm = cm.sort_values(["prod_sn", "collected_at", "category_path"], na_position="last")
    else:
        cm = cm.sort_values(["prod_sn", "category_path"])

    def agg_paths(g: pd.DataFrame) -> list[str]:
        return _unique_preserve_order(g["category_path"].tolist())

    def agg_names(g: pd.DataFrame) -> list[str]:
        # depth1~3를 flatten한 후 unique
        vals = []
        vals += g["category_depth1"].tolist()
        vals += g["category_depth2"].tolist()
        vals += g["category_depth3"].tolist()
        return _unique_preserve_order(vals)

    def primary_depths(g: pd.DataFrame):
        # 첫 row 기준 대표 depth (ERD에서 products에 대표 카테고리 넣기로 한 경우 대비)
        first = g.iloc[0]
        d1 = str(first["category_depth1"]).strip() if pd.notna(first["category_depth1"]) else None
        d2 = str(first["category_depth2"]).strip() if pd.notna(first["category_depth2"]) else None
        d3 = str(first["category_depth3"]).strip() if pd.notna(first["category_depth3"]) else None
        return d1 or None, d2 or None, d3 or None

    grouped = cm.groupby("prod_sn", as_index=False)

    # 집계 생성
    paths_series = grouped.apply(lambda g: agg_paths(g), include_groups=False)
    names_series = grouped.apply(lambda g: agg_names(g), include_groups=False)
    prim_series = grouped.apply(lambda g: primary_depths(g), include_groups=False)

    # grouped.apply 결과가 Series(인덱스=prod_sn)로 나오므로 정리
    agg_df = pd.DataFrame({
        "prod_sn": paths_series.index.astype(int),
        "category_paths_all": paths_series.values,
        "category_names_all": names_series.values,
        "primary_depths": prim_series.values,
    })

    agg_df["category_paths_all_json"] = agg_df["category_paths_all"].apply(_to_json_list)
    agg_df["category_names_all_json"] = agg_df["category_names_all"].apply(_to_json_list)

    agg_df["category_depth1_primary"] = agg_df["primary_depths"].apply(lambda t: t[0] if t else None)
    agg_df["category_depth2_primary"] = agg_df["primary_depths"].apply(lambda t: t[1] if t else None)
    agg_df["category_depth3_primary"] = agg_df["primary_depths"].apply(lambda t: t[2] if t else None)

    agg_df = agg_df.drop(columns=["category_paths_all", "category_names_all", "primary_depths"])

    return agg_df


def main():
    # 1) 로드
    products = load_latest_table("./data/raw/products", "products")
    category_map = load_latest_table("./data/raw/category_map", "category_map")

    if products.empty:
        raise RuntimeError("products empty. collect_products 결과를 확인하세요.")
    if category_map.empty:
        log.warning("category_map empty. 카테고리 집계 없이 products_enriched 생성합니다.")

    # prod_sn 타입 강제
    if "prod_sn" not in products.columns:
        raise RuntimeError("products missing 'prod_sn' column. collect_products.py에서 prod_sn 저장 여부 확인 필요")

    products = products.copy()
    products = products.dropna(subset=["prod_sn"])
    products["prod_sn"] = products["prod_sn"].astype(int)

    # 2) 카테고리 집계
    if not category_map.empty:
        cat_agg = build_category_aggregates(category_map)

        # 3) 조인
        enriched = products.merge(cat_agg, on="prod_sn", how="left")
    else:
        enriched = products.copy()
        enriched["category_paths_all_json"] = None
        enriched["category_names_all_json"] = None
        enriched["category_depth1_primary"] = None
        enriched["category_depth2_primary"] = None
        enriched["category_depth3_primary"] = None

    # 4) 미매핑 fallback 정책
    # ERD/정책: 카테고리 매핑이 없는 상품은 '미분류'로 대표 depth1를 채우고,
    # paths/names는 빈 리스트로 둘지/미분류를 넣을지 선택 가능.
    # 여기서는 대표 depth1만 '미분류'로 채우고, 리스트는 빈 리스트로 채움(일관성 좋음).
    mask_no_cat = enriched["category_paths_all_json"].isna()

    enriched.loc[mask_no_cat, "category_depth1_primary"] = "미분류"
    enriched.loc[mask_no_cat, "category_depth2_primary"] = None
    enriched.loc[mask_no_cat, "category_depth3_primary"] = None
    enriched.loc[mask_no_cat, "category_paths_all_json"] = _to_json_list([])
    enriched.loc[mask_no_cat, "category_names_all_json"] = _to_json_list(["미분류"])

    # 5) metadata
    enriched["enriched_at"] = now_dt()

    # 6) 저장
    out = save_table(enriched, "./data/processed/products_enriched", "products_enriched")
    log.info(f"saved: {out} rows={len(enriched)}")


if __name__ == "__main__":
    main()
