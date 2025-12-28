from datetime import datetime

import pandas as pd

from src.common.logger import get_logger
from src.common.storage import load_latest_table, save_table

log = get_logger("derive_product_concerns")

def now_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    # 공식 concern 매핑 로드
    m = load_latest_table("./data/raw/product_concern_map", "product_concern_map")
    m["prod_sn"] = m["prod_sn"].astype(int)

    # 전체 상품 목록(180개) 기준으로 빈 리스트까지 포함시키기 위해 detail_urls_all 로드
    detail = load_latest_table("./data/raw/detail_urls", "detail_urls_all")
    all_prod = detail[["prod_sn"]].copy()
    all_prod["prod_sn"] = all_prod["prod_sn"].astype(int)

    # 상품별 concern 집계 (공식)
    agg = (
        m.sort_values(["prod_sn", "concern_type"])
         .groupby("prod_sn", as_index=False)
         .agg(
            concern_types=("concern_type", lambda x: list(dict.fromkeys(x.tolist()))),
            concerns=("concern_name", lambda x: list(dict.fromkeys(x.tolist()))),
         )
    )

    # 전체 상품에 left join → concern 없는 상품도 포함
    out_df = all_prod.merge(agg, on="prod_sn", how="left")

    # NaN을 빈 리스트로 치환
    out_df["concern_types"] = out_df["concern_types"].apply(lambda x: x if isinstance(x, list) else [])
    out_df["concerns"] = out_df["concerns"].apply(lambda x: x if isinstance(x, list) else [])

    out_df["collected_at"] = now_dt()

    out = save_table(out_df, "./data/derived/product_concerns", "product_concerns")
    log.info(f"saved: {out} rows={len(out_df)}")

    # QA
    n_with = (out_df["concerns"].apply(len) > 0).sum()
    log.info(f"QA: total={len(out_df)} with_concern={n_with} without={len(out_df) - n_with}")

if __name__ == "__main__":
    main()
