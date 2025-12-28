from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
import yaml

from src.common.logger import get_logger
from src.common.storage import load_latest_table, save_table

log = get_logger("derive_product_concern_pred_map")

KEYWORDS_PATH = "./data/raw/product_keywords/product_keywords.parquet"
RULES_PATH = "./config/concern_pred_rules.yaml"

# 상품당 추정 concern 상한
TOPK_PER_PRODUCT = 3

def now_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def norm(s: str) -> str:
    return (s or "").strip().lower()

def score_product(keywords: List[str], rules: Dict[str, Any]):
    """
    키워드 리스트와 규칙을 비교해 concern별 점수 산출.
    - 부분일치 기반 (trigger in keyword)
    - score = 매칭 트리거 수 * weight
    """
    kws = [norm(k) for k in keywords if isinstance(k, str) and k.strip()]
    out = []
    for ctype, meta in rules.items():
        triggers = [norm(t) for t in meta.get("triggers", [])]
        w = float(meta.get("weight", 1.0))
        hits = []
        for t in triggers:
            if any(t in k for k in kws):
                hits.append(t)
        if hits:
            out.append({
                "concern_type": ctype,
                "concern_name": meta.get("name", ctype),
                "score": len(set(hits)) * w,
                "evidence_keywords": list(dict.fromkeys(hits))[:5],
            })
    out.sort(key=lambda x: x["score"], reverse=True)
    return out

def main():
    rules = load_yaml(RULES_PATH)

    # 전체 상품 목록(180개) 기준 (키워드 없는 상품도 포함시키기 위해)
    detail = load_latest_table("./data/raw/detail_urls", "detail_urls_all")[["prod_sn"]].copy()
    detail["prod_sn"] = detail["prod_sn"].astype(int)

    # OCR 키워드 로드
    kw_df = pd.read_parquet(KEYWORDS_PATH)
    kw_df["prod_sn"] = kw_df["prod_sn"].astype(int)

    # prod_sn 기준으로 병합(키워드 없는 상품 포함)
    base = detail.merge(kw_df[["prod_sn", "keywords"]], on="prod_sn", how="left")
    def coerce_keywords(x):
        if isinstance(x, list):
            return x
        if hasattr(x, "tolist"):     # numpy.ndarray 포함
            try:
                v = x.tolist()
                return v if isinstance(v, list) else []
            except Exception:
                return []
        if isinstance(x, tuple):
            return list(x)
        return []
    base["keywords"] = base["keywords"].apply(coerce_keywords)


    rows = []
    created_at = now_dt()

    for _, r in base.iterrows():
        prod_sn = int(r["prod_sn"])
        keywords = r["keywords"]

        scored = score_product(keywords, rules)
        if not scored:
            continue

        # top-k만 저장
        for rank, item in enumerate(scored[:TOPK_PER_PRODUCT], start=1):
            rows.append({
                "prod_sn": prod_sn,
                "concern_type": item["concern_type"],
                "concern_name": item["concern_name"],
                "rank": rank,
                "confidence": float(item["score"]),  # 점수 기반 (0~1 확률이 아니라 ranking용)
                "evidence_keywords": item["evidence_keywords"],
                "source": "ocr_keywords",
                "created_at": created_at,
            })

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        raise RuntimeError("product_concern_pred_map empty. 규칙/키워드 입력을 점검하세요.")

    # 중복 방지(prod_sn, concern_type)
    out_df = out_df.sort_values(["prod_sn", "rank"]).drop_duplicates(["prod_sn", "concern_type"], keep="first")

    out = save_table(out_df, "./data/derived/product_concern_pred_map", "product_concern_pred_map")
    log.info(f"saved: {out} rows={len(out_df)}")

    # QA
    mapped = out_df["prod_sn"].nunique()
    log.info(f"QA: total_products={len(detail)} predicted_mapped_products={mapped}")

if __name__ == "__main__":
    main()
