from datetime import datetime
import pandas as pd

from src.common.logger import get_logger
from src.common.storage import load_latest_table, save_table

log = get_logger("derive_product_concern_final")

TOPK_PRED = 3  # 추정 concern 최대 개수

def now_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def main():
    # 전체 상품 기준
    detail = load_latest_table("./data/raw/detail_urls", "detail_urls_all")[["prod_sn"]]
    detail["prod_sn"] = detail["prod_sn"].astype(int)

    # 공식 concern
    official = load_latest_table("./data/derived/product_concerns", "product_concerns")
    official["prod_sn"] = official["prod_sn"].astype(int)

    # 추정 concern
    pred = load_latest_table("./data/derived/product_concern_pred_map", "product_concern_pred_map")
    pred["prod_sn"] = pred["prod_sn"].astype(int)

    rows = []
    created_at = now_dt()

    for _, r in detail.iterrows():
        prod_sn = int(r["prod_sn"])

        off = official[official["prod_sn"] == prod_sn]
        if not off.empty and len(off.iloc[0]["concerns"]) > 0:
            # 공식 concern 사용
            rows.append({
                "prod_sn": prod_sn,
                "source": "official",
                "concerns": off.iloc[0]["concerns"],
                "concern_types": off.iloc[0]["concern_types"],
                "created_at": created_at,
            })
            continue

        # 공식이 없으면 추정 concern 사용
        p = pred[pred["prod_sn"] == prod_sn].sort_values("rank").head(TOPK_PRED)
        if not p.empty:
            rows.append({
                "prod_sn": prod_sn,
                "source": "predicted",
                "concerns": p["concern_name"].tolist(),
                "concern_types": p["concern_type"].tolist(),
                "created_at": created_at,
            })
        else:
            # 둘 다 없으면 빈 리스트
            rows.append({
                "prod_sn": prod_sn,
                "source": "none",
                "concerns": [],
                "concern_types": [],
                "created_at": created_at,
            })

    out_df = pd.DataFrame(rows)

    out = save_table(out_df, "./data/derived/product_concern_final", "product_concern_final")
    log.info(f"saved: {out} rows={len(out_df)}")

    # QA
    log.info(
        "QA: "
        f"official={(out_df.source=='official').sum()} "
        f"predicted={(out_df.source=='predicted').sum()} "
        f"none={(out_df.source=='none').sum()}"
    )

if __name__ == "__main__":
    main()
