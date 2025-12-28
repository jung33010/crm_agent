import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
import yaml

from src.common.logger import get_logger
from src.common.storage import save_table, dedupe
from src.common.config import apply_sample

log = get_logger("collect_products_api")

API_URL = "https://api-gw.amoremall.com/display/v2/M01/sis/online-products/by-brand"
DETAIL_URL_TMPL = "https://www.amoremall.com/kr/ko/product/detail?onlineProdSn={sn}"

def now_dt():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def build_headers(cfg: dict) -> dict:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ko",
        "origin": "https://www.amoremall.com",
        "referer": cfg["brand_entry"]["listing_url"],
        "user-agent": cfg.get("user_agent", "Mozilla/5.0"),
        "x-g1ecp-channel": "PCWeb",
        "x-g1ecp-cartnonmemberkey": cfg.get("x_headers", {}).get("cartnonmemberkey", ""),
    }

def request_page(session, headers, brand_sns, limit, offset, sort_type=None):
    params = [
        ("brandSns", str(brand_sns)),
        ("limit", str(limit)),
        ("offset", str(offset)),
    ]
    if sort_type:
        params.append(("sortType", str(sort_type)))

    url = f"{API_URL}?{urlencode(params)}"
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def get_items(payload: dict) -> list[dict]:
    # ✅ 방금 확인된 구조: payload["products"]
    return payload.get("products", [])

def main():
    cfg = yaml.safe_load(open("./config/targets.yaml", "r", encoding="utf-8"))

    brand = cfg.get("brand", "innisfree")
    brand_sns = int(cfg["brand_entry"]["brand_sns"])
    limit = int(cfg.get("run", {}).get("api_limit", 40))
    sort_type = cfg.get("run", {}).get("api_sort_type", None)

    headers = build_headers(cfg)
    session = requests.Session()

    rows = []
    collected_at = now_dt()

    offset = 0
    while True:
        payload = request_page(session, headers, brand_sns, limit, offset, sort_type)
        items = get_items(payload)
        if not items:
            log.info("[INFO] no more items, stop paging")
            break

        for it in items:
            sn = it.get("onlineProdSn")
            if not sn:
                continue

            rows.append({
                "prod_sn": int(sn),  # ✅ PK
                "brand": it.get("brandName") or brand,
                "product_name": it.get("onlineProdName"),
                "price": it.get("standardPrice"),
                "sale_price": it.get("discountedPrice"),
                "capacity": it.get("lineDesc"),
                "product_url": DETAIL_URL_TMPL.format(sn=int(sn)),
                "image_url": it.get("imgUrl"),
                "description": None,  # ✅ 상세에서 보강 가능
                "collected_at": collected_at,
            })

        offset += limit
        log.info(f"fetched offset={offset} rows={len(rows)}")
        time.sleep(0.15)

        if offset > 50000:
            log.warning("offset exceeded safety limit, stopping")
            break

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("products empty. API 응답/파라미터 확인 필요")

    df = dedupe(df, ["prod_sn"])
    df = apply_sample(df, cfg)  # 샘플링 옵션 유지

    out = save_table(df, "./data/raw/products", "products")
    log.info(f"saved: {out} rows={len(df)}")

if __name__ == "__main__":
    main()
