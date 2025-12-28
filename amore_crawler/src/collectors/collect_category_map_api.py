import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
import yaml

from src.common.logger import get_logger
from src.common.storage import save_table, dedupe

log = get_logger("collect_category_map_api")

API_URL = "https://api-gw.amoremall.com/display/v2/M01/sis/online-products/by-brand"
DETAIL_URL_TMPL = "https://www.amoremall.com/kr/ko/product/detail?onlineProdSn={sn}"

# ✅ 대분류 후보 (depth1)
TOP_CATEGORIES = [
    "스킨케어", "메이크업", "향수", "생활용품",
    "소품&도구", "뷰티푸드", "남성",
    "베이비", "뷰티디바이스", "반려동물용품"
]

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

def request_page(session, headers, brand_sns, limit, offset):
    params = [
        ("brandSns", str(brand_sns)),
        ("limit", str(limit)),
        ("offset", str(offset)),
    ]
    url = f"{API_URL}?{urlencode(params)}"
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def get_items(payload: dict) -> list[dict]:
    return payload.get("products", [])

def normalize_depths(names: list[str]):
    if not names:
        return None, None, None

    d1 = None
    rest = []

    for n in names:
        if n in TOP_CATEGORIES and d1 is None:
            d1 = n
        else:
            rest.append(n)

    if d1 is None:
        d1 = names[0]
        rest = names[1:]

    if len(rest) == 0:
        return d1, None, None
    if len(rest) == 1:
        return d1, rest[0], None

    # depth3가 가장 구체적인 경우가 대부분
    return d1, rest[-1], rest[0]

def build_path(d1, d2, d3):
    return ">".join([x for x in [d1, d2, d3] if x])

def main():
    cfg = yaml.safe_load(open("./config/targets.yaml", "r", encoding="utf-8"))

    brand = cfg.get("brand", "innisfree")
    brand_sns = int(cfg["brand_entry"]["brand_sns"])
    limit = int(cfg.get("run", {}).get("api_limit", 40))

    headers = build_headers(cfg)
    session = requests.Session()

    rows = []
    collected_at = now_dt()

    offset = 0
    while True:
        payload = request_page(session, headers, brand_sns, limit, offset)
        items = get_items(payload)
        if not items:
            break

        for it in items:
            sn = it.get("onlineProdSn")
            if not sn:
                continue

            names = it.get("displayCateNames") or []
            sns = it.get("displayCategorySns") or []

            d1, d2, d3 = normalize_depths(names)
            path = build_path(d1, d2, d3)

            rows.append({
                "brand": brand,
                "prod_sn": int(sn),
                "category_depth1": d1,
                "category_depth2": d2,
                "category_depth3": d3,
                "category_path": path,
                "category_names_all": names,
                "category_sns_all": sns,
                "detail_url": DETAIL_URL_TMPL.format(sn=int(sn)),
                "collected_at": collected_at,
            })

        offset += limit
        log.info(f"fetched offset={offset} rows={len(rows)}")
        time.sleep(0.15)

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("category_map empty")

    df = dedupe(df, ["prod_sn", "category_path"])
    out = save_table(df, "./data/raw/category_map", "category_map")
    log.info(f"saved: {out} rows={len(df)}")

if __name__ == "__main__":
    main()
