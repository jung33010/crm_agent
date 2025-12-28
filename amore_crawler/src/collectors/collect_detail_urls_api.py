import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
import yaml

from src.common.logger import get_logger
from src.common.storage import save_table, dedupe
from src.common.config import apply_sample


log = get_logger("collect_detail_urls_api")

API_URL = "https://api-gw.amoremall.com/display/v2/M01/sis/online-products/by-brand"
DETAIL_URL_TMPL = "https://www.amoremall.com/kr/ko/product/detail?onlineProdSn={sn}&onlineProdCode={code}"
DETAIL_URL_TMPL_SN_ONLY = "https://www.amoremall.com/kr/ko/product/detail?onlineProdSn={sn}"

def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    

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

def request_page(session, headers, brand_sns, category_sns_all, limit, offset, sort_type):
    url = "https://api-gw.amoremall.com/display/v2/M01/sis/online-products/by-brand"

    brand_sn = brand_sns[0] if isinstance(brand_sns, list) else brand_sns

    params = {
        "brandSns": int(brand_sn),
        "containsFilter": "true",
        "limit": int(limit),
        "offset": int(offset),
        "sortType": sort_type,
    }

    # ✅ 핵심: 반복 query param으로 만들어지게 list로 넣는다
    if category_sns_all:
        params["categorySns"] = [int(x) for x in category_sns_all]

    r = session.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    cfg = load_yaml("./config/targets.yaml")

    brand = cfg.get("brand", "innisfree")
    # brand_sn은 단일 정수로 두는 게 보통이라, 어떤 키로 와도 흡수
    brand_sn = (
        cfg.get("brand_sn")
        or cfg.get("brandSn")
        or cfg.get("brandSNS")
        or cfg.get("brand_sns")  # 혹시 단일값으로 들어있을 수도
    )

    if isinstance(brand_sn, list):
        brand_sns = [int(x) for x in brand_sn]
    else:
        if brand_sn is None:
            raise KeyError("targets.yaml에 brand_sn(또는 brandSn) 키가 필요합니다. 예: brand_sn: 204")
        brand_sns = [int(brand_sn)]

    category_sns_all = cfg.get("category_sns_all", [])  # 전체 카테고리 sns 리스트
    limit = int(cfg.get("run", {}).get("limit", 40))
    sort_type = cfg.get("run", {}).get("sort_type", "Bestselling")

    session = requests.Session()
    headers = build_headers(cfg)

    rows = []
    offset = 0
    collected_at = now_dt()

    # URL 템플릿
    DETAIL_URL_TMPL = "https://www.amoremall.com/kr/ko/product/detail?onlineProdSn={sn}&onlineProdCode={code}"
    DETAIL_URL_TMPL_SN_ONLY = "https://www.amoremall.com/kr/ko/product/detail?onlineProdSn={sn}"

    # (디버그) 첫 페이지에서 payload 구조 확인
    payload0 = request_page(session, headers, brand_sns, category_sns_all, limit, offset, sort_type)
    log.info(f"[DEBUG] payload top keys: {list(payload0.keys())}")
    items0 = get_items(payload0)
    log.info(f"[DEBUG] first page items count: {len(items0)}")
    if items0:
        log.info(f"[DEBUG] first item keys: {list(items0[0].keys())}")

    # 첫 페이지 처리도 포함되도록 offset=0부터 루프
    offset = 0

    while True:
        payload = request_page(session, headers, brand_sns, category_sns_all, limit, offset, sort_type)
        items = get_items(payload)

        if not items:
            log.info("[INFO] no more items, stop paging")
            break

        for it in items:
            sn = it.get("onlineProdSn")
            if not sn:
                continue

            code = it.get("onlineProdCode")

            detail_url = (
                DETAIL_URL_TMPL.format(sn=int(sn), code=str(code))
                if code else
                DETAIL_URL_TMPL_SN_ONLY.format(sn=int(sn))
            )

            rows.append({
                "brand": brand,
                "prod_sn": int(sn),
                "online_prod_code": str(code) if code is not None else None,
                "detail_url": detail_url,
                "collected_at": collected_at,
            })

        offset += limit
        log.info(f"fetched offset={offset} rows={len(rows)}")
        time.sleep(0.15)

        # 안전장치
        if offset > 50000:
            log.warning("[WARN] offset too large, stop paging")
            break

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("detail_urls_all empty. API 응답 구조/파라미터 확인 필요")

    # ✅ prod_sn 기준 중복 제거 (API paging/정렬 변화 대비)
    df = dedupe(df, ["prod_sn"])

    out = save_table(df, "./data/raw/detail_urls", "detail_urls_all")
    log.info(f"saved: {out} rows={len(df)}")



def _find_first_list_of_dicts(obj):
    """dict/list 내부에서 'dict들의 list'를 재귀적으로 찾아 첫 번째를 반환."""
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            return obj
        for x in obj:
            found = _find_first_list_of_dicts(x)
            if found is not None:
                return found
    elif isinstance(obj, dict):
        for v in obj.values():
            found = _find_first_list_of_dicts(v)
            if found is not None:
                return found
    return None


def get_items(payload: dict) -> list[dict]:
    """
    응답 구조가 바뀌어도 작동하도록:
    - 흔한 후보 키를 우선 체크
    - 없으면 재귀 탐색으로 dict-list를 찾아 반환
    """
    if not isinstance(payload, dict):
        return []

    # 1) 자주 쓰는 경로 후보들
    candidates = [
        ("data", "list"),
        ("data", "items"),
        ("data", "products"),
        ("data", "onlineProducts"),
        ("data", "contents"),
        ("list",),
        ("items",),
        ("products",),
        ("onlineProducts",),
        ("contents",),
    ]

    for path in candidates:
        cur = payload
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, list) and (not cur or isinstance(cur[0], dict)):
            return cur

    # 2) 재귀 탐색 fallback
    found = _find_first_list_of_dicts(payload)
    return found or []

if __name__ == "__main__":
    main()
