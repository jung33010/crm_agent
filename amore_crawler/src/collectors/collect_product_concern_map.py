import re
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yaml
from selenium.webdriver.common.by import By

from src.common.logger import get_logger
from src.common.selenium_driver import create_driver
from src.common.storage import load_latest_table, save_table, dedupe
from src.common.config import apply_sample

log = get_logger("collect_product_concern_map")

PROD_SN_RE = re.compile(r"[?&]onlineProdSn=(\d+)", re.IGNORECASE)

def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def now_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def extract_prod_sn(href: str) -> Optional[int]:
    if not href:
        return None
    m = PROD_SN_RE.search(href)
    return int(m.group(1)) if m else None

def uniq_keep_order(xs: List[int]) -> List[int]:
    seen = set()
    out = []
    for x in xs:
        if x is None:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def scroll_to_end(driver, max_rounds: int = 60, sleep: float = 1.0):
    """
    무한 스크롤/추가 로딩을 고려해 페이지 끝까지 내림.
    - scrollHeight가 3회 연속으로 변하지 않으면 종료.
    """
    last_h = 0
    stable = 0
    for _ in range(max_rounds):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(sleep)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            stable += 1
        else:
            stable = 0
            last_h = h
        if stable >= 3:
            break

def collect_prod_sns_from_list_page(driver, url: str) -> List[int]:
    driver.get(url)
    time.sleep(1.3)

    scroll_to_end(driver)

    # onlineProdSn이 들어간 링크만 긁기
    links = driver.find_elements(By.CSS_SELECTOR, "a[href*='onlineProdSn=']")
    prod_sns: List[int] = []
    for a in links:
        href = a.get_attribute("href") or ""
        sn = extract_prod_sn(href)
        if sn:
            prod_sns.append(sn)

    return uniq_keep_order(prod_sns)

def main():
    cfg = load_yaml("./config/concerns_filter_urls.yaml")
    brand = cfg.get("brand", "unknown")
    concerns: Dict[str, Dict] = cfg["concerns"]

    # QA 조인을 위해 기존 detail_urls_all 로드 (prod_sn 기준)
    detail_df = load_latest_table("./data/raw/detail_urls", "detail_urls_all")
    detail_df = apply_sample(detail_df, cfg)  # targets.yaml 샘플링과 동일한 컨벤션을 쓰고 있으면 유지

    valid_prod_sns = set(detail_df["prod_sn"].astype(int).tolist())

    driver = create_driver()

    rows = []
    collected_at = now_dt()

    try:
        for concern_type, meta in concerns.items():
            concern_name = meta["name"]
            url = meta["url"]

            prod_sns = collect_prod_sns_from_list_page(driver, url)
            log.info(f"concern={concern_type} scraped={len(prod_sns)}")

            # 우리 제품 목록(180개) 기준으로만 필터링
            prod_sns_in_scope = [sn for sn in prod_sns if sn in valid_prod_sns]
            log.info(f"concern={concern_type} in_scope={len(prod_sns_in_scope)}")

            for sn in prod_sns_in_scope:
                rows.append({
                    "brand": brand,
                    "prod_sn": int(sn),
                    "concern_type": concern_type,
                    "concern_name": concern_name,
                    "source_url": url,
                    "collected_at": collected_at,
                })

            time.sleep(0.2)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("product_concern_map empty. 셀렉터/스크롤 로직 또는 목록 페이지 구조를 점검하세요.")

    df = dedupe(df, ["prod_sn", "concern_type"])
    out = save_table(df, "./data/raw/product_concern_map", "product_concern_map")
    log.info(f"saved: {out} rows={len(df)}")

    # --------------------
    # QA 리포트 출력
    # --------------------
    mapped = df.groupby("prod_sn")["concern_type"].nunique()
    total = len(valid_prod_sns)
    mapped_products = mapped.index.nunique()
    log.info(f"QA: total_products={total} mapped_products={mapped_products} unmapped={total - mapped_products}")

    # 다중 concern 상품
    multi = mapped[mapped >= 2].sort_values(ascending=False)
    if len(multi) > 0:
        log.info(f"QA: multi_concern_products={len(multi)} top5={multi.head(5).to_dict()}")

    # 미매핑 상품 샘플
    unmapped = sorted(list(valid_prod_sns - set(df["prod_sn"].astype(int).tolist())))
    if unmapped:
        log.info(f"QA: unmapped_sample={unmapped[:10]}")

if __name__ == "__main__":
    main()
