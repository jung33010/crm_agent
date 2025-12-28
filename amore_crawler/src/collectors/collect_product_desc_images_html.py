# src/collectors/collect_product_desc_images_html.py
import time
from datetime import datetime
from typing import Optional, List

import pandas as pd
import yaml
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.common.logger import get_logger
from src.common.selenium_driver import create_driver
from src.common.storage import load_latest_table, save_table, dedupe
from src.common.config import apply_sample

log = get_logger("collect_product_desc_images_html")

# ============================================================
# v1 베이스: 상세설명 영역 셀렉터
# ============================================================
DESC_SECTION = "#productDesc"
DESC_MORE_BTN = "#productDesc > section > div > div.article.productArea > div.prdImgWrap > button"

# v1에서 잘 잡히던 상세설명 이미지 셀렉터(전역 탐색 금지)
DESC_IMG_SELECTORS = [
    "div.contenteditor-htmlcode img",
    "#productDesc div.prdImgWrap img",
]

# (추가) 일부 케이스에서 htmlcode wrapper가 data-itemtype으로 붙는 경우
DESC_IMG_FALLBACK_SELECTORS = [
    '#productDesc div[data-itemtype="htmlcode"] img',
    "#productDesc .contenteditor-root img",   # root는 넓지만 #productDesc 내부로만 제한
]

# ============================================================
# 유틸
# ============================================================
def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def now_dt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def uniq_keep_order(xs: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in xs:
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def _scroll_to(driver, css: str, block: str = "center") -> bool:
    els = driver.find_elements(By.CSS_SELECTOR, css)
    if not els:
        return False
    driver.execute_script(f"arguments[0].scrollIntoView({{block:'{block}'}});", els[0])
    time.sleep(0.25)
    return True

def _click(driver, css: str) -> bool:
    els = driver.find_elements(By.CSS_SELECTOR, css)
    if not els:
        return False
    el = els[0]
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

def _get_img_url(img) -> Optional[str]:
    """
    v1 확장판:
    - src / data-src / data-origin 우선
    - 없으면 srcset 첫 항목
    """
    for attr in ("data-origin", "data-src", "src"):
        v = (img.get_attribute(attr) or "").strip()
        if v.startswith("http"):
            return v

    srcset = (img.get_attribute("srcset") or "").strip()
    if srcset:
        first = srcset.split(",")[0].strip().split(" ")[0]
        if first.startswith("http"):
            return first

    return None

def _count_imgs_in_desc_scope(driver) -> int:
    n = 0
    for sel in DESC_IMG_SELECTORS:
        try:
            n += len(driver.find_elements(By.CSS_SELECTOR, sel))
        except Exception:
            pass
    return n

def _debug_desc_dom(driver, prod_sn: int, online_prod_code: Optional[str]) -> None:
    """
    누락 원인 확인을 위해, 최소한의 DOM 신호만 로그로 남김.
    (페이지 소스 저장 같은 무거운 디버그는 여기서는 안 함)
    """
    has_desc = bool(driver.find_elements(By.CSS_SELECTOR, DESC_SECTION))
    n_editor = len(driver.find_elements(By.CSS_SELECTOR, "div.contenteditor-htmlcode"))
    n_imgs_a = len(driver.find_elements(By.CSS_SELECTOR, "div.contenteditor-htmlcode img"))
    n_imgs_b = len(driver.find_elements(By.CSS_SELECTOR, "#productDesc div.prdImgWrap img"))
    n_imgs_c = len(driver.find_elements(By.CSS_SELECTOR, '#productDesc div[data-itemtype="htmlcode"] img'))
    log.warning(
        f"debug(prod_sn={prod_sn}, code={online_prod_code}) "
        f"has#productDesc={has_desc} editor={n_editor} "
        f"imgs(htmlcode)={n_imgs_a} imgs(prdImgWrap)={n_imgs_b} imgs(itemtype)={n_imgs_c}"
    )

# ============================================================
# 핵심: v1 베이스 상세설명 이미지 수집
# ============================================================
def extract_desc_images_v1_base(
    driver,
    product_url: str,
    prod_sn: int,
    online_prod_code: Optional[str],
) -> List[str]:
    """
    v1 방식 그대로:
    - #productDesc로 이동
    - 더보기 클릭(있으면)
    - 스크롤/대기 반복하면서 상세설명 컨테이너 내부 img만 수집
    - 전역 img 수집 절대 금지
    """
    driver.get(product_url)

    # #productDesc가 나타날 때까지 대기(없으면 바로 실패)
    try:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, DESC_SECTION))
        )
    except Exception:
        return []

    # 상세설명 섹션으로 이동
    if not _scroll_to(driver, DESC_SECTION):
        return []

    # 더보기(있으면) 클릭
    _click(driver, DESC_MORE_BTN)
    time.sleep(0.9)

    urls: List[str] = []

    # lazy-load 대응: v1처럼 3회 반복 스크롤
    for _ in range(3):
        driver.execute_script("window.scrollBy(0, 1200);")
        time.sleep(0.9)

        imgs = []

        # v1 메인 셀렉터
        for sel in DESC_IMG_SELECTORS:
            try:
                imgs += driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                continue

        # 1차에서 없으면 fallback 셀렉터도 시도(그래도 #productDesc 내부로 제한)
        if not imgs:
            for sel in DESC_IMG_FALLBACK_SELECTORS:
                try:
                    imgs += driver.find_elements(By.CSS_SELECTOR, sel)
                except Exception:
                    continue

        for img in imgs:
            u = _get_img_url(img)
            if u:
                urls.append(u)

        # 중간에도 중복 제거
        urls = uniq_keep_order(urls)

        # 보통 1~3장. 1장이라도 잡히면 남은 스크롤을 과도하게 돌릴 필요 없음(과수집 방지)
        if len(urls) >= 3:
            break

    urls = uniq_keep_order(urls)

    # 과수집 방지: 이 범위에서 20장 이상이면 구조가 잘못 잡힌 것(대표/배너 섞임 가능)
    if len(urls) > 12:
        log.warning(f"too many desc imgs ({len(urls)}) prod_sn={prod_sn} -> trim")
        urls = urls[:12]

    # 디버그: 끝까지 못 잡으면 최소 DOM 신호 출력
    if not urls:
        _debug_desc_dom(driver, prod_sn, online_prod_code)

    return urls

# ============================================================
# main
# ============================================================
def main():
    cfg = load_yaml("./config/targets.yaml")

    # 입력 테이블: detail_urls_all
    detail_df = load_latest_table("./data/raw/detail_urls", "detail_urls_all")
    detail_df = apply_sample(detail_df, cfg)

    collected_at = now_dt()
    rows = []

    driver = create_driver()

    try:
        for _, r in detail_df.iterrows():
            prod_sn = int(r["prod_sn"])
            product_url = r["detail_url"]
            online_prod_code = r.get("online_prod_code")

            img_urls = []
            try:
                img_urls = extract_desc_images_v1_base(
                    driver=driver,
                    product_url=product_url,
                    prod_sn=prod_sn,
                    online_prod_code=online_prod_code,
                )
            except Exception as e:
                log.warning(f"selenium fail prod_sn={prod_sn} err={e}")
                img_urls = []

            if not img_urls:
                log.warning(f"no desc images prod_sn={prod_sn} onlineProdCode={online_prod_code}")
                continue

            for seq, img_url in enumerate(img_urls):
                rows.append(
                    {
                        "prod_sn": prod_sn,
                        "online_prod_code": online_prod_code,
                        "image_seq": seq,
                        "image_url": img_url,
                        "collected_at": collected_at,
                    }
                )

            time.sleep(0.05)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(
            "product_ocr_text empty. "
            "v1 베이스 셀렉터(#productDesc / contenteditor-htmlcode / prdImgWrap)를 점검하세요."
        )

    df = dedupe(df, ["prod_sn", "image_url"])
    out = save_table(df, "./data/raw/product_ocr_text", "product_ocr_text")
    log.info(f"saved: {out} rows={len(df)}")

if __name__ == "__main__":
    main()
