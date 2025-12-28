import pandas as pd
from src.common.logger import get_logger
from src.common.storage import load_latest_table

log = get_logger("validate")

def check_required(df: pd.DataFrame, required: list[str], name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{name}] missing columns: {missing}")

def main():
    products = load_latest_table("./data/raw/products", "products")
    check_required(products, ["product_url", "product_name"], "products")

    category_map = load_latest_table("./data/raw/category_map", "category_map")
    check_required(category_map, ["product_url", "category_name"], "category_map")

    # concern_map은 placeholder로 비어있을 수 있어 컬럼 존재만 확인
    concern_map = load_latest_table("./data/raw/concern_map", "concern_map")
    check_required(concern_map, ["product_url", "concern_name"], "concern_map")

    ocr = load_latest_table("./data/raw/product_ocr_text", "product_ocr_text")
    check_required(ocr, ["product_url", "image_seq", "image_url", "ocr_text"], "product_ocr_text")

    log.info(f"products rows={len(products)} null(product_name)={products['product_name'].isna().mean():.2%}")
    log.info(f"category_map rows={len(category_map)}")
    log.info(f"concern_map rows={len(concern_map)}")
    log.info(f"product_ocr_text rows={len(ocr)}")

if __name__ == "__main__":
    main()
