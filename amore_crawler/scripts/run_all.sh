#!/usr/bin/env bash
set -e

python -m src.collectors.collect_detail_urls
python -m src.collectors.collect_products
python -m src.collectors.collect_category_map
python -m src.collectors.collect_concern_map
python -m src.collectors.collect_product_ocr_text
python -m src.pipelines.validate
python -m src.pipelines.build_features
