# amore-crawl

아모레몰(예: 이니스프리) 제품 데이터를 수집/정제하여 다음 산출물을 생성.

## Outputs
- (1) data/raw/detail_urls/ : 상세 URL 목록
- (2) data/raw/products/ : 상품 상세 기반 제품 테이블 (Table1)
- (3) data/raw/category_map/ : 카테고리-상품 매핑 (Table2)
- (4) data/raw/concern_map/ : 피부고민-상품 매핑 (Table3)
- (4-2) data/raw/product_ocr_text/ : 이미지별 OCR 텍스트 (V2 ERD 반영)
- (5) data/processed/products_enriched/ : 조인/피벗된 최종 산출물

## V2 ERD Notes (현재 기준)
- product_ocr_text.image_seq 포함 (상품 이미지별 순번으로 OCR 텍스트 관리)
- line_discount / subtotal_amount 제거
- user_to_product_concern_map 제거
- orders는 total_amount만 유지(주문 크롤링은 본 레포 범위 밖, 스키마 일관성만 유지)

## Setup
```bash
cp .env.example .env
pip install -r requirements.txt
