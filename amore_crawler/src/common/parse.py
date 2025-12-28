import re
from urllib.parse import urlparse, parse_qs

_price_re = re.compile(r"[^0-9]")

def parse_price(text: str | None) -> int | None:
    if not text:
        return None
    digits = _price_re.sub("", text)
    return int(digits) if digits else None

def extract_query_param(url: str, key: str) -> str | None:
    try:
        q = parse_qs(urlparse(url).query)
        v = q.get(key)
        return v[0] if v else None
    except Exception:
        return None

def normalize_ws(text: str | None) -> str | None:
    if not text:
        return None
    return re.sub(r"\s+", " ", text).strip()
