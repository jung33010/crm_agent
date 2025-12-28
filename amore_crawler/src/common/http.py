import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .logger import get_logger

log = get_logger("http")

_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "15"))
_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))

class HttpClient:
    def __init__(self):
        self.sess = requests.Session()
        self.sess.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; amore-crawl/1.0)"
        })

    @retry(
        reraise=True,
        stop=stop_after_attempt(_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((requests.RequestException,)),
    )
    def get(self, url: str, **kwargs) -> requests.Response:
        timeout = kwargs.pop("timeout", _TIMEOUT)
        resp = self.sess.get(url, timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp
