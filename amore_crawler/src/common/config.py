from __future__ import annotations
from pathlib import Path
import yaml
import pandas as pd

def load_yaml(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def apply_sample(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    sample = cfg.get("sample", {}) or {}
    if not sample.get("enabled"):
        return df

    n = int(sample.get("n_detail_urls", 0))
    if n <= 0:
        return df.iloc[0:0].copy()

    method = (sample.get("method") or "head").lower()
    seed = int(sample.get("seed", 42))

    if method == "random":
        return df.sample(n=min(n, len(df)), random_state=seed).reset_index(drop=True)

    return df.head(min(n, len(df))).reset_index(drop=True)
