import os
from datetime import date
from pathlib import Path
import pandas as pd

def _run_date() -> str:
    rd = os.getenv("RUN_DATE")
    return rd if rd else date.today().isoformat()

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def save_table(df: pd.DataFrame, out_dir: str | Path, name_prefix: str):
    fmt = os.getenv("OUTPUT_FORMAT", "parquet").lower()
    run_date = _run_date()
    out_dir = ensure_dir(out_dir)

    if fmt == "csv":
        out = out_dir / f"{name_prefix}_{run_date}.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        return out

    out = out_dir / f"{name_prefix}_{run_date}.parquet"
    df.to_parquet(out, index=False)
    return out

def load_latest_table(in_dir: str | Path, prefix: str) -> pd.DataFrame:
    in_dir = Path(in_dir)
    files = sorted(in_dir.glob(f"{prefix}_*.parquet")) + sorted(in_dir.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"No files found in {in_dir} for prefix={prefix}")

    latest = files[-1]
    if latest.suffix == ".csv":
        return pd.read_csv(latest)
    return pd.read_parquet(latest)

def dedupe(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    return df.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)
