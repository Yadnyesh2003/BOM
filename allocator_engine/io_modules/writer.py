import polars as pl
from pathlib import Path

def write_csv(df: pl.DataFrame, path: Path):
    df.write_csv(path)
