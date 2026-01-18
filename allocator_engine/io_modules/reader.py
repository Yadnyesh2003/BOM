import polars as pl
from pathlib import Path

def read_csv(file_path: Path, logger=None):
    try:
        return pl.read_csv(file_path)
    except Exception:
        if logger:
            logger.error(f"Failed to read CSV: {file_path}", exc_info=True)
        raise
