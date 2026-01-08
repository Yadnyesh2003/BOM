import polars as pl
from pathlib import Path

def read_csv(file_path: Path) -> pl.DataFrame:
    return pl.read_csv(file_path)

def read_inputs(base_path: Path):
    bom_df = read_csv(base_path / "input" / "BOM_Input.csv")
    so_df = read_csv(base_path / "input" / "OID_QTY_RP.csv")
    prod_df = read_csv(base_path / "input" / "Production_Report.csv")
    return bom_df, so_df, prod_df
