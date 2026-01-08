from pathlib import Path
from io_modules.reader import read_csv
from io_modules.writer import write_csv
from core.stock_manager import StockManager
from core.bom_tree import BOMTree
from strategies.partial_allocation import PartialAllocator
from io_modules.config_reader import read_config
import polars as pl

# 1️⃣ Paths
BASE_PATH = Path(r"D:\000 VDL TESTING WORK\Polars_Alloc_Refactor")
CONFIG_PATH = BASE_PATH / "allocator_engine" / "config" / "allocation_config.yaml"

# 2️⃣ Load config
config = read_config(CONFIG_PATH)

# 3️⃣ Read input files from config
bom_df = read_csv(BASE_PATH / "input" / config["csv_inputs"]["bom"])
so_df = read_csv(BASE_PATH / "input" / config["csv_inputs"]["so"])
prod_df = read_csv(BASE_PATH / "input" / config["csv_inputs"]["production"])

# 4️⃣ Clean data
bom_df = bom_df.with_columns([
    pl.col("Finished_Good").cast(pl.Utf8).str.strip_chars(),
    pl.col("Parent").cast(pl.Utf8).str.strip_chars(),
    pl.col("Child").cast(pl.Utf8).str.strip_chars(),
    pl.col("BOM_Ratio_Of_Child").fill_null(0).cast(pl.Float64)
])
prod_df = prod_df.with_columns([
    pl.col("Child").cast(pl.Utf8).str.strip_chars(),
    pl.col("Order_ID").cast(pl.Utf8).str.strip_chars(),
    pl.col("Total_Stock").fill_null(0).cast(pl.Float64)
])

# 5️⃣ Aggregate stock
so_stock_df = (
    prod_df.filter(pl.col("Order_ID").is_not_null() & (pl.col("Order_ID") != ""))
    .group_by(["Order_ID", "Child"])
    .agg(pl.sum("Total_Stock").alias("Stock"))
)
item_stock_df = (
    prod_df.filter(pl.col("Order_ID").is_null() | (pl.col("Order_ID") == ""))
    .group_by("Child")
    .agg(pl.sum("Total_Stock").alias("Stock"))
)

# 6️⃣ Initialize stock manager and BOM
stock_manager = StockManager()
stock_manager.load_stock(so_stock_df, item_stock_df)
bom_tree_obj = BOMTree(bom_df)

# 7️⃣ Run allocation based on config
allocation_type = config.get("allocation_type", "partial")

if allocation_type == "partial":
    allocator = PartialAllocator(so_df, bom_tree_obj, stock_manager)
    output_df = allocator.allocate()
else:
    raise NotImplementedError(f"Allocation type {allocation_type} not implemented yet.")

# 8️⃣ Write output
output_file = BASE_PATH / "output" / config.get("output_file", "bom_output.csv")
write_csv(output_df, output_file)
