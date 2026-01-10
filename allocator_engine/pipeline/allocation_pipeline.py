from io_modules.reader import read_csv
from io_modules.writer import write_csv
from pathlib import Path
import polars as pl
from pipeline.phase_registry import COMPONENT_ALLOCATORS
from common.stock_manager import StockManager
from common.bom_tree import BOMTree
from core.component_allocation.strategies.partial import PartialComponentAllocator

class AllocationPipeline:
    def __init__(self, config):
        self.config = config

    def run(self):
        data = self._load_initial_inputs()
        if self.config["phases"]["order_allocation"]["enabled"]:
            data = self._run_order_allocation(data)
        if self.config["phases"]["component_allocation"]["enabled"]:
            data = self._run_component_allocation(data)
        self._write_outputs(data)

    # -------- internal pipeline steps --------

    def _load_initial_inputs(self):
        base_path = Path(self.config["base_path"])
        data = {}
        # BOM is always needed
        data["bom_df"] = read_csv(
            base_path / "input" / self.config["csv_inputs"]["bom"]
        )
        # Decide source of SO & stock based on phases
        if self.config["phases"]["order_allocation"]["enabled"]:
            data["so_df"] = read_csv(
                base_path / "input" / self.config["csv_inputs"]["so"]
            )
            data["stock_df"] = read_csv(
                base_path / "input" / self.config["csv_inputs"]["stock"]
            )
        else:
            # Phase 1 skipped → read from intermediate
            data["so_df"] = read_csv(
                base_path / "intermediate" / self.config["csv_inputs"]["so"]
            )
            data["stock_df"] = read_csv(
                base_path / "intermediate" / self.config["csv_inputs"]["stock"]
            )
            data["bom_df"] = read_csv(
                base_path / "intermediate" / self.config["csv_inputs"]["bom"]
            )
        return data

    def _run_order_allocation(self, data):
        print("No logic writtten here...................................")
        return data

    def _run_component_allocation(self, data):
        """
        Component allocation phase
        Replicates the logic that previously lived in main.py
        """
        # Extract dataframes from pipeline data
        bom_df = data["bom_df"]
        so_df = data["so_df"]
        prod_df = data["stock_df"]

        # Clean data
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

        # Aggregate stock
        so_stock_df = (
            prod_df
            .filter(pl.col("Order_ID").is_not_null() & (pl.col("Order_ID") != ""))
            .group_by(["Order_ID", "Child"])
            .agg(pl.sum("Total_Stock").alias("Stock"))
        )
        item_stock_df = (
            prod_df
            .filter(pl.col("Order_ID").is_null() | (pl.col("Order_ID") == ""))
            .group_by("Child")
            .agg(pl.sum("Total_Stock").alias("Stock"))
        )

        # Initialize StockManager & BOMTree
        stock_manager = StockManager()
        stock_manager.load_stock(so_stock_df, item_stock_df)
        bom_tree_obj = BOMTree(bom_df)

        # Choose allocator from config
        # allocation_type = self.config.get("allocation_type", "partial")
        # if allocation_type == "partial":
        #     allocator = PartialComponentAllocator(
        #         so_df,
        #         bom_tree_obj,
        #         stock_manager
        #     )
        #     output_df = allocator.allocate()
        
        alloc_type = self.config["phases"]["component_allocation"]["type"]

        allocator_cls = COMPONENT_ALLOCATORS.get(alloc_type)
        if not allocator_cls:
            raise ValueError(f"Unsupported component allocation type: {alloc_type}")

        allocator = allocator_cls(
            so_df,
            bom_tree_obj,
            stock_manager
        )

        output_df = allocator.allocate()

        # else:
        #     raise NotImplementedError(
        #         f"Allocation type '{allocation_type}' not implemented"
        #     )
        #  Store output in pipeline data
        data["component_allocation_df"] = output_df

        return data


    def _write_outputs(self, data):
        base_path = Path(self.config["base_path"])
        output_dir = base_path / self.config["phases"]["component_allocation"]["output_path"]
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / "component_allocation_output.csv"

        write_csv(data["component_allocation_df"], output_file)

        print(f"✅ Component allocation written to: {output_file}")