from io_modules.reader import read_csv
from io_modules.writer import write_csv
from pathlib import Path
import polars as pl
from pipeline.phase_registry import COMPONENT_ALLOCATORS
from pipeline.phase_registry import ORDER_ALLOCATORS
from common.stock_manager import StockManager
from common.bom_tree import BOMTree
from core.component_allocation.strategies.partial import PartialComponentAllocator
from utils.logger import EngineLogger

class AllocationPipeline:
    def __init__(self, config, logger):
        self.config = config

    # def run(self):
    #     data = self._load_initial_inputs()
    #     if self.config["phases"]["order_allocation"]["enabled"]:
    #         data = self._run_order_allocation(data)
    #     if self.config["phases"]["component_allocation"]["enabled"]:
    #         data = self._run_component_allocation(data)
    #     self._write_outputs(data)
    def run(self):
        phases = self.config["phases"]

        if not phases["order_allocation"]["enabled"] and \
        not phases["component_allocation"]["enabled"]:
            raise ValueError(
                "Invalid config: At least one phase must be enabled "
                "(order_allocation or component_allocation)"
            )

        data = self._load_initial_inputs()

        if phases["order_allocation"]["enabled"]:
            data = self._run_order_allocation(data)

        if phases["component_allocation"]["enabled"]:
            data = self._run_component_allocation(data)

        self._write_outputs(data)


    def _read_phase_inputs(self, phase_name: str) -> dict:
        phase_cfg = self.config["phases"][phase_name]
        base_path = Path(self.config["base_path"])

        input_root = base_path / phase_cfg["input_source"]
        csv_cfg = phase_cfg["csv_inputs"]

        data = {}

        if "bom" in csv_cfg:
            data["bom_df"] = read_csv(input_root / csv_cfg["bom"])

        if "so" in csv_cfg:
            data["so_df"] = read_csv(input_root / csv_cfg["so"])

        if "stock" in csv_cfg:
            data["stock_df"] = read_csv(input_root / csv_cfg["stock"])

        return data


    # -------- internal pipeline steps --------

    def _load_initial_inputs(self):
        data = {}

        # Order Allocation inputs
        if self.config["phases"]["order_allocation"]["enabled"]:
            data.update(self._read_phase_inputs("order_allocation"))

        # Component Allocation inputs (only if OrderAlloc is disabled)
        elif self.config["phases"]["component_allocation"]["enabled"]:
            data.update(self._read_phase_inputs("component_allocation"))

        return data

    def _run_order_allocation(self, data):
        so_df = data["so_df"]
        stock_df = data["stock_df"]

        # Clean stock
        stock_df = stock_df.with_columns([
            pl.col("Child").cast(pl.Utf8).str.strip_chars(),
            pl.col("Plant").cast(pl.Utf8).str.strip_chars(),
            pl.col("Order_ID").cast(pl.Utf8).str.strip_chars(),
            pl.col("Stock").fill_null(0).cast(pl.Float64)
        ])

        # SO-level FG stock
        so_stock_df = (
            stock_df
            .filter(pl.col("Order_ID").is_not_null() & (pl.col("Order_ID") != ""))
            .group_by(["Order_ID", "Child", "Plant"])
            .agg(pl.sum("Stock").alias("Stock"))
        )

        # ITEM-level FG stock
        item_stock_df = (
            stock_df
            .filter(pl.col("Order_ID").is_null() | (pl.col("Order_ID") == ""))
            .group_by(["Child", "Plant"])
            .agg(pl.sum("Stock").alias("Stock"))
        )

        stock_manager = StockManager()
        stock_manager.load_stock(so_stock_df, item_stock_df)

        alloc_type = self.config["phases"]["order_allocation"]["type"]
        allocator_cls = ORDER_ALLOCATORS.get(alloc_type)

        allocator = allocator_cls(so_df, stock_manager)
        updated_so_df, remaining_stock_df = allocator.allocate()

        data["so_df"] = updated_so_df
        data["stock_df"] = remaining_stock_df

        return data

    def _run_component_allocation(self, data):
        """
        Component allocation phase
        Replicates the logic that previously lived in main.py
        """
        # Extract dataframes from pipeline data
        bom_df = data["bom_df"]
        so_df = data["so_df"]
        stock_df = data["stock_df"]

        # Clean data
        bom_df = bom_df.with_columns([
            pl.col("Finished_Good").cast(pl.Utf8).str.strip_chars(),
            pl.col("Plant").cast(pl.Utf8).str.strip_chars(),
            pl.col("Parent").cast(pl.Utf8).str.strip_chars(),
            pl.col("Child").cast(pl.Utf8).str.strip_chars(),
            pl.col("BOM_Ratio_Of_Child").fill_null(0).cast(pl.Float64)
        ])
        stock_df = stock_df.with_columns([
            pl.col("Child").cast(pl.Utf8).str.strip_chars(),
            pl.col("Plant").cast(pl.Utf8).str.strip_chars(),
            pl.col("Order_ID").cast(pl.Utf8).str.strip_chars(),
            pl.col("Stock").fill_null(0).cast(pl.Float64)
        ])

        # Aggregate stock
        so_stock_df = (
            stock_df
            .filter(pl.col("Order_ID").is_not_null() & (pl.col("Order_ID") != ""))
            .group_by(["Order_ID", "Child", "Plant"])
            .agg(pl.sum("Stock").alias("Stock"))
        )
        item_stock_df = (
            stock_df
            .filter(pl.col("Order_ID").is_null() | (pl.col("Order_ID") == ""))
            .group_by(["Child", "Plant"])
            .agg(pl.sum("Stock").alias("Stock"))
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


    # def _write_outputs(self, data):
    #     base_path = Path(self.config["base_path"])

    #     # ---------------- ORDER ALLOCATION OUTPUTS ----------------
    #     if self.config["phases"]["order_allocation"]["enabled"]:
    #         order_out_dir = base_path / self.config["phases"]["order_allocation"]["output_path"]
    #         order_out_dir.mkdir(parents=True, exist_ok=True)

    #         # SO output (remaining qty)
    #         so_file = order_out_dir / self.config["csv_inputs"]["so"]
    #         write_csv(data["so_df"], so_file)

    #         # Remaining stock
    #         stock_file = order_out_dir / "Remaining_Stock.csv"
    #         write_csv(data["stock_df"], stock_file)

    #         print(f"Order allocation SO written to: {so_file}")
    #         print(f"Remaining stock written to: {stock_file}")

    #     # ---------------- COMPONENT ALLOCATION OUTPUTS ----------------
    #     if self.config["phases"]["component_allocation"]["enabled"]:
    #         comp_out_dir = base_path / self.config["phases"]["component_allocation"]["output_path"]
    #         comp_out_dir.mkdir(parents=True, exist_ok=True)

    #         comp_file = comp_out_dir / "component_allocation_output.csv"
    #         write_csv(data["component_allocation_df"], comp_file)

    #         print(f"Component allocation written to: {comp_file}")

    def _write_outputs(self, data):
        base_path = Path(self.config["base_path"])

        # ---------------- ORDER ALLOCATION OUTPUTS ----------------
        if self.config["phases"]["order_allocation"]["enabled"]:
            order_cfg = self.config["phases"]["order_allocation"]
            order_out_dir = base_path / order_cfg["output_path"]
            order_out_dir.mkdir(parents=True, exist_ok=True)

            so_filename = order_cfg["csv_inputs"]["so"]
            so_file = order_out_dir / so_filename
            write_csv(data["so_df"], so_file)

            stock_file = order_out_dir / order_cfg["csv_inputs"]["stock"]
            write_csv(data["stock_df"], stock_file)

            print(f"Order allocation SO written to: {so_file}")
            print(f"Remaining stock written to: {stock_file}")

        # ---------------- COMPONENT ALLOCATION OUTPUTS ----------------
        if self.config["phases"]["component_allocation"]["enabled"]:
            comp_cfg = self.config["phases"]["component_allocation"]
            comp_out_dir = base_path / comp_cfg["output_path"]
            comp_out_dir.mkdir(parents=True, exist_ok=True)

            comp_file = comp_out_dir / "component_allocation_output.csv"
            write_csv(data["component_allocation_df"], comp_file)

            print(f"Component allocation written to: {comp_file}")
