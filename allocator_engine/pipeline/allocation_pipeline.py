from io_modules.reader import read_csv
from io_modules.writer import write_csv
from pathlib import Path
import polars as pl
from pipeline.phase_registry import COMPONENT_ALLOCATORS
from pipeline.phase_registry import ORDER_ALLOCATORS
from common.stock_manager import StockManager
from common.bom_tree import BOMTree
from utils.schema_resolver import SchemaResolver

class AllocationPipeline:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger


    def run(self):
        phases = self.config["phases"]

        if not phases["order_allocation"]["enabled"] and \
        not phases["component_allocation"]["enabled"]:
            self.logger.error(
                "Invalid config: At least one phase must be enabled "
                "(order_allocation or component_allocation)"
            )
            return


        data = {}
        # -------- ORDER ALLOCATION --------
        if phases["order_allocation"]["enabled"]:
            alloc_type = phases["order_allocation"]["type"]
            allocator_cls = ORDER_ALLOCATORS[alloc_type]
            
            self._read_phase_inputs("order_allocation", allocator_cls, data)
            data = self._run_order_allocation(data)

        # -------- COMPONENT ALLOCATION --------
        if phases["component_allocation"]["enabled"]:
            alloc_type = phases["component_allocation"]["type"]
            allocator_cls = COMPONENT_ALLOCATORS[alloc_type]

            self._read_phase_inputs("component_allocation", allocator_cls, data)
            data = self._run_component_allocation(data)

        self._write_outputs(data)


    def _read_phase_inputs(self, phase_name: str, allocator_cls, data: dict) -> None:
        phase_cfg = self.config["phases"][phase_name]
        base_path = Path(self.config["base_path"])
        schemas = self.config["schemas"]

        input_root = base_path / phase_cfg["input_source"]
        csv_cfg = phase_cfg["csv_inputs"]

        required_schemas = allocator_cls.resolved_required_schemas()

        for src, cols in required_schemas.items():
            key = f"{src}_df"

            # DO NOT overwrite outputs from previous phases
            if key in data:
                continue

            # This phase does not provide this input
            if src not in csv_cfg:
                continue

            raw_df = read_csv(input_root / csv_cfg[src])

            data[key] = SchemaResolver.resolve(
                df=raw_df,
                schema_cfg=schemas[src],
                required_keys=cols,
                df_name=src.upper(),
                logger=self.logger
            )
            
        return data


    # -------- internal pipeline steps --------

    def _run_order_allocation(self, data):
        so_df = data["so_df"]
        stock_df = data["stock_df"]

        # Clean stock
        stock_df = stock_df.with_columns([
            pl.col("order_id").cast(pl.Utf8).str.strip_chars(),
            pl.col("item_id").cast(pl.Utf8).str.strip_chars(),
            pl.col("plant").cast(pl.Utf8).str.strip_chars(),
            pl.col("stock").fill_null(0).cast(pl.Float64)
        ])

        # SO-level FG stock
        so_stock_df = (
            stock_df
            .filter(pl.col("order_id").is_not_null() & (pl.col("order_id") != ""))
            .group_by(["order_id", "plant", "item_id"])
            .agg(pl.sum("stock").alias("stock"))
        )

        # ITEM-level FG stock
        item_stock_df = (
            stock_df
            .filter(pl.col("order_id").is_null() | (pl.col("order_id") == ""))
            .group_by(["plant", "item_id"])
            .agg(pl.sum("stock").alias("stock"))
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
            pl.col("root_parent").cast(pl.Utf8).str.strip_chars(),
            pl.col("plant").cast(pl.Utf8).str.strip_chars(),
            pl.col("parent").cast(pl.Utf8).str.strip_chars(),
            pl.col("child").cast(pl.Utf8).str.strip_chars(),
            pl.col("comp_qty").fill_null(0).cast(pl.Float64)
        ])
        stock_df = stock_df.with_columns([
            pl.col("order_id").cast(pl.Utf8).str.strip_chars(),
            pl.col("item_id").cast(pl.Utf8).str.strip_chars(),
            pl.col("plant").cast(pl.Utf8).str.strip_chars(),
            pl.col("stock").fill_null(0).cast(pl.Float64)
        ])

        # Aggregate stock
        so_stock_df = (
            stock_df
            .filter(pl.col("order_id").is_not_null() & (pl.col("order_id") != ""))
            .group_by(["order_id", "item_id", "plant"])
            .agg(pl.sum("stock").alias("stock"))
        )
        item_stock_df = (
            stock_df
            .filter(pl.col("order_id").is_null() | (pl.col("order_id") == ""))
            .group_by(["item_id", "plant"])
            .agg(pl.sum("stock").alias("stock"))
        )

        # Initialize StockManager & BOMTree
        stock_manager = StockManager()
        stock_manager.load_stock(so_stock_df, item_stock_df)
        bom_tree_obj = BOMTree(bom_df)

        # Choose allocator from config
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
        data["so_df"] = allocator.so_df
        data["component_allocation_df"] = output_df

        return data


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

            so_file = comp_out_dir / "orders_after_component_allocation.csv"
            write_csv(data["so_df"], so_file)

            print(f"Component allocation written to: {comp_file}")
