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
            self.logger.info("Order Allocation Phase started for %s Allocation", alloc_type.capitalize())
            
            self._read_phase_inputs("order_allocation", allocator_cls, data)
            data = self._run_order_allocation(data)

        # -------- COMPONENT ALLOCATION --------
        if phases["component_allocation"]["enabled"]:
            alloc_type = phases["component_allocation"]["type"]
            allocator_cls = COMPONENT_ALLOCATORS[alloc_type]
            self.logger.info("Component Allocation Phase started for %s Allocation", alloc_type.capitalize())

            self._read_phase_inputs("component_allocation", allocator_cls, data)
            data = self._run_component_allocation(data)

        self._write_outputs(data)


    def _read_phase_inputs(self, phase_name: str, allocator_cls, data: dict) -> None:
        self.logger.info("Reading Input Files...")
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
                df_name=f"{src.upper()} FILE",
                logger=self.logger
            )    
        self.logger.info("All Input Files Read Successfully")    
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
        self.logger.info("Stock Data Cleaned")

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
        self.logger.info("Stock Aggregation Completed.")
        stock_manager = StockManager(self.logger)
        stock_manager.load_stock(so_stock_df, item_stock_df)
        self.logger.info("Loaded Stock Data in Stock Manager.")

        alloc_type = self.config["phases"]["order_allocation"]["type"]
        allocator_cls = ORDER_ALLOCATORS.get(alloc_type)
        if not allocator_cls:
            self.logger.error("Unsupported Order Allocation type: %s", alloc_type)
            raise ValueError(f"Unsupported Order Allocation type: {alloc_type}")

        allocator = allocator_cls(so_df, stock_manager, logger=self.logger)
        self.logger.info("Running %s Order Allocation...", alloc_type.capitalize())
        updated_so_df, remaining_stock_df = allocator.allocate()
        self.logger.info("%s Order Allocation Completed.", alloc_type.capitalize())

        data["so_df"] = updated_so_df
        data["stock_df"] = remaining_stock_df

        self.logger.info("Updated SO and Stock Data Stored in Pipeline Data.")
        self.logger.info("Order Allocation Phase Completed.")

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

        self.logger.info("BOM & STOCK Data Cleaned.")

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

        self.logger.info("Stock Aggregation Completed.")

        # Initialize StockManager & BOMTree
        stock_manager = StockManager(self.logger)
        stock_manager.load_stock(so_stock_df, item_stock_df)
        self.logger.info("Loaded Stock Data in Stock Manager.")
        bom_tree_obj = BOMTree(bom_df)
        self.logger.info("BOMTree initialized successfully with %d BOM roots.",len(bom_tree_obj.bom_tree_map))


        # Choose allocator from config
        alloc_type = self.config["phases"]["component_allocation"]["type"]

        allocator_cls = COMPONENT_ALLOCATORS.get(alloc_type)
        if not allocator_cls:
            self.logger.error("Unsupported Component Allocation type: %s", alloc_type)
            raise ValueError(f"Unsupported Component Allocation type: {alloc_type}")

        allocator = allocator_cls(
            so_df,
            bom_tree_obj,
            stock_manager,
            logger=self.logger
        )
        self.logger.info("Running %s Partial Allocation...", alloc_type.capitalize())
        output_df = allocator.allocate()
        self.logger.info("%s Partial Allocation Completed.", alloc_type.capitalize())
        data["so_df"] = allocator.so_df
        data["component_allocation_df"] = output_df
        self.logger.info("Updated SO and Component Allocation Data")
        self.logger.info("Component Allocation Phase Completed.")
        return data


    def _write_outputs(self, data):
        try: 
            base_path = Path(self.config["base_path"])
            self.logger.info("Starting output write phase.")

            # ---------------- ORDER ALLOCATION OUTPUTS ----------------
            if self.config["phases"]["order_allocation"]["enabled"]:
                order_cfg = self.config["phases"]["order_allocation"]
                order_out_dir = base_path / order_cfg["output_path"]
                order_out_dir.mkdir(parents=True, exist_ok=True)
                self.logger.debug("Order allocation output directory ready: %s", order_out_dir)

                so_filename = order_cfg["csv_inputs"]["so"]
                so_file = order_out_dir / so_filename
                write_csv(data["so_df"], so_file)
                self.logger.info("Order allocation SO written: %s (rows=%d)", so_file, data["so_df"].height)

                stock_file = order_out_dir / order_cfg["csv_inputs"]["stock"]
                write_csv(data["stock_df"], stock_file)
                self.logger.info("Remaining stock written: %s (rows=%d)", stock_file, data["stock_df"].height)

            else:
                self.logger.info("Order allocation output skipped (phase disabled).")

            # ---------------- COMPONENT ALLOCATION OUTPUTS ----------------
            if self.config["phases"]["component_allocation"]["enabled"]:
                comp_cfg = self.config["phases"]["component_allocation"]
                comp_out_dir = base_path / comp_cfg["output_path"]
                comp_out_dir.mkdir(parents=True, exist_ok=True)
                self.logger.debug("Component allocation output directory ready: %s", comp_out_dir)

                comp_file = comp_out_dir / "component_allocation_output.csv"
                write_csv(data["component_allocation_df"], comp_file)
                self.logger.info("Component Allocation output written: %s (rows=%d)", comp_file, data["component_allocation_df"].height)

                so_file = comp_out_dir / "orders_after_component_allocation.csv"
                write_csv(data["so_df"], so_file)
                self.logger.info("SO Data after Component Allocation written: %s (rows=%d)", so_file, data["so_df"].height)
            
            else:
                self.logger.info("Component allocation output skipped (phase disabled).")

            self.logger.info("Output write phase completed.")

        except Exception as e:
            self.logger.critical("Failed to write output files: %s", str(e), exc_info=True)
            raise