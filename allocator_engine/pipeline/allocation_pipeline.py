from io_modules.reader import read_csv
from pathlib import Path

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
                # base_path / "intermediate" / "order_allocation" / "oid_qty_rp.csv"
                base_path / "intermediate" / "oid_qty_rp.csv"
            )
            data["stock_df"] = read_csv(
                base_path / "intermediate" / "remaining_stock.csv"
                # base_path / "intermediate" / "order_allocation" / "remaining_stock.csv"
            )
            data["bom_df"] = read_csv(
                base_path / "intermediate" / "bom_input.csv"
                # base_path / "intermediate" / "order_allocation" / "remaining_stock.csv"
            )

        return data

    def _run_order_allocation(self, data):
        return data

    def _run_component_allocation(self, data):
        return data

    def _write_outputs(self, data):
        pass
