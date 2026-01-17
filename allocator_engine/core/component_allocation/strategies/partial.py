import polars as pl
from collections import deque
import logging

from core.component_allocation.base_component_allocator import BaseComponentAllocator

logger = logging.getLogger(__name__)

class PartialComponentAllocator(BaseComponentAllocator):
    """
    Partial allocation strategy using BFS on BOM tree.
    Performs component explosion and allocates stock where available.
    Adds order-level component allocation remarks into so_df.
    """

    @classmethod
    def extra_required_schemas(cls):
        return {}


    def allocate(self) -> pl.DataFrame:
        order_remarks: dict[str, str] = {}

        def add_remark(order_id: str, message: str) -> None:
            """Append-safe remark writer."""
            order_remarks[order_id] = f"{order_remarks.get(order_id, '')}{' | ' if order_id in order_remarks else ''}{message}"

        # Output storage
        output_columns = {col: [] for col in [
            "SO_ID", "Plant", "Parent", "BOM_Level",
            "Item", "Order_Qty", "Stock_Before",
            "Allocated_Qty", "Order_Remaining", "Remaining_Stock"
        ]}

        def append_row(**kwargs):
            for k, v in kwargs.items():
                output_columns[k].append(v)

        # Iterate Sales Orders
        for r in self.so_df.iter_rows(named=True):
            so_id = str(r["order_id"]).strip()
            fg = str(r["fg_id"]).strip()
            plant = str(r["plant"]).strip()
            fg_qty = float(r.get("order_qty") or 0.0)

            resolved_root, bom_tree, resolution_type = self.bom_tree.resolve_fg(fg, plant)

            if resolution_type == "NOT_FOUND":
                add_remark(
                    so_id,
                    f"No BOM found where '{fg}' exists as FG or SFG at Plant '{plant}'. Order skipped."
                )
                continue

            if not bom_tree:
                add_remark(
                    so_id,
                    f"BOM tree empty for resolved root '{resolved_root}' at Plant '{plant}'. Order skipped."
                )
                continue

            if resolution_type == "SFG":
                add_remark(
                    so_id,
                    f"Ordered FG '{fg}' treated as SFG under BOM of '{resolved_root}'."
                )

            if fg_qty <= 0:
                add_remark(so_id, "Order quantity is zero; BOM exploded without allocation.")

            # BFS initialization
            queue = deque([{
                "item": fg,          # always start from ordered item
                "parent": "",
                "level": 0,
                "order_qty": fg_qty
            }])

            while queue:
                current = queue.popleft()
                item = current["item"]
                parent = current["parent"]
                level = current["level"]
                order_qty = float(current["order_qty"] or 0.0)

                if order_qty > 0:
                    if not self.stock_manager.has_stock(plant, so_id, item):
                        add_remark(
                            so_id,
                            f"No stock data for child component '{item}' at plant '{plant}'."
                        )
                        available = 0
                    else:
                        available = self.stock_manager.get_stock(plant, so_id, item)
                    allocated = min(order_qty, available)
                    if self.config.get("round_allocation", False):
                        allocated = round(allocated, 2)
                    remaining = order_qty - allocated
                    stock_remaining = available - allocated
                    self.stock_manager.set_stock(plant, so_id, item, stock_remaining)
                else:
                    available = allocated = remaining = stock_remaining = 0.0

                # Capture output row
                append_row(
                    SO_ID=so_id,
                    Plant=plant,
                    Parent=parent,
                    BOM_Level=level,
                    Item=item,
                    Order_Qty=order_qty,
                    Stock_Before=available,
                    Allocated_Qty=allocated,
                    Order_Remaining=remaining,
                    Remaining_Stock=stock_remaining
                )

                # Always explode children
                for child in bom_tree.get(item, []):
                    queue.append({
                        "item": child["child"],
                        "parent": item,
                        "level": level + 1,
                        "order_qty": remaining * child["ratio"]
                    })

            # Successful processing remark
            add_remark(so_id, "Order processed via component allocation. BOM exploded and stock allocation attempted.")

        # Create output DataFrame
        output_df = pl.DataFrame({
            "SO_ID": pl.Series(output_columns["SO_ID"], dtype=pl.Utf8),
            "Plant": pl.Series(output_columns["Plant"], dtype=pl.Utf8),
            "Parent": pl.Series(output_columns["Parent"], dtype=pl.Utf8),
            "BOM_Level": pl.Series(output_columns["BOM_Level"], dtype=pl.Int64),
            "Item": pl.Series(output_columns["Item"], dtype=pl.Utf8),
            "Order_Qty": pl.Series(output_columns["Order_Qty"], dtype=pl.Float64),
            "Stock_Before": pl.Series(output_columns["Stock_Before"], dtype=pl.Float64),
            "Allocated_Qty": pl.Series(output_columns["Allocated_Qty"], dtype=pl.Float64),
            "Order_Remaining": pl.Series(output_columns["Order_Remaining"], dtype=pl.Float64),
            "Remaining_Stock": pl.Series(output_columns["Remaining_Stock"], dtype=pl.Float64),
        })

        # Merge remarks into so_df
        if order_remarks:
            remarks_df = pl.DataFrame({
                "order_id": list(order_remarks.keys()),
                "component_allocation_remarks": list(order_remarks.values())
            })
            self.so_df = self.so_df.join(remarks_df, on="order_id", how="left")

        return output_df