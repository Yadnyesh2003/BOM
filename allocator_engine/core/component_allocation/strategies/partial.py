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

            # BOM validation
            if (fg, plant) not in self.bom_tree.bom_tree_map:
                logger.warning("Skipping order %s: BOM not found for FG=%s Plant=%s", so_id, fg, plant)
                add_remark(so_id, f"BOM not present for FG '{fg}' at Plant '{plant}'. Order skipped.")
                continue

            bom_tree = self.bom_tree.get_tree(fg, plant)
            if not bom_tree:
                logger.warning("Skipping order %s: Empty BOM tree for FG=%s Plant=%s", so_id, fg, plant)
                add_remark(so_id, f"BOM tree empty for FG '{fg}' at Plant '{plant}'. Order skipped.")
                continue

            if fg_qty <= 0:
                add_remark(so_id, "Order quantity is zero; BOM exploded without allocation.")

            # BFS initialization
            queue = deque([{"item": fg, "parent": "", "level": 0, "order_qty": fg_qty}])

            while queue:
                current = queue.popleft()
                item = current["item"]
                parent = current["parent"]
                level = current["level"]
                order_qty = float(current["order_qty"] or 0.0)

                if order_qty > 0:
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







# import polars as pl
# from collections import deque
# import logging

# from core.component_allocation.base_component_allocator import BaseComponentAllocator

# logger = logging.getLogger(__name__)


# class PartialComponentAllocator(BaseComponentAllocator):
#     """
#     Partial allocation strategy using BFS on BOM tree.
#     Performs component explosion and allocates stock where available.
#     Adds order-level component allocation remarks into so_df.
#     """

#     def allocate(self) -> pl.DataFrame:
#         # -----------------------------
#         # Order-level remarks store
#         # -----------------------------
#         order_remarks: dict[str, str] = {}

#         def add_remark(order_id: str, message: str) -> None:
#             """Append-safe remark writer."""
#             if order_id in order_remarks:
#                 order_remarks[order_id] += " | " + message
#             else:
#                 order_remarks[order_id] = message

#         # -----------------------------
#         # Output columns
#         # -----------------------------
#         output_columns = {
#             "SO_ID": [],
#             "Plant": [],
#             "Parent": [],
#             "BOM_Level": [],
#             "Item": [],
#             "Order_Qty": [],
#             "Stock_Before": [],
#             "Allocated_Qty": [],
#             "Order_Remaining": [],
#             "Remaining_Stock": []
#         }

#         # -----------------------------
#         # Iterate Sales Orders
#         # -----------------------------
#         for r in self.so_df.iter_rows(named=True):
#             so_id = str(r["order_id"]).strip()
#             fg = str(r["fg_id"]).strip()
#             plant = str(r["plant"]).strip()
#             fg_qty = float(r.get("order_qty") or 0.0)

#             # -----------------------------
#             # BOM existence validation
#             # -----------------------------
#             if (fg, plant) not in self.bom_tree.bom_tree_map:
#                 logger.warning(
#                     "Skipping order %s: BOM not found for FG=%s Plant=%s",
#                     so_id, fg, plant
#                 )
#                 add_remark(
#                     so_id,
#                     f"BOM not present for FG '{fg}' at Plant '{plant}'. Order skipped."
#                 )
#                 continue

#             bom_tree = self.bom_tree.get_tree(fg, plant)
#             if not bom_tree:
#                 logger.warning(
#                     "Skipping order %s: Empty BOM tree for FG=%s Plant=%s",
#                     so_id, fg, plant
#                 )
#                 add_remark(
#                     so_id,
#                     f"BOM tree empty for FG '{fg}' at Plant '{plant}'. Order skipped."
#                 )
#                 continue

#             if fg_qty <= 0:
#                 add_remark(
#                     so_id,
#                     "Order quantity is zero; BOM exploded without allocation."
#                 )

#             # -----------------------------
#             # BFS initialization
#             # -----------------------------
#             queue = deque([
#                 {
#                     "item": fg,
#                     "parent": "",
#                     "level": 0,
#                     "order_qty": fg_qty
#                 }
#             ])

#             # -----------------------------
#             # BFS traversal
#             # -----------------------------
#             while queue:
#                 current = queue.popleft()

#                 item = current["item"]
#                 parent = current["parent"]
#                 level = current["level"]
#                 order_qty = float(current["order_qty"] or 0.0)

#                 if order_qty > 0:
#                     available = self.stock_manager.get_stock(
#                         plant, so_id, item
#                     )

#                     allocated = min(order_qty, available)
#                     if self.config.get("round_allocation", False):
#                         allocated = round(allocated, 2)

#                     remaining = order_qty - allocated
#                     stock_remaining = available - allocated

#                     self.stock_manager.set_stock(
#                         plant, so_id, item, stock_remaining
#                     )
#                 else:
#                     available = 0.0
#                     allocated = 0.0
#                     remaining = 0.0
#                     stock_remaining = 0.0

#                 # -----------------------------
#                 # Capture output row
#                 # -----------------------------
#                 output_columns["SO_ID"].append(so_id)
#                 output_columns["Plant"].append(plant)
#                 output_columns["Parent"].append(parent)
#                 output_columns["BOM_Level"].append(level)
#                 output_columns["Item"].append(item)
#                 output_columns["Order_Qty"].append(order_qty)
#                 output_columns["Stock_Before"].append(available)
#                 output_columns["Allocated_Qty"].append(allocated)
#                 output_columns["Order_Remaining"].append(remaining)
#                 output_columns["Remaining_Stock"].append(stock_remaining)

#                 # -----------------------------
#                 # Always explode children
#                 # -----------------------------
#                 for child in bom_tree.get(item, []):
#                     queue.append({
#                         "item": child["child"],
#                         "parent": item,
#                         "level": level + 1,
#                         "order_qty": remaining * child["ratio"]
#                     })

#             # -----------------------------
#             # Successful processing remark
#             # -----------------------------
#             add_remark(
#                 so_id,
#                 "Order processed via component allocation. BOM exploded and stock allocation attempted."
#             )

#         # -----------------------------
#         # Output DataFrame
#         # -----------------------------
#         output_df = pl.DataFrame({
#             "SO_ID": pl.Series(output_columns["SO_ID"], dtype=pl.Utf8),
#             "Plant": pl.Series(output_columns["Plant"], dtype=pl.Utf8),
#             "Parent": pl.Series(output_columns["Parent"], dtype=pl.Utf8),
#             "BOM_Level": pl.Series(output_columns["BOM_Level"], dtype=pl.Int64),
#             "Item": pl.Series(output_columns["Item"], dtype=pl.Utf8),
#             "Order_Qty": pl.Series(output_columns["Order_Qty"], dtype=pl.Float64),
#             "Stock_Before": pl.Series(output_columns["Stock_Before"], dtype=pl.Float64),
#             "Allocated_Qty": pl.Series(output_columns["Allocated_Qty"], dtype=pl.Float64),
#             "Order_Remaining": pl.Series(output_columns["Order_Remaining"], dtype=pl.Float64),
#             "Remaining_Stock": pl.Series(output_columns["Remaining_Stock"], dtype=pl.Float64),
#         })

#         # -----------------------------
#         # Merge remarks into so_df
#         # -----------------------------
#         if order_remarks:
#             remarks_df = pl.DataFrame({
#                 "order_id": list(order_remarks.keys()),
#                 "component_allocation_remarks": list(order_remarks.values())
#             })

#             self.so_df = self.so_df.join(
#                 remarks_df,
#                 on="order_id",
#                 how="left"
#             )

#         return output_df

