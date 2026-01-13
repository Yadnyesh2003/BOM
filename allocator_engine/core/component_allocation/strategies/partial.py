import polars as pl
from core.component_allocation.base_component_allocator import BaseComponentAllocator

class PartialComponentAllocator(BaseComponentAllocator):
    """
    Partial allocation strategy using DFS on BOM tree.
    """

    def __init__(self, so_df: pl.DataFrame, bom_tree, stock_manager, config: dict = None):
        super().__init__(so_df, bom_tree, stock_manager)
        self.config = config or {}

    def allocate(self) -> pl.DataFrame:
        # Initialize output columns
        output_columns = {name: [] for name in [
            "SO_ID","Plant","Sr_No","BOM_Level","Item",
            "Order_Qty","Stock_Before","Allocated_Qty",
            "Order_Remaining","Remaining_Stock"
        ]}

        # Loop through each sales order
        for so_index, r in enumerate(self.so_df.iter_rows(named=True)):
            so_id = str(r["SO_ID"]).strip()
            fg = str(r["FG_ID"]).strip()
            plant = str(r["Plant"]).strip()
            fg_qty = float(r.get("Order_Qty") or 0)

            # Skip if BOM not found
            if (fg, plant) not in self.bom_tree.bom_tree_map:
                print(f"[SKIP] No BOM for FG={fg}, Plant={plant}")
                continue

            bom_tree = self.bom_tree.get_tree(fg, plant)
            # if not bom_tree:
            #     print(f"[SKIP] No BOM for FG={fg}, Plant={plant}")
            #     continue
            
            stack = [{
                "item": fg,
                "level": 0,
                "order_qty": fg_qty,
                "sr_no": str(so_index + 1)
            }]

            while stack:
                current = stack.pop()
                available = self.stock_manager.get_stock(plant, so_id, current["item"])

                # Compute allocation (respecting config if needed)
                allocated = min(current["order_qty"], available)
                if self.config.get("round_allocation", False):
                    allocated = round(allocated, 2)  # Example: round to 2 decimals

                remaining = current["order_qty"] - allocated
                stock_remaining = available - allocated

                # Update stock
                self.stock_manager.set_stock(plant, so_id, current["item"], stock_remaining)

                # Append to output
                output_columns["SO_ID"].append(so_id)
                output_columns["Plant"].append(plant)
                output_columns["Sr_No"].append(current["sr_no"])
                output_columns["BOM_Level"].append(current["level"])
                output_columns["Item"].append(current["item"])
                output_columns["Order_Qty"].append(current["order_qty"])
                output_columns["Stock_Before"].append(available)
                output_columns["Allocated_Qty"].append(allocated)
                output_columns["Order_Remaining"].append(remaining)
                output_columns["Remaining_Stock"].append(stock_remaining)

                # Add children to stack for DFS
                if remaining > 0:
                    children = bom_tree.get(current["item"], [])
                    for i in range(len(children) - 1, -1, -1):
                        c = children[i]
                        stack.append({
                            "item": c["child"],
                            "level": current["level"] + 1,
                            "order_qty": remaining * c["ratio"],
                            "sr_no": f"{current['sr_no']}.{i + 1}"
                        })

        # Convert to Polars DataFrame with proper types
        output_df = pl.DataFrame({
            "SO_ID": pl.Series(output_columns["SO_ID"], dtype=pl.Utf8),
            "Plant": pl.Series(output_columns["Plant"], dtype=pl.Utf8),
            "Sr_No": pl.Series(output_columns["Sr_No"], dtype=pl.Utf8),
            "BOM_Level": pl.Series(output_columns["BOM_Level"], dtype=pl.Float64),
            "Item": pl.Series(output_columns["Item"], dtype=pl.Utf8),
            "Order_Qty": pl.Series(output_columns["Order_Qty"], dtype=pl.Float64),
            "Stock_Before": pl.Series(output_columns["Stock_Before"], dtype=pl.Float64),
            "Allocated_Qty": pl.Series(output_columns["Allocated_Qty"], dtype=pl.Float64),
            "Order_Remaining": pl.Series(output_columns["Order_Remaining"], dtype=pl.Float64),
            "Remaining_Stock": pl.Series(output_columns["Remaining_Stock"], dtype=pl.Float64)
        })

        return output_df



# # USING BFS APPROACH IF NEEDED - PLANT LOGIC MISSING
# import polars as pl
# from collections import deque
# from core.component_allocation.base_component_allocator import BaseComponentAllocator

# class PartialComponentAllocator(BaseComponentAllocator):
#     """
#     Partial allocation strategy using BFS on BOM tree.
#     """

#     def __init__(self, so_df: pl.DataFrame, bom_tree, stock_manager, config: dict = None):
#         super().__init__(so_df, bom_tree, stock_manager)
#         self.config = config or {}

#     def allocate(self) -> pl.DataFrame:
#         # Initialize output columns
#         output_columns = {name: [] for name in [
#             "SO_ID","Sr_No","BOM_Level","Item",
#             "Order_Qty","Stock_Before","Allocated_Qty",
#             "Order_Remaining","Remaining_Stock"
#         ]}

#         # Loop through each sales order
#         for so_index, r in enumerate(self.so_df.iter_rows(named=True)):
#             so_id = str(r["SO_ID"]).strip()
#             fg = str(r["FG_ID"]).strip()
#             fg_qty = float(r.get("Order_Qty") or 0)

#             # Skip if BOM not found
#             if fg not in self.bom_tree.bom_tree_map:
#                 continue

#             bom_tree = self.bom_tree.get_tree(fg)
#             queue = deque([{
#                 "item": fg,
#                 "level": 0,
#                 "order_qty": fg_qty,
#                 "sr_no": str(so_index + 1)
#             }])

#             while queue:
#                 current = queue.popleft()
#                 available = self.stock_manager.get_stock(so_id, current["item"])

#                 # Compute allocation (respecting config if needed)
#                 allocated = min(current["order_qty"], available)
#                 if self.config.get("round_allocation", False):
#                     allocated = round(allocated, 2)  # Example: round to 2 decimals

#                 remaining = current["order_qty"] - allocated
#                 stock_remaining = available - allocated

#                 # Update stock
#                 self.stock_manager.set_stock(so_id, current["item"], stock_remaining)

#                 # Append to output
#                 output_columns["SO_ID"].append(so_id)
#                 output_columns["Sr_No"].append(current["sr_no"])
#                 output_columns["BOM_Level"].append(current["level"])
#                 output_columns["Item"].append(current["item"])
#                 output_columns["Order_Qty"].append(current["order_qty"])
#                 output_columns["Stock_Before"].append(available)
#                 output_columns["Allocated_Qty"].append(allocated)
#                 output_columns["Order_Remaining"].append(remaining)
#                 output_columns["Remaining_Stock"].append(stock_remaining)

#                 # Add children to queue for BFS
#                 if remaining > 0:
#                     children = bom_tree.get(current["item"], [])
#                     for i, c in enumerate(children):
#                         queue.append({
#                             "item": c["child"],
#                             "level": current["level"] + 1,
#                             "order_qty": remaining * c["ratio"],
#                             "sr_no": f"{current['sr_no']}.{i + 1}"
#                         })

#         # Convert to Polars DataFrame with proper types
#         output_df = pl.DataFrame({
#             "SO_ID": pl.Series(output_columns["SO_ID"], dtype=pl.Utf8),
#             "Sr_No": pl.Series(output_columns["Sr_No"], dtype=pl.Utf8),
#             "BOM_Level": pl.Series(output_columns["BOM_Level"], dtype=pl.Float64),
#             "Item": pl.Series(output_columns["Item"], dtype=pl.Utf8),
#             "Order_Qty": pl.Series(output_columns["Order_Qty"], dtype=pl.Float64),
#             "Stock_Before": pl.Series(output_columns["Stock_Before"], dtype=pl.Float64),
#             "Allocated_Qty": pl.Series(output_columns["Allocated_Qty"], dtype=pl.Float64),
#             "Order_Remaining": pl.Series(output_columns["Order_Remaining"], dtype=pl.Float64),
#             "Remaining_Stock": pl.Series(output_columns["Remaining_Stock"], dtype=pl.Float64)
#         })

#         return output_df