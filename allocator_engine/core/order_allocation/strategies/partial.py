# import polars as pl
# from core.order_allocation.base_order_allocator import BaseOrderAllocator


# class PartialOrderAllocator(BaseOrderAllocator):
#     """
#     Partial Order Allocation.
#     Supports SO-level stock and ITEM-level stock.
#     """

#     @classmethod
#     def extra_required_schemas(cls):
#         return {}

#     def allocate(self):
#         self.logger.info("Partial Order Allocation started")
#         so_rows = []

#         for r in self.so_df.iter_rows(named=True): 
#             so_id = str(r["order_id"]).strip()
#             fg = str(r["fg_id"]).strip()
#             plant = str(r["plant"]).strip()
#             order_qty = float(r["order_qty"] or 0)

#             self.logger.debug(f"Processing SO '{so_id}' | FG '{fg}' | Plant '{plant}' | Order Qty {order_qty}")

#             available = self.stock_manager.get_stock(
#                 plant=plant,
#                 so_id=so_id,
#                 item=fg
#             )
#             self.logger.debug(f"Available stock for FG '{fg}' at plant '{plant}': {available}")

#             if available > 0:
#                 allocated = min(order_qty, available)
#                 remark = (
#                     f"Stock found for FG '{fg}'. "
#                     f"Allocated {allocated} out of {order_qty}."
#                 )
#                 self.logger.info(f"Allocated {allocated} units for SO '{so_id}' | FG '{fg}'")
#             else:
#                 allocated = 0
#                 remark = (
#                     f"No stock found for FG '{fg}'. "
#                     f"Allocated 0 out of {order_qty}."
#                 )
#                 self.logger.warning(f"No stock to allocate for SO '{so_id}' | FG '{fg}'")
#             remaining_order = order_qty - allocated
#             remaining_stock = available - allocated
#             self.logger.debug(f"Remaining order: {remaining_order}, Remaining stock: {remaining_stock}")

#             # Update stock (SO-level first, ITEM fallback handled internally)
#             self.stock_manager.set_stock(
#                 plant=plant,
#                 so_id=so_id,
#                 item=fg,
#                 value=remaining_stock
#             )
#             self.logger.debug(f"Updated stock for FG '{fg}' at plant '{plant}' to {remaining_stock}")

#             so_rows.append({
#                 "order_id": so_id,
#                 "plant": plant,
#                 "fg_id": fg,
#                 "order_qty": remaining_order,
#                 "order_allocation_remarks": remark
#             })
#         updated_so_df = pl.DataFrame(so_rows)
#         self.logger.info("SO allocation complete. Building remaining stock dataframe.")


#         # Build Remaining Stock DF (SO + ITEM)
#         stock_rows = []
#         for key, qty in self.stock_manager.remaining_stock.items():
#             plant, scope, item = key.split("__")

#             stock_rows.append({
#                 "order_id": None if scope == "ITEM" else scope,
#                 "item_id": item,
#                 "plant": plant,
#                 "stock": qty
#             })
#             self.logger.debug(f"Remaining stock - Plant: {plant}, Scope: {scope}, Item: {item}, Qty: {qty}")

#         remaining_stock_df = pl.DataFrame(stock_rows)
#         self.logger.info("Remaining stock dataframe created successfully.")

#         return updated_so_df, remaining_stock_df



import polars as pl
from core.order_allocation.base_order_allocator import BaseOrderAllocator


class PartialOrderAllocator(BaseOrderAllocator):
    """
    Partial Order Allocation.
    Strategy:
    - Try to allocate as much as possible
    - Allocation priority: SOH -> QC -> Transit
    """

    @classmethod
    def extra_required_schemas(cls):
        return {}

    def allocate(self):
        self.logger.info("Partial Order Allocation started")

        so_rows = []

        for r in self.so_df.iter_rows(named=True):
            so_id = str(r["order_id"]).strip()
            fg = str(r["fg_id"]).strip()
            plant = str(r["plant"]).strip()
            order_qty = float(r["order_qty"] or 0)

            self.logger.debug(
                "Processing SO | SO=%s | FG=%s | Plant=%s | OrderQty=%s",
                so_id, fg, plant, order_qty
            )

            # --------------------------------------------
            # STRATEGY DECIDES QTY TO CONSUME
            # Partial strategy = try to fulfill full demand
            # --------------------------------------------
            qty_to_consume = order_qty

            allocation, unfulfilled = self.stock_manager.consume_with_priority(
                plant=plant,
                so_id=so_id,
                item=fg,
                consume_qty=qty_to_consume
            )

            allocated_qty = qty_to_consume - unfulfilled
            remaining_order = order_qty - allocated_qty

            if allocated_qty > 0:
                remark = (
                    f"Allocated {allocated_qty} out of {order_qty} "
                    f"(SOH/QC/Transit priority applied)"
                )
                self.logger.info(
                    "SO=%s | FG=%s | Allocated=%s | RemainingOrder=%s",
                    so_id, fg, allocated_qty, remaining_order
                )
            else:
                remark = (
                    f"No stock available for FG '{fg}'. "
                    f"Allocated 0 out of {order_qty}."
                )
                self.logger.warning(
                    "SO=%s | FG=%s | No allocation possible",
                    so_id, fg
                )

            so_rows.append({
                "order_id": so_id,
                "plant": plant,
                "fg_id": fg,
                "order_qty": remaining_order,
                "order_allocation_remarks": remark
            })

        updated_so_df = pl.DataFrame(so_rows)

        self.logger.info(
            "Partial Order Allocation completed. Preparing remaining stock dataframe."
        )

        # --------------------------------------------
        # BUILD REMAINING STOCK DF (MULTI-BUCKET)
        # --------------------------------------------
        stock_rows = []

        for key, buckets in self.stock_manager.remaining_stock.items():
            plant, scope, item = key.split("__")

            row = {
                "order_id": None if scope == "ITEM" else scope,
                "item_id": item,
                "plant": plant
            }

            for col, qty in buckets.items():
                row[col] = qty

            stock_rows.append(row)

            self.logger.debug(
                "Remaining stock | Plant=%s | Scope=%s | Item=%s | Buckets=%s",
                plant, scope, item, buckets
            )

        remaining_stock_df = pl.DataFrame(stock_rows)

        self.logger.info("Remaining stock dataframe created successfully.")

        return updated_so_df, remaining_stock_df