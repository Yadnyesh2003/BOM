import polars as pl
from core.order_allocation.base_order_allocator import BaseOrderAllocator

REQUIRED_COLUMNS = {
    "so": ["order_id", "fg_id", "plant", "order_qty"],
    "stock": ["order_id", "item_id", "plant", "stock"]
}

class PartialOrderAllocator(BaseOrderAllocator):
    """
    Partial Order Allocation.
    Supports SO-level stock and ITEM-level stock.
    """

    def allocate(self):
        so_rows = []

        for r in self.so_df.iter_rows(named=True): 
            so_id = str(r["order_id"]).strip()
            fg = str(r["fg_id"]).strip()
            plant = str(r["plant"]).strip()
            order_qty = float(r["order_qty"] or 0)

            available = self.stock_manager.get_stock(
                plant=plant,
                so_id=so_id,
                item=fg
            )

            allocated = min(order_qty, available)
            remaining_order = order_qty - allocated
            remaining_stock = available - allocated

            # Update stock (SO-level first, ITEM fallback handled internally)
            self.stock_manager.set_stock(
                plant=plant,
                so_id=so_id,
                item=fg,
                value=remaining_stock
            )

            so_rows.append({
                "order_id": so_id,
                "plant": plant,
                "fg_id": fg,
                "order_qty": remaining_order
            })

        updated_so_df = pl.DataFrame(so_rows)

        # Build Remaining Stock DF (SO + ITEM)
        stock_rows = []
        for key, qty in self.stock_manager.remaining_stock.items():
            plant, scope, item = key.split("__")

            stock_rows.append({
                "order_id": None if scope == "ITEM" else scope,
                "item_id": item,
                "plant": plant,
                "stock": qty
            })

        remaining_stock_df = pl.DataFrame(stock_rows)

        return updated_so_df, remaining_stock_df
