class StockManager:
    def __init__(self, logger):
        self.remaining_stock = {}
        self.logger = logger

    def _key(self, plant, so_id, item):
        if so_id:
            return f"{plant}__{so_id}__{item}"
        return f"{plant}__ITEM__{item}"


    def load_stock(self, so_stock_df, item_stock_df):
        for r in so_stock_df.iter_rows(named=True):
            key = self._key(r["plant"], r["order_id"], r["item_id"])
            self.remaining_stock[key] = self._extract_stock_buckets(r)

        for r in item_stock_df.iter_rows(named=True):
            key = self._key(r["plant"], None, r["item_id"])
            self.remaining_stock[key] = self._extract_stock_buckets(r)


    def _extract_stock_buckets(self, row):
        return {
            "stock_on_hand": float(row.get("stock_on_hand", 0) or 0),
            "stock_in_qc": float(row.get("stock_in_qc", 0) or 0),
            "stock_in_transit": float(row.get("stock_in_transit", 0) or 0),
        }


    def consume_with_priority(self, plant, so_id, item, consume_qty):
        """
        Deducts consume_qty from available stock buckets
        in priority order and returns:
        - allocation breakdown
        - unfulfilled quantity (if stock insufficient)
        """

        key_so = self._key(plant, so_id, item)
        key_item = self._key(plant, None, item)
        
        # IMPORTANT: Do NOT create stock if it never existed
        if key_so not in self.remaining_stock and key_item not in self.remaining_stock:
            self.logger.debug(
                "No stock entry exists. Skipping consumption | Plant=%s | SO=%s | Item=%s",
                plant, so_id, item
            )
            return {
                "stock_on_hand": 0.0,
                "stock_in_qc": 0.0,
                "stock_in_transit": 0.0
            }, float(consume_qty or 0)

        buckets = self.get_stock_buckets(plant, so_id, item)
        allocation = {
            "stock_on_hand": 0.0,
            "stock_in_qc": 0.0,
            "stock_in_transit": 0.0
        }
        remaining_to_consume = float(consume_qty or 0)
        
        self.logger.debug("Stock consume start | Plant=%s | SO=%s | Item=%s | Consume=%s | Buckets=%s", plant, so_id, item, remaining_to_consume, buckets)
        
        for col in ["stock_on_hand", "stock_in_qc", "stock_in_transit"]:
            if remaining_to_consume <= 0:
                break

            available = float(buckets.get(col, 0) or 0)
            if available <= 0:
                continue

            used = min(available, remaining_to_consume)
            allocation[col] = used
            buckets[col] = available - used
            remaining_to_consume -= used

        self.set_stock_buckets(plant, so_id, item, buckets)

        self.logger.info("Stock consume done | Allocation=%s | Unfulfilled=%s | Final Buckets=%s", allocation, remaining_to_consume, buckets)

        return allocation, remaining_to_consume



    def get_stock_buckets(self, plant, so_id, item):
        return self.remaining_stock.get(
            self._key(plant, so_id, item),
            self.remaining_stock.get(self._key(plant, None, item), {})
        )


    def set_stock_buckets(self, plant, so_id, item, buckets):
        key = self._key(plant, so_id, item)
        if key in self.remaining_stock:
            self.remaining_stock[key] = buckets
        else:
            self.logger.warning("Attempted to update non-existent stock | Plant=%s | SO=%s | Item=%s", plant, so_id, item)
            return

