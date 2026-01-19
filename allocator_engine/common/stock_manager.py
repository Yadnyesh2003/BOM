class StockManager:
    def __init__(self, logger):
        self.remaining_stock = {}
        self.logger = logger

    def _key(self, plant, so_id, item):
        if so_id:
            return f"{plant}__{so_id}__{item}"
        return f"{plant}__ITEM__{item}"

    # def load_stock(self, so_stock_df, item_stock_df):
    #     self.logger.debug("Loading stock: SO rows=%s, ITEM rows=%s", so_stock_df.height, item_stock_df.height)

    #     for r in so_stock_df.iter_rows(named=True):
    #         key = self._key(r["plant"], r["order_id"], r["item_id"])
    #         self.remaining_stock[key] = r["stock"]

    #     for r in item_stock_df.iter_rows(named=True):
    #         key = self._key(r["plant"], None, r["item_id"])
    #         self.remaining_stock[key] = r["stock"]

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

    def has_stock(self, plant, so_id, item):
        self.logger.debug("Checking stock for plant=%s, so_id=%s, item=%s", plant, so_id, item)
        return (
            self._key(plant, so_id, item) in self.remaining_stock
            or self._key(plant, None, item) in self.remaining_stock
        )

    def get_stock(self, plant, so_id, item):
        key = self._key(plant, so_id, item)
        if key in self.remaining_stock:
            self.logger.debug("Retrieved stock for SO-level key %s: %s", key, self.remaining_stock[key])
            return self.remaining_stock[key]
        self.logger.debug("Retrieved stock for ITEM-level key %s: %s", self._key(plant, None, item), self.remaining_stock.get(self._key(plant, None, item), 0))
        return self.remaining_stock.get(self._key(plant, None, item), 0)


    def set_stock(self, plant, so_id, item, value):
        so_key = self._key(plant, so_id, item)
        item_key = self._key(plant, None, item)

        if so_key in self.remaining_stock:
            self.remaining_stock[so_key] = value
            self.logger.debug("Updated stock for SO-level key %s: %s", so_key, value)
        elif item_key in self.remaining_stock:
            self.remaining_stock[item_key] = value
            self.logger.debug("Updated stock for ITEM-level key %s: %s", item_key, value)
        else:
            self.logger.warning("Attempted to set stock for non-existent key: %s or %s", so_key, item_key)
            return


    # def allocate_with_priority(self, plant, so_id, item, required_qty):
    #     buckets = self.get_stock_buckets(plant, so_id, item)

    #     allocation = {
    #         "stock_on_hand": 0,
    #         "stock_in_qc": 0,
    #         "stock_in_transit": 0
    #     }

    #     remaining = required_qty

    #     for col in ["stock_on_hand", "stock_in_qc", "stock_in_transit"]:
    #         available = buckets.get(col, 0)
    #         if available <= 0 or remaining <= 0:
    #             continue

    #         alloc = min(available, remaining)
    #         allocation[col] = alloc
    #         buckets[col] -= alloc
    #         remaining -= alloc

    #     self.set_stock_buckets(plant, so_id, item, buckets)

    #     return allocation, remaining


    def consume_with_priority(self, plant, so_id, item, consume_qty):
        """
        Deducts consume_qty from available stock buckets
        in priority order and returns:
        - allocation breakdown
        - unfulfilled quantity (if stock insufficient)
        """
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
            self.remaining_stock[self._key(plant, None, item)] = buckets

