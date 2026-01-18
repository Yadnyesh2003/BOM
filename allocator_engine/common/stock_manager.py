class StockManager:
    def __init__(self, logger):
        self.remaining_stock = {}
        self.logger = logger

    def _key(self, plant, so_id, item):
        if so_id:
            return f"{plant}__{so_id}__{item}"
        return f"{plant}__ITEM__{item}"

    def load_stock(self, so_stock_df, item_stock_df):
        self.logger.debug("Loading stock: SO rows=%s, ITEM rows=%s", so_stock_df.height, item_stock_df.height)

        for r in so_stock_df.iter_rows(named=True):
            key = self._key(r["plant"], r["order_id"], r["item_id"])
            self.remaining_stock[key] = r["stock"]

        for r in item_stock_df.iter_rows(named=True):
            key = self._key(r["plant"], None, r["item_id"])
            self.remaining_stock[key] = r["stock"]

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