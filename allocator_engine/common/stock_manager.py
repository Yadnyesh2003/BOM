class StockManager:
    def __init__(self):
        self.remaining_stock = {}

    def _key(self, plant, so_id, item):
        if so_id:
            return f"{plant}__{so_id}__{item}"
        return f"{plant}__ITEM__{item}"

    def load_stock(self, so_stock_df, item_stock_df):
        for r in so_stock_df.iter_rows(named=True):
            key = self._key(r["plant"], r["order_id"], r["item_id"])
            self.remaining_stock[key] = r["stock"]

        for r in item_stock_df.iter_rows(named=True):
            key = self._key(r["plant"], None, r["item_id"])
            self.remaining_stock[key] = r["stock"]

    def has_stock(self, plant, so_id, item):
        return (
            self._key(plant, so_id, item) in self.remaining_stock
            or self._key(plant, None, item) in self.remaining_stock
        )

    def get_stock(self, plant, so_id, item):
        key = self._key(plant, so_id, item)
        if key in self.remaining_stock:
            return self.remaining_stock[key]
        return self.remaining_stock.get(self._key(plant, None, item), 0)


    def set_stock(self, plant, so_id, item, value):
        so_key = self._key(plant, so_id, item)
        item_key = self._key(plant, None, item)

        if so_key in self.remaining_stock:
            self.remaining_stock[so_key] = value
        elif item_key in self.remaining_stock:
            self.remaining_stock[item_key] = value
        else:
            return