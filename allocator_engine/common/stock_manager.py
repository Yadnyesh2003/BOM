# class StockManager:
#     def __init__(self):
#         self.remaining_stock = {}

#     def load_stock(self, so_stock_df, item_stock_df):
#         for r in so_stock_df.iter_rows(named=True):
#             self.remaining_stock[f"{r['Order_ID']}__{r['Child']}"] = r["Stock"]
#         for r in item_stock_df.iter_rows(named=True):
#             self.remaining_stock[f"ITEM__{r['Child']}"] = r["Stock"]

#     def get_stock(self, so_id, item):
#         key = f"{so_id}__{item}"
#         if key in self.remaining_stock:
#             return self.remaining_stock[key]
#         return self.remaining_stock.get(f"ITEM__{item}", 0)

#     def set_stock(self, so_id, item, value):
#         key = f"{so_id}__{item}"
#         if key in self.remaining_stock:
#             self.remaining_stock[key] = value
#         else:
#             self.remaining_stock[f"ITEM__{item}"] = value


class StockManager:
    def __init__(self):
        self.remaining_stock = {}

    def _key(self, plant, so_id, item):
        if so_id:
            return f"{plant}__{so_id}__{item}"
        return f"{plant}__ITEM__{item}"

    def load_stock(self, so_stock_df, item_stock_df):
        for r in so_stock_df.iter_rows(named=True):
            key = self._key(r["Plant"], r["Order_ID"], r["Child"])
            self.remaining_stock[key] = r["Stock"]

        for r in item_stock_df.iter_rows(named=True):
            key = self._key(r["Plant"], None, r["Child"])
            self.remaining_stock[key] = r["Stock"]

    def get_stock(self, plant, so_id, item):
        key = self._key(plant, so_id, item)
        if key in self.remaining_stock:
            return self.remaining_stock[key]
        return self.remaining_stock.get(self._key(plant, None, item), 0)

    def set_stock(self, plant, so_id, item, value):
        key = self._key(plant, so_id, item)
        if key in self.remaining_stock:
            self.remaining_stock[key] = value
        else:
            self.remaining_stock[self._key(plant, None, item)] = value
