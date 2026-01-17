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


# class StockManager:
#     """
#     Manages stock with strict separation between:
#     - stock existence (what came from input files)
#     - stock quantity (remaining after allocation)

#     Guarantees:
#     - Never creates new stock rows that were not present in input
#     - Zero stock is allowed ONLY for existing rows
#     """

#     def __init__(self):
#         # Remaining stock quantities
#         self.remaining_stock = {}

#         # Keys that originally existed in input stock files
#         self.original_keys = set()

#     def _key(self, plant, so_id, item):
#         if so_id:
#             return f"{plant}__{so_id}__{item}"
#         return f"{plant}__ITEM__{item}"

#     # -------------------------------------------------
#     # LOAD INPUT STOCK
#     # -------------------------------------------------

#     def load_stock(self, so_stock_df, item_stock_df):
#         """
#         Loads stock from input files and records existence.
#         """

#         # SO-level stock
#         for r in so_stock_df.iter_rows(named=True):
#             key = self._key(r["plant"], r["order_id"], r["item_id"])
#             self.remaining_stock[key] = r["stock"]
#             self.original_keys.add(key)

#         # Item-level stock
#         for r in item_stock_df.iter_rows(named=True):
#             key = self._key(r["plant"], None, r["item_id"])
#             self.remaining_stock[key] = r["stock"]
#             self.original_keys.add(key)

#     # -------------------------------------------------
#     # STOCK QUERIES
#     # -------------------------------------------------

#     def has_stock(self, plant, so_id, item):
#         """
#         Checks if stock ROW exists in input (SO-level or item-level).
#         """
#         return (
#             self._key(plant, so_id, item) in self.original_keys
#             or self._key(plant, None, item) in self.original_keys
#         )

#     def get_stock(self, plant, so_id, item):
#         """
#         Returns stock quantity.
#         If stock row does not exist, returns 0 WITHOUT creating it.
#         """
#         so_key = self._key(plant, so_id, item)
#         item_key = self._key(plant, None, item)

#         if so_key in self.remaining_stock:
#             return self.remaining_stock[so_key]

#         if item_key in self.remaining_stock:
#             return self.remaining_stock[item_key]

#         return 0

#     # -------------------------------------------------
#     # UPDATE STOCK (STRICT)
#     # -------------------------------------------------

#     def set_stock(self, plant, so_id, item, value):
#         """
#         Updates remaining stock ONLY if the stock row existed in input.
#         Does NOT create new stock rows.
#         """
#         so_key = self._key(plant, so_id, item)
#         item_key = self._key(plant, None, item)

#         if so_key in self.original_keys:
#             self.remaining_stock[so_key] = value
#         elif item_key in self.original_keys:
#             self.remaining_stock[item_key] = value
#         else:
#             # 🚫 Do NOT create phantom stock rows
#             return

#     # -------------------------------------------------
#     # EXPORT STOCK FOR OUTPUT
#     # -------------------------------------------------

#     def export_remaining_stock(self):
#         """
#         Returns ONLY stock rows that existed in input.
#         Phantom rows are filtered out.
#         """
#         output = {}

#         for key in self.original_keys:
#             if key in self.remaining_stock:
#                 output[key] = self.remaining_stock[key]

#         return output
