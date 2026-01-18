from abc import ABC, abstractmethod
import polars as pl
from common.stock_manager import StockManager

class BaseOrderAllocator(ABC):
    def __init__(self, so_df: pl.DataFrame, stock_manager: StockManager, config=None, logger=None) -> None:
        """
        :param so_df: Sales order dataframe
        :param stock_manager: StockManager object to get/set stock
        """
        self.so_df = so_df
        self.stock_manager = stock_manager
        self.config = config or {}
        self.logger = logger

    @abstractmethod
    def allocate(self, logger) -> pl.DataFrame:
        """
        Returns:
        - updated_so_df (remaining qty)
        - remaining_stock_df
        """
        pass

    @classmethod
    def base_required_schemas(cls):
        return {
            "so": ["order_id", "fg_id", "plant", "order_qty"],
            "stock": ["order_id", "item_id", "plant", "stock"]
        }

    @classmethod
    def extra_required_schemas(cls):
        return {}

    @classmethod
    def resolved_required_schemas(cls):
        merged = {k: list(v) for k, v in cls.base_required_schemas().items()}
        for src, cols in cls.extra_required_schemas().items():
            merged.setdefault(src, [])
            merged[src].extend(cols)
        return merged
