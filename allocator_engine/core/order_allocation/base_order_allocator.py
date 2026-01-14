from abc import ABC, abstractmethod
import polars as pl
from common.stock_manager import StockManager

class BaseOrderAllocator(ABC):
    def __init__(self, so_df: pl.DataFrame, stock_manager: StockManager) -> None:
        """
        :param so_df: Sales order dataframe
        :param stock_manager: StockManager object to get/set stock
        """
        self.so_df = so_df
        self.stock_manager = stock_manager

    @abstractmethod
    def allocate(self) -> pl.DataFrame:
        """
        Returns:
        - updated_so_df (remaining qty)
        - remaining_stock_df
        """
        pass
