from abc import ABC, abstractmethod

class BaseOrderAllocator(ABC):
    @abstractmethod
    def allocate(self, so_df, stock_df):
        """
        Returns:
        - updated_so_df (remaining qty)
        - remaining_stock_df
        """
        pass
