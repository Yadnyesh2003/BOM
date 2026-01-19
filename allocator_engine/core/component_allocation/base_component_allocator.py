from abc import ABC, abstractmethod
import polars as pl
from common.bom_tree import BOMTree
from common.stock_manager import StockManager

class BaseComponentAllocator(ABC):
    """
    Abstract base class for all Component Allocation strategies.
    Defines the interface that every allocator must implement.
    """

    def __init__(self, so_df: pl.DataFrame, bom_tree: BOMTree, stock_manager: StockManager, config=None, logger=None) -> None:
        """
        :param so_df: Sales order dataframe
        :param bom_tree: BOMTree object with precomputed BOM
        :param stock_manager: StockManager object to get/set stock
        """
        self.so_df = so_df
        self.bom_tree = bom_tree
        self.stock_manager = stock_manager
        self.config = config or {}
        self.logger = logger

    @abstractmethod
    def allocate(self) -> pl.DataFrame:
        """
        Perform allocation according to the strategy.
        Must return a Polars DataFrame with columns:
        SO_ID, Sr_No, BOM_Level, Item, Order_Qty, Stock_Before,
        Allocated_Qty, Order_Remaining, Remaining_Stock
        """
        pass
    
    @classmethod
    def base_required_schemas(cls):
        return {
            "so": ["order_id", "fg_id", "plant", "order_qty"],
            "bom": ["root_parent", "parent", "child", "comp_qty", "plant"],
            "stock": ["order_id", "item_id", "plant", "stock_on_hand", "stock_in_qc", "stock_in_transit"]
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
