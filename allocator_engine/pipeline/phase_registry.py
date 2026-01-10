# Order Allocation strategies
# from order_allocation.strategies.partial import PartialOrderAllocator
# from order_allocation.strategies.batchwise import BatchwiseOrderAllocator

# Component Allocation strategies
from allocator_engine.core.component_allocation.strategies.partial import PartialComponentAllocator
# from component_allocation.strategies.batchwise import BatchwiseComponentAllocator
# from component_allocation.strategies.levelwise import LevelwiseComponentAllocator



ORDER_ALLOCATORS = {
    # "partial": PartialOrderAllocator,
    # "batchwise": BatchwiseOrderAllocator
}

COMPONENT_ALLOCATORS = {
    "partial": PartialComponentAllocator,
}
