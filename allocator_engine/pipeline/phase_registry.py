# Order Allocation strategies
from core.order_allocation.strategies.partial import PartialOrderAllocator

# Component Allocation strategies
from core.component_allocation.strategies.partial import PartialComponentAllocator



ORDER_ALLOCATORS = {
    "partial": PartialOrderAllocator,
    # "batchwise": BatchwiseOrderAllocator,
}

COMPONENT_ALLOCATORS = {
    "partial": PartialComponentAllocator,
}
