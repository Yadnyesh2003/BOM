


class PartialOrderAllocator(BaseAllocator):
    """
    Allocates orders partially based on available resources.
    """

    def allocate(self, order, resources):
        allocated = {}
        remaining_quantity = order.quantity

        for resource in resources:
            if remaining_quantity <= 0:
                break

            allocatable_quantity = min(resource.available_quantity, remaining_quantity)
            if allocatable_quantity > 0:
                allocated[resource.id] = allocatable_quantity
                remaining_quantity -= allocatable_quantity

        return allocated