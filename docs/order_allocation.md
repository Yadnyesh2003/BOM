# Order Allocation

## Location

- `core/order_allocation/`
  - `base_order_allocator.py`
  - `strategies/partial.py` (implemented)

---

## Purpose

Allocate Finished Good (FG) level stock to Sales Orders (SO). Supports:

- **SO-level stock**: allocated to a specific SO  
- **ITEM-level stock**: fallback/pooled stock per plant

---

## Base Class

`BaseOrderAllocator` defines:

- `__init__(so_df, stock_manager, config=None)`
- `allocate()` → expected to return `(updated_so_df, remaining_stock_df)`
- `base_required_schemas()` → returns required minimal schema mapping

---

## Strategy Implemented: PartialOrderAllocator

**Behavior:**

- For each SO, tries to get stock for FG via `StockManager.get_stock(plant, so_id, item)`.
- Allocates `min(order_qty, available)`.
- Updates stock via `StockManager.set_stock`.
- Records a per-order remark with allocation details in `order_allocation_remarks`.

**Produces:**

- `updated_so_df` — contains `order_id`, `plant`, `fg_id`, `order_qty` (remaining), and `order_allocation_remarks`
- `remaining_stock_df` — transforms `StockManager.remaining_stock` to a Polars DataFrame (keys split into `plant`, `scope`, `item`)

---

## Notes & Suggestions

- Currently, `PartialOrderAllocator` updates stock in-place via `StockManager.set_stock`.  
- For alternative allocation policies (FIFO, expiry, batch-lot):
  - Extend `StockManager` to hold batch metadata
  - Write a new allocator that interprets batch-level rules
- `remaining_stock_df` uses a string key format `plant__scope__item`. Keep this consistent if adding other components.
