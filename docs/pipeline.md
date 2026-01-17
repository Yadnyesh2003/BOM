# Pipeline Deep Dive: All About Allocation Pipeline 

## Files

- `pipeline/allocation_pipeline.py`
- `pipeline/phase_registry.py`

---

## Main Responsibilities of AllocationPipeline

- Validate that at least one phase is enabled.
- For each enabled phase:
  - Read required CSV inputs using `SchemaResolver`.
  - Run the appropriate allocator (selected from `phase_registry`).
  - Mutate the data dictionary to pass outputs to the next phase.
  - Write outputs to the configured `output_path`.

---

## `_read_phase_inputs`

- Determines required schema keys from the allocator class:  
  `allocator_cls.resolved_required_schemas()` merges `base_required_schemas` + `extra_required_schemas`.
- Reads CSVs using `io_modules/reader.read_csv`.
- Resolves/renames columns via `SchemaResolver.resolve`.
- Avoids re-reading inputs if already present (allows previous phase outputs to be used).

---

## `_run_order_allocation`

- Cleans stock DataFrame and aggregates into:
  - `so_stock_df`: rows where `order_id` present (SO-level)
  - `item_stock_df`: rows where `order_id` missing/empty (ITEM-level)
- Loads stock into `StockManager`.
- Instantiates order allocator and calls `.allocate()` to get:
  - `updated_so_df`
  - `remaining_stock_df`
- Updates pipeline data.

---

## `_run_component_allocation`

- Cleans BOM & stock DataFrames.
- Aggregates stock similarly (SO vs ITEM).
- Builds `BOMTree` from BOM DataFrame.
- Loads stock into `StockManager`.
- Chooses component allocator and calls `.allocate()` to produce:
  - `component_allocation_df`
  - possibly updated `so_df` (strategies may annotate `so_df`)

---

## `_write_outputs`

- Writes back CSVs for each enabled phase into the configured `output_path` under `base_path`.
- Uses `io_modules/writer.write_csv`.
