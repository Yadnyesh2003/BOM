# Allocation Engine

A modular, configuration-driven allocation engine for:
- Order allocation (Finished Good level stock allocation)
- Component allocation (BOM explosion + component-level allocation)

This documentation was generated from the repository structure and source code. The engine is implemented in Python using Polars for tabular operations and a strategy-based architecture for allocation behaviors. The code analyzed is under `allocator_engine/`.

---

# Quickstart: Setup & Run

## 1. Clone the repository
```bash
git clone <your-repo-link>
cd <repo-name>
```

## 2. Create and activate a virtual environment
```bash
python -m venv venv
```
```bash
# Windows - To activate the virtual environment
venv\Scripts\activate
```
```bash
# Linux / Mac - To activate the virtual environment
source venv/bin/activate
```

## 3. Install dependencies
```bash
pip install -r requirements.txt
```

## 4. Navigate to the engine folder
```bash
cd allocator_engine
```

## 5. NOTE:
- Update config/config.yaml as needed
- Place input CSV files in paths expected by your config
- Place intermediate files if required by your workflow

## 6. Run the main script
```bash
python main.py
```

---

# Overview

The Allocation Engine is a modular, strategy-driven data processing pipeline designed to perform:

- Order Allocation (Finished Goods allocation against stock)
- Component Allocation (BOM explosion + component-level stock allocation)

The engine is configuration-driven, extensible via strategies, and schema-resilient, allowing it to adapt to multiple allocation behaviors (partial, batchwise, levelwise, orderwise, etc.) without breaking the pipeline.

The system is optimized for:

- Large CSV-based datasets
- Deterministic allocation logic
- Clear separation of concerns
- Future extensibility for additional allocation strategies

---

# High-Level Flow
```scss
main.py
  └── AllocationPipeline
        ├── Order Allocation Phase (optional)
        │     ├── Schema Resolution
        │     ├── Stock Aggregation
        │     ├── Order Allocation Strategy
        │     └── Updated SO + Remaining Stock
        │
        ├── Component Allocation Phase (optional)
        │     ├── BOM Tree Construction
        │     ├── Stock Manager Initialization
        │     ├── Component Allocation Strategy
        │     └── Component Allocation Output
        │
        └── Output Writer
```
Each phase is independently switchable via config.yaml.

---

# Key Architectural Principles

## 1. Pipeline Pattern

- AllocationPipeline orchestrates execution  
- Each phase is isolated and composable  
- Output of one phase becomes input to the next  

## 2. Strategy Pattern

- Allocation behavior is encapsulated inside strategy classes  
- Adding a new strategy does not require pipeline changes  
- Strategies are registered via `phase_registry.py`  

## 3. Single Source of Truth for Stock

- StockManager maintains mutable stock state  
- Both SO-level and ITEM-level stock are supported  
- Prevents duplicated stock logic across strategies  

## 4. Schema Abstraction Layer

- SchemaResolver decouples input CSV headers from internal logic  
- Enables seamless switching between client datasets  
- Prevents pipeline breakage when CSV headers differ  

---

# Directory Structure & Responsibilities
```arduino
allocator_engine/
│
├── main.py
│   └── Entry point – config loading, logger setup, pipeline execution
│
├── pipeline/
│   ├── allocation_pipeline.py
│   │   └── Orchestrates phases & data flow
│   └── phase_registry.py
│       └── Strategy registration
│
├── core/
│   ├── order_allocation/
│   │   ├── base_order_allocator.py
│   │   └── strategies/
│   │       └── partial.py
│   │
│   └── component_allocation/
│       ├── base_component_allocator.py
│       └── strategies/
│           └── partial.py
│
├── common/
│   ├── stock_manager.py
│   │   └── Centralized stock state manager
│   └── bom_tree.py
│       └── Precomputed BOM tree per FG + Plant
│
├── utils/
│   ├── logger.py
│   │   └── Structured run-based logging
│   └── schema_resolver.py
│       └── Schema validation & normalization
│
├── io_modules/
│   ├── reader.py
│   ├── writer.py
│   └── config_reader.py
│
└── config/
    └── config.yaml
```

---

# Data Flow Between Phases
| Phase                | Input                    | Output                       |
| -------------------- | ------------------------ | ---------------------------- |
| Order Allocation     | SO CSV + Stock CSV       | Updated SO + Remaining Stock |
| Component Allocation | Updated SO + BOM + Stock | Component Allocation Report  |

Intermediate data is passed in-memory, not via temporary files.

---

