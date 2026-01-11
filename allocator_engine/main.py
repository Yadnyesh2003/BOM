from pathlib import Path
from io_modules.config_reader import read_config
from pipeline.allocation_pipeline import AllocationPipeline

# Always Update BASE_PATH to your local path before running
BASE_PATH = Path(r"D:\000 VDL TESTING WORK\Polars_Alloc_Refactor")

# No Need to Update CONFIG_PATH
CONFIG_PATH = BASE_PATH / "allocator_engine" / "config" / "config.yaml"

# Read config and run pipeline
config = read_config(CONFIG_PATH)
pipeline = AllocationPipeline(config)
pipeline.run()