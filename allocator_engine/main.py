from pathlib import Path
import yaml

from pipeline.allocation_pipeline import AllocationPipeline
from utils.logger import EngineLogger

# --------------------------------------------------
# Resolve paths
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

# --------------------------------------------------
# Load config
# --------------------------------------------------
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# --------------------------------------------------
# Setup logger (ONCE)
# --------------------------------------------------
log_level = config.get("logging", {}).get("level", "INFO")

logger = EngineLogger(
    base_path=config["base_path"],
    client=config.get("client", "UNKNOWN"),
    level=log_level
)

# --------------------------------------------------
# Run pipeline
# --------------------------------------------------
try:
    logger.info("Starting allocation pipeline")

    pipeline = AllocationPipeline(config, logger)
    pipeline.run()

    logger.info("Pipeline completed successfully")

except Exception:
    logger.critical("Fatal pipeline error occurred", exc_info=True)
    raise

finally:
    logger.write_run_footer()
