import yaml
from pathlib import Path

def read_config(config_path: Path) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
