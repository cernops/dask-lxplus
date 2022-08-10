from pathlib import Path

import dask
import yaml

PYPKG_DIR = Path(__file__).parent
CONFIG_FILE = "jobqueue-cern.yaml"
PKG_CONFIG_FILE = PYPKG_DIR / CONFIG_FILE

def _ensure_user_config_file():
    dask.config.ensure_file(source=PKG_CONFIG_FILE)

def _set_base_config(priority: str = "old"):
    with open(PKG_CONFIG_FILE) as f:
        defaults = yaml.safe_load(f)

    dask.config.update(dask.config.config, defaults,priority=priority)

def _user_config_file_path() -> Path:
    return Path(dask.config.PATH) / CONFIG_FILE