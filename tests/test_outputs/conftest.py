"""Fixtures used to assists output tests."""
# Standard Library Imports
import pytest
import os
from typing import Callable

# Local Imports
from src.utils.config import safeload_yaml, merge_configs

def read_config() -> dict:
    """Read config for tests."""
    # read config file (relative path is consistent for tests)
    user_path = os.path.join("src", "user_config.yaml")
    dev_path = os.path.join("src", "dev_config.yaml")
    user_config = safeload_yaml(user_path)
    dev_config = safeload_yaml(dev_path)
    user_config.pop("config_validation", None)
    dev_config.pop("config_validation", None)
    config = merge_configs(user_config, dev_config)
    return config

CONFIG = read_config()
LOCATION = CONFIG["global"]["network_or_hdfs"]

@pytest.fixture(scope="module")
def write_csv_func() -> Callable:
    """Import and return the correct write_csv function."""
    # import the correct write_csv (assumption config is correct)
    if LOCATION.lower() == "network":
        from src.utils.local_file_mods import write_local_csv as write_csv
    else:
        from src.utils.hdfs_mods import write_hdfs_csv as write_csv
    return write_csv
