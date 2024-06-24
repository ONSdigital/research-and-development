"""Fixtures used to assists output tests."""
# Standard Library Imports
import pytest
import os
from typing import Callable

# Local Imports
from src.utils.helpers import ConfigSettings

def read_config() -> dict:
    """Read config for tests."""
    # read config file (relative path is consistent for tests)
    config_path = os.path.join("src", "developer_config.yaml")
    conf_obj = ConfigSettings(config_path)
    config = conf_obj.config_dict
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
