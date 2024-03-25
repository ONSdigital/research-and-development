"""Tests for intram_by_pg.py."""
# Standard Library Imports
import pytest
import os
import logging
import pathlib

# Third Party Imports
import pandas as pd

# Local Imports
from src.outputs.intram_by_pg import output_intram_by_pg
from src.utils.helpers import Config_settings
from src.staging.validation import validate_data_with_schema
from src.staging.staging_helpers import load_valdiate_mapper

# read config file
config_path = os.path.join("src", "developer_config.yaml")
conf_obj = Config_settings(config_path)
config = conf_obj.config_dict

# Assign config values to paths
LOCATION = config["global"]["network_or_hdfs"]
PATHS = config[f"{LOCATION}_paths"]

# create logger (required pass to function)
TestLogger = logging.getLogger(__name__)

# import the correct write_csv (assumption config is correct)
if LOCATION == "network":
    from src.utils.local_file_mods import write_local_csv as write_csv
    from src.utils.local_file_mods import local_file_exists as check_file_exists
    from src.utils.local_file_mods import read_local_csv as read_csv
else:
    from src.utils.hdfs_mods import write_hdfs_csv as write_csv
    from src.utils.hdfs_mods import hdfs_file_exists as check_file_exists
    from src.utils.hdfs_mods import read_hdfs_csv as read_csv


class TestOutputIntramByPG(object):
    """Tests for output_intram_bt_pg."""
    
    def setup_tmp_dir(self, path: pathlib.Path) -> None:
        """Set up the output directory given a temp path."""
        # create output dirs
        output_parent = os.path.join(path, "outputs")
        output_child = os.path.join(output_parent, "output_intram_by_pg")
        os.makedirs(output_child)
        # update config
        PATHS["output_path"] = output_parent
        return None


    @pytest.fixture(scope="function")
    def pg_detailed_df(self) -> pd.DataFrame:
        pg_detailed_mapper = load_valdiate_mapper(
            "pg_detailed_mapper_path",
            PATHS,
            check_file_exists,
            read_csv,
            TestLogger,
            validate_data_with_schema,
            None,
        )
        return pg_detailed_mapper


    @pytest.fixture(scope="function")
    def input_data_gb(self) -> pd.DataFrame:
        """Fixture for gb input data for tests."""
        columns = ["reference", "instance", "201", "211"]
        data = [
            [1, 1, 'AA', 4628363.6364],
            [1, 2, 'AA', 0.0],
            [2, 1, 'AA', 0.0],
            [3, 1, 'I', 244533.1667],
            [4, 1, 'I', 244523.1667],
            [5, 1, 'D', 79911.0],
            [6, 1, 'AH', 26254673.0],
            [7, 1, 'AD', 2196.1027],
            [8, 1, 'C', 282622.6444],
            [9, 1, 'Z', 90163.1053]
            ]
        input_df = pd.DataFrame(data=data, columns=columns)
        return input_df


    @pytest.fixture(scope="function")
    def input_data_ni(self) -> pd.DataFrame:
        """Fixture for NI input data for tests."""
        columns = ["reference", "instance", "201", "211"]
        data = [
            [1, 1, 'C', 27.0],
            [1, 2, 'G', 102.0],
            [2, 1, 'AA', 250.0],
            [3, 1, 'I', 628.0],
            [4, 1, 'D', 18.0],
            [5, 1, 'AA', 7.0],
            [6, 1, 'E', 41.0],
            [7, 1, 'AA', 0.0],
            [8, 1, 'AD', 143.0],
            [9, 1, 'J', 138.0],
            ]
        input_df = pd.DataFrame(data=data, columns=columns)
        return input_df

    
    def test_output_intram_by_pg_raises(self, tmp_path):
        """Defensive tests for output_intram_by_pg."""
        self.setup_tmp_dir(pathlib.Path(tmp_path))


    def test_output_intram_by_pg_gb(self, 
                                    tmp_path,
                                    input_data_gb):
        """Tests for output_intram_by_pg without NI data."""
        self.setup_tmp_dir(pathlib.Path(tmp_path))


    def test_output_intram_by_pg_uk(self, 
                                    tmp_path,
                                    input_data_gb,
                                    input_data_ni):
        """Tests for output_intram_by_pg with NI data."""
        self.setup_tmp_dir(pathlib.Path(tmp_path))