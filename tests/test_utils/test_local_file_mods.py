import pytest
import os
import json
import io
import pathlib
from typing import Union

import pandas as pd
import yaml

from src.utils.local_file_mods import (
    rd_read_csv,
    rd_write_csv,
    rd_load_json,
    rd_file_exists,
    rd_file_size,
    check_file_exists,
    rd_mkdir,
    # rd_open,
    rd_write_feather,
    safeload_yaml,
)


@pytest.fixture
def input_data():
    data = {
        "run_id": [1, 2],
        "timestamp": ["Time:1", "Time:2"],
        "version": ["0.0.0", "0.0.1"],
        "duration": [5.0, 6.0],
    }
    return pd.DataFrame(data)


@pytest.fixture
def expout_data():
    data = {
        "run_id": [1, 2],
        "timestamp": ["Time:1", "Time:2"],
        "version": ["0.0.0", "0.0.1"],
        "duration": [5.0, 6.0],
    }
    return pd.DataFrame(data)


@pytest.fixture
def test_csv_file(tmp_path, input_data):
    # Write the input data to a csv for testing
    filepath = tmp_path / "test.csv"
    input_data.to_csv(filepath, index=False)
    return filepath


def test_rd_read_csv(test_csv_file, expout_data):
    # Creating df using the function and test csv
    df = rd_read_csv(str(test_csv_file))
    # Make sure the reader function has returned a df
    assert isinstance(df, pd.DataFrame)
    # Check that the df is the same as the expected data
    pd.testing.assert_frame_equal(df, expout_data)


def test_rd_write_csv(tmp_path, input_data):
    filepath = tmp_path / "test.csv"

    rd_write_csv(str(filepath), input_data)

    # Use os and pathlib to check that the written file exists
    assert os.path.exists(filepath)
    assert pathlib.Path.exists(filepath)

    # Read the written CSV file and compare the content
    df = pd.read_csv(filepath)
    pd.testing.assert_frame_equal(df, input_data)


def test_rd_load_json(tmp_path):
    # Create a test dictionary to write to json
    test_data_dict = {"key1": "value1", "key2": "value2"}
    json_filepath = tmp_path / "test.json"

    # Dump the test data to json
    with open(json_filepath, "w") as file:
        file.write(json.dumps(test_data_dict))

    # Load the json file and compare the content
    loaded_data = rd_load_json(str(json_filepath))

    # Test that the loaded json exactly equals the test_data_dict
    assert loaded_data == test_data_dict


def test_rd_file_exists(tmp_path):
    filepath = tmp_path / "test_file.txt"
    # Checking that it doesn't give a false positive
    assert not rd_file_exists(str(filepath))

    with open(filepath, "w") as file:
        file.write("Test content")
    # Checking for (correct) positive
    assert rd_file_exists(str(filepath))


def test_rd_file_size(tmp_path):
    filepath = tmp_path / "test_file.txt"
    # Create the file with a tiny bit of content
    with open(filepath, "w") as file:
        file.write("Test content")

    # Check the file size is correct using both os and pathlib
    assert rd_file_size(str(filepath)) == os.path.getsize(filepath)
    assert rd_file_size(str(filepath)) == pathlib.Path(filepath).stat().st_size


def test_check_file_exists(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, "w") as file:
        file.write("Test content")

    # Check that the file just created exists
    assert check_file_exists("test_file.txt", str(tmp_path))

    # Test that the correct error is raised when file not present
    with pytest.raises(FileNotFoundError):
        check_file_exists("nonexistent_file.txt", str(tmp_path))


def test_rd_mkdir(tmp_path):
    folderpath = tmp_path / "test_folder"
    rd_mkdir(str(folderpath))

    # Check that the folder we just created exists
    assert os.path.exists(folderpath)


def test_rd_file_write_feather(tmp_path, input_data, expout_data):
    # Set up path and write data to feather
    filepath = tmp_path / "test.feather"
    rd_write_feather(str(filepath), input_data)

    # Make sure the file has been created
    assert os.path.exists(filepath)

    # Read the written feather file and compare the content
    df = pd.read_feather(filepath)
    pd.testing.assert_frame_equal(df, expout_data)


def write_dict_to_yaml(_dict: dict, path: Union[str, pathlib.Path]) -> None:
    """Write a dictionary as a yaml file

    Args:
        _dict (dict): The data to save to the yaml file.
        path (Union[str, pathlib.Path]): The save path (including file ext).

    Returns:
        None
    """
    with open(path, "w") as f:
        yaml.dump(_dict, f, default_flow_style=False)


class TestSafeloadYaml(object):
    """Tests for safeload_yaml."""

    def test_safeload_yaml(self, tmp_path):
        """General tests for safeload_yaml."""
        # save yaml first
        test_data = {
            "a": [1, 2, 3],
            "b": "string",
            "c": 6,
            "d": True,
            "e": {"bool": False, "float": 4.5},
        }
        test_path = os.path.join(tmp_path, "test_yaml.yaml")
        write_dict_to_yaml(_dict=test_data, path=test_path)
        # read in the data
        assert os.path.exists(test_path), f".yaml file not found at {test_path}"
        data = safeload_yaml(test_path)
        assert test_data == data, "Data read from yaml is incorrect."
