import pytest
import pandas as pd
import os
import json
import io
from pathlib import Path

from src.utils.local_file_mods import (
    read_local_csv,
    write_local_csv,
    load_local_json,
    local_file_exists,
    local_file_size,
    check_file_exists,
    local_mkdir,
    local_open,
    local_write_feather,
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


def test_read_local_csv(test_csv_file, expout_data):
    # Creating df using the function and test csv
    df = read_local_csv(str(test_csv_file))
    # Make sure the reader function has returned a df
    assert isinstance(df, pd.DataFrame)
    # Check that the df is the same as the expected data
    pd.testing.assert_frame_equal(df, expout_data)


def test_write_local_csv(tmp_path, input_data):
    filepath = tmp_path / "test.csv"

    write_local_csv(str(filepath), input_data)

    # Use os and pathlib to check that the written file exists
    assert os.path.exists(filepath)
    assert Path.exists(filepath)

    # Read the written CSV file and compare the content
    df = pd.read_csv(filepath)
    pd.testing.assert_frame_equal(df, input_data)


def test_load_local_json(tmp_path):
    # Create a test dictionary to write to json
    test_data_dict = {"key1": "value1", "key2": "value2"}
    json_filepath = tmp_path / "test.json"

    # Dump the test data to json
    with open(json_filepath, "w") as file:
        file.write(json.dumps(test_data_dict))

    # Load the json file and compare the content
    loaded_data = load_local_json(str(json_filepath))

    # Test that the loaded json exactly equals the test_data_dict
    assert loaded_data == test_data_dict


def test_local_file_exists(tmp_path):
    filepath = tmp_path / "test_file.txt"
    # Checking that it doesn't give a false positive
    assert not local_file_exists(str(filepath))

    with open(filepath, "w") as file:
        file.write("Test content")
    # Checking for (correct) positive
    assert local_file_exists(str(filepath))


def test_local_file_size(tmp_path):
    filepath = tmp_path / "test_file.txt"
    # Create the file with a tiny bit of content
    with open(filepath, "w") as file:
        file.write("Test content")

    # Check the file size is correct using both os and pathlib
    assert local_file_size(str(filepath)) == os.path.getsize(filepath)
    assert local_file_size(str(filepath)) == Path(filepath).stat().st_size


def test_check_file_exists(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, "w") as file:
        file.write("Test content")

    # Check that the file just created exists
    assert check_file_exists("test_file.txt", str(tmp_path))

    # Test that the correct error is raised when file not present
    with pytest.raises(FileNotFoundError):
        check_file_exists("nonexistent_file.txt", str(tmp_path))


def test_local_mkdir(tmp_path):
    folderpath = tmp_path / "test_folder"
    local_mkdir(str(folderpath))

    # Check that the folder we just created exists
    assert os.path.exists(folderpath)


def test_local_open(tmp_path):
    filepath = tmp_path / "test_file.txt"
    mode = "w"
    file = local_open(str(filepath), mode)

    # Check that it opens as a "BufferedIOBase object, buffer"
    assert isinstance(file, io.TextIOWrapper)
    assert os.path.exists(filepath)


def test_local_file_write_feather(tmp_path, input_data, expout_data):
    # Set up path and write data to feather
    filepath = tmp_path / "test.feather"
    local_write_feather(str(filepath), input_data)

    # Make sure the file has been created
    assert os.path.exists(filepath)

    # Read the written feather file and compare the content
    df = pd.read_feather(filepath)
    pd.testing.assert_frame_equal(df, expout_data)
