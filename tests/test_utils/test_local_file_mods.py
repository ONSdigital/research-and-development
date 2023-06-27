import pytest
import pandas as pd
import os
import json
import io
import pyarrow as pa

from src.utils.local_file_mods import (
    read_local_csv,
    write_local_csv,
    load_local_json,
    local_file_exists,
    local_file_size,
    check_file_exists,
    local_mkdir,
    local_open,
    local_file_write_feather,
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
    filepath = tmp_path / "test.csv"
    input_data.to_csv(filepath, index=False)
    return filepath


def test_read_local_csv(test_csv_file, expout_data):
    df = read_local_csv(str(test_csv_file))
    assert isinstance(df, pd.DataFrame)
    pd.testing.assert_frame_equal(df, expout_data)


def test_write_local_csv(tmp_path, input_data):
    filepath = tmp_path / "test.csv"
    
    write_local_csv(str(filepath), input_data)

    assert os.path.exists(filepath)

    # Read the written CSV file and compare the content
    df = pd.read_csv(filepath)
    pd.testing.assert_frame_equal(df, input_data)
    

def test_load_local_json(tmp_path):
    data = {"key1": "value1", "key2": "value2"}
    filepath = tmp_path / "test.json"
    
    # Dump the test data to json
    with open(filepath, "w") as file:
        file.write(json.dumps(data))

    # Load the json file and compare the content
    loaded_data = load_local_json(str(filepath))
    assert loaded_data == data


def test_local_file_exists(tmp_path):
    filepath = tmp_path / "test_file.txt"
    assert not local_file_exists(str(filepath))

    with open(filepath, "w") as file:
        file.write("Test content")

    assert local_file_exists(str(filepath))


def test_local_file_size(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, "w") as file:
        file.write("Test content")

    assert local_file_size(str(filepath)) == os.path.getsize(filepath)


def test_check_file_exists(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, "w") as file:
        file.write("Test content")

    assert check_file_exists("test_file.txt", str(tmp_path))

    with pytest.raises(FileNotFoundError):
        check_file_exists("nonexistent_file.txt", str(tmp_path))


def test_local_mkdir(tmp_path):
    folderpath = tmp_path / "test_folder"
    local_mkdir(str(folderpath))
    assert os.path.exists(folderpath)


def test_local_open(tmp_path):
    filepath = tmp_path / "test_file.txt"
    mode = "w"
    file = local_open(str(filepath), mode)
    assert isinstance(file, io.TextIOWrapper)
    assert os.path.exists(filepath)


def test_local_file_write_feather(tmp_path, expout_data):
    filepath = tmp_path / "test.feather"
    local_file_write_feather(str(filepath), expout_data)

    assert os.path.exists(filepath)

    # Read the written feather file and compare the content
    df = pd.read_feather(filepath)
    pd.testing.assert_frame_equal(df, expout_data)
