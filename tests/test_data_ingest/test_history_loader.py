import pytest
from typing import Generator, List

from src.data_ingest.history_loader import history_years, hist_paths_to_load, load_history

@pytest.fixture
def hist_folder_path():
    return "/path/to/hist_folder/"

@pytest.fixture
def read_csv_func():
    def mock_read_csv_func(path):
        # Mock implementation of read_csv_func
        pass
    return mock_read_csv_func

def test_history_years():
    current = 2023
    back_history = 3

    expected_result = [2022, 2021, 2020]

    result = list(history_years(current, back_history))
    assert result == expected_result

    back_history = 0

    expected_result = []

    result = list(history_years(current, back_history))
    assert result == expected_result

def test_hist_paths_to_load(hist_folder_path):
    history_years = [2022, 2021, 2020]

    expected_result = [
        "/path/to/hist_folder/qv_BERD_202212_qv6_reformatted.csv",
        "/path/to/hist_folder/qv_BERD_202112_qv6_reformatted.csv",
        "/path/to/hist_folder/qv_BERD_202012_qv6_reformatted.csv"
    ]

    result = hist_paths_to_load(hist_folder_path, history_years)
    assert result == expected_result

    history_years = []

    expected_result = []

    result = hist_paths_to_load(hist_folder_path, history_years)
    assert result == expected_result

def test_load_history(hist_folder_path, read_csv_func, monkeypatch):
    year_generator = history_years(2023, 3)

    def mock_info(message):
        pass
    
    # mock_info function that serves as a replacement for the .info method.
    monkeypatch.setattr("src.history_loader.history_loader_logger.info", mock_info)


    expected_paths_load_list = [
        "/path/to/hist_folder/qv_BERD_202212_qv6_reformatted.csv",
        "/path/to/hist_folder/qv_BERD_202112_qv6_reformatted.csv",
        "/path/to/hist_folder/qv_BERD_202012_qv6_reformatted.csv"
    ]

    result = load_history(year_generator, hist_folder_path, read_csv_func)

    assert read_csv_func.call_count == 3
    read_csv_func.assert_called_with("/path/to/hist_folder/qv_BERD_202212_qv6_reformatted.csv")
    assert result is None
