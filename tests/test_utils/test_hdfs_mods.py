"""Unit testing module."""
# Import testing packages
from unittest import mock
import pandas as pd
import pytest

# Import modules to test
import sys

from src.utils.hdfs_mods import (
    rd_read_csv,
    rd_write_csv,
    rd_load_json,
    rd_file_exists,
    rd_file_size,
    check_file_exists,
)

# mark tests in file
pytestmark = pytest.mark.runhdfs

sys.modules["mock_f"] = mock.Mock()


class TestReadCsv2:
    """Tests for rd_read_csv function."""

    @mock.patch("src.utils.hdfs_mods.hdfs.open")
    @mock.patch("src.utils.hdfs_mods.pd.read_csv")
    def test_rd_read_csv2(self, mock_read_csv, mock_open):
        """Test the expected functionality of rd_read_csv."""
        # Mock the hdfs.open function
        mock_file = mock_open.return_value.__enter__.return_value

        # Mock the pd.read_csv function
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        mock_read_csv.return_value = mock_df

        # Call the rd_read_csv function
        filepath = "file/path/filename.csv"
        df_result = rd_read_csv(filepath)

        # Assert that hdfs.open was called with the correct filepath and mode
        mock_open.assert_called_once_with(filepath, "r")

        # Assert that pd.read_csv was called with the correct file object
        mock_read_csv.assert_called_once_with(mock_file, thousands=",")

        # Assert that the returned DataFrame is equal to the mocked DataFrame
        pd.testing.assert_frame_equal(df_result, mock_df)


class TestReadCsv:
    """Tests for rd_append function."""

    def mock_data(self):

        data = {
            "run_id": [1, 2],
            "timestamp": ["Time:1", "Time:2"],
            "version": ["0.0.0", "0.0.1"],
            "duration": [5.0, 6.0],
        }

        return pd.DataFrame(data)

    @mock.patch("src.utils.hdfs_mods.pd.read_csv")
    @mock.patch("src.utils.hdfs_mods.hdfs.open")
    def test_rd_read_csv(self, mock_open, mock_read_csv):
        """Test the expected functionality of rd_read_csv.

        Note:
            we pass the two patches defined above the function.
            firstly, mock_hdfs which refers to the bottom decorator
            secondly mock_pd_csv refers to the decorater above it.
        """
        mock_file = mock_open.return_value.__enter__.return_value
        mock_read_csv.return_value = self.mock_data()

        # Call the rd_read_csv function
        filepath = "file/path/filename.csv"
        df_result = rd_read_csv(filepath)

        # Assert that hdfs.open was called with the correct filepath and mode
        mock_open.assert_called_once_with(filepath, "r")

        # Assert that pd.read_csv was called with the correct file object
        mock_read_csv.assert_called_once_with(mock_file, thousands=",")
        pd.testing.assert_frame_equal(df_result, mock_read_csv)


class TestWriteCsv:
    @mock.patch("src.utils.hdfs_mods.hdfs")
    def test_rd_write_csv(self, mock_hdfs):
        """Test the expected functionality of rd_write_csv."""

        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        test_df = pd.DataFrame({"col": ["data"]})

        with mock.patch.object(test_df, "to_csv") as to_csv_mock:
            rd_write_csv("file/path/filename.csv", test_df)
            # make sure mocked data object was used inside 'with open'
            to_csv_mock.assert_called_with(
                sys.modules["mock_f"],
                date_format="%Y-%m-%d %H:%M:%S.%f+00",
                index=False,
            )


class TestLoadJson:
    """Tests for hdfs json loader."""

    def input_data(self):

        data = {
            "Col1": [1, 2, 3],
            "Col2": [4, 5, 6],
            "Col3": [7, 8, 9],
            "Col4": [10, 11, 12],
        }

        return data

    def expout_data(self):

        data = {
            "Col1": [1, 2, 3],
            "Col2": [4, 5, 6],
            "Col3": [7, 8, 9],
            "Col4": [10, 11, 12],
        }

        return data

    @mock.patch("src.utils.hdfs_mods.json")
    @mock.patch("src.utils.hdfs_mods.hdfs")
    def test_rd_load_json(self, mock_hdfs, mock_json):
        """Test the expected functionality of rd_load_json."""

        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        mock_json.load.return_value = self.input_data()

        json_result = rd_load_json("file/path/filename.json")

        mock_json.load.assert_called_with(sys.modules["mock_f"])

        json_expout = self.expout_data()

        assert json_result == json_expout


class TestHdfsFileExists:
    """Tests for function to check a file exists in HDFS."""

    @mock.patch("src.utils.hdfs_mods.hdfs.path.exists")
    def test_rd_file_exists(self, mock_rd_is_file):
        """Mock tests for rd_file_exists function in True and False cases."""
        # check True is returned if file exists
        mock_rd_is_file.return_value = True
        result1 = rd_file_exists("file/truepath/filename.csv")
        assert result1

        # check False is returned if file does not exist
        mock_rd_is_file.return_value = False
        result2 = rd_file_exists("file/falsepath/filename.csv")
        assert not result2


class TestHdfsFileSize:
    """Tests for function to return size of file in HDFS."""

    @mock.patch("src.utils.hdfs_mods.hdfs.path.getsize")
    def test_rd_file_size(self, mock_rd_getsize):
        """Mock test for rd_file_size to return size of file in hdfs."""
        mock_rd_getsize.return_value = 300
        result = rd_file_size("filepath/filename.csv")
        assert result == 300


class TestCheckFileExists:
    """Tests for function to check a file exists in HDFS."""

    @mock.patch("src.utils.hdfs_mods.hdfs.path.getsize")
    @mock.patch("src.utils.hdfs_mods.hdfs.path.exists")
    def test_check_rd_file_exists(self, mock_rd_is_file, mock_get_filesize):
        """Mock tests for check_rd_file_exists function in True and False cases."""
        # check True is returned if file exists and is non-empty
        mock_rd_is_file.return_value = True
        mock_get_filesize.return_value = 300
        result1 = check_file_exists("file/truepath/filename.csv")
        assert result1

        mock_get_filesize.return_value = 0
        result2 = check_file_exists("file/truepath/filename.csv")
        assert not result2

        # check error raised if file doesn't exist
        with pytest.raises(FileNotFoundError):
            mock_rd_is_file.return_value = False
            check_file_exists("file/truepath/filename.csv")
