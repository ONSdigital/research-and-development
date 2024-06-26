"""Unit testing module."""
# Import testing packages
from unittest import mock
import pandas as pd
import pytest

# Import modules to test
import sys

from src.utils.hdfs_mods import (
    read_rd_csv,
    write_rd_csv,
    rd_load_json,
    rd_file_exists,
    rd_file_size,
    check_file_exists,
)

# mark tests in file
pytestmark = pytest.mark.runhdfs

sys.modules["mock_f"] = mock.Mock()


class TestReadCsv:
    """Tests for rd_append function."""

    def input_data(self):

        data = {
            "run_id": [1, 2],
            "timestamp": ["Time:1", "Time:2"],
            "version": ["0.0.0", "0.0.1"],
            "duration": [5.0, 6.0],
        }

        return pd.DataFrame(data)

    def expout_data(self):

        data = {
            "run_id": [1, 2],
            "timestamp": ["Time:1", "Time:2"],
            "version": ["0.0.0", "0.0.1"],
            "duration": [5.0, 6.0],
        }

        return pd.DataFrame(data)

    @mock.patch("src.utils.hdfs_mods.pd")
    @mock.patch("src.utils.hdfs_mods.hdfs")
    def test_read_rd_csv(self, mock_hdfs, mock_pd_csv):
        """Test the expected functionality of read_rd_csv.

        Note:
            we pass the two patches defined above the function.
            firstly, mock_hdfs which refers to the bottom decorator
            secondly mock_pd_csv refers to the decorater above it.
        """
        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        mock_pd_csv.read_csv.return_value = self.input_data()

        df_result = read_rd_csv("file/path/filename.csv")

        # make sure function was called with mocked parameter in 'with open'
        mock_pd_csv.read_csv.assert_called_with(sys.modules["mock_f"])

        df_expout = self.expout_data()
        pd.testing.assert_frame_equal(df_result, df_expout)


class TestWriteCsv:
    @mock.patch("src.utils.hdfs_mods.hdfs")
    def test_write_rd_csv(self, mock_hdfs):
        """Test the expected functionality of write_rd_csv."""

        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        test_df = pd.DataFrame({"col": ["data"]})

        with mock.patch.object(test_df, "to_csv") as to_csv_mock:
            write_rd_csv("file/path/filename.csv", test_df)
            # make sure mocked data object was used inside 'with open'
            to_csv_mock.assert_called_with(sys.modules["mock_f"], index=False)


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
