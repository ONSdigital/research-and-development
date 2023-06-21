"""Unit testing module."""
# Import testing packages
from unittest import mock
import pandas as pd

# Import modules to test
import os
import sys

from src.utils.hdfs_mods import (
    read_hdfs_csv,
    write_hdfs_csv,
    hdfs_load_json,
    check_file_exists,
)  # noqa

sys.modules["mock_f"] = mock.Mock()


class TestReadCsv:
    """Tests for hdfs_append function."""

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
    def test_read_hdfs_csv(self, mock_hdfs, mock_pd_csv):
        """Test the expected functionality of read_hdfs_csv."""

        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        mock_pd_csv.read_csv.return_value = self.input_data()

        df_result = read_hdfs_csv("file/path/filename.csv")

        mock_pd_csv.read_csv.assert_called_with(sys.modules["mock_f"])

        df_expout = self.expout_data()
        pd.testing.assert_frame_equal(df_result, df_expout)


class TestWriteCsv:
    @mock.patch("src.utils.hdfs_mods.hdfs")
    def test_write_hdfs_csv(self, mock_hdfs):
        """Test the expected functionality of write_hdfs_csv."""

        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        test_df = pd.DataFrame({"col": ["data"]})

        with mock.patch.object(test_df, "to_csv") as to_csv_mock:
            write_hdfs_csv("file/path/filename.csv", test_df)
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
    def test_hdfs_load_json(self, mock_hdfs, mock_json):
        """Test the expected functionality of hdfs_load_json."""

        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        mock_json.load.return_value = self.input_data()

        json_result = hdfs_load_json("file/path/filename.json")

        mock_json.load.assert_called_with(sys.modules["mock_f"])

        json_expout = self.expout_data()

        assert json_result == json_expout


class TestCheckFileExists:
    @mock.patch("src.utils.hdfs_mods.hdfs")
    def test_check_file_exists(self, mock_hdfs):
        """Test the check_file_exists function."""

        mock_hdfs.open.return_value.__enter__.return_value = sys.modules["mock_f"]

        # Act: use pytest to assert the result
        # Create emptyfile.py if it doesn't already exist
        empty_file = open("emptyfile.py", "w")

        # developer_config.yaml should exist and be non-empty
        result_1 = check_file_exists(
            "developer_config.yaml", "/home/cdsw/research-and-development/src/"
        )
        result_3 = check_file_exists(
            empty_file.name, "/home/cdsw/research-and-development/"
        )

        # Delete emptyfile.py after testing
        os.remove(empty_file.name)

        # Assert
        assert isinstance(result_1, bool)
        assert result_1
        assert not result_3
