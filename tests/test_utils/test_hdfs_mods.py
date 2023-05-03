"""Unit testing module."""
# Import testing packages
from unittest import mock
import pandas as pd

# Import modules to test
from src.utils.hdfs_mods import read_hdfs_csv, write_hdfs_csv


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

        mock_f = mock.Mock()
        mock_hdfs.open.return_value.__enter__.return_value = mock_f

        mock_pd_csv.read_csv.return_value = self.input_data()

        df_result = read_hdfs_csv("file/path/filename.csv")

        mock_pd_csv.read_csv.assert_called_with(mock_f)

        df_expout = self.expout_data()
        pd.testing.assert_frame_equal(df_result, df_expout)


class TestWriteCsv:
    @mock.patch("src.utils.hdfs_mods.hdfs")
    def test_write_hdfs_csv(self, mock_hdfs):
        """Test the expected functionality of write_hdfs_csv."""

        mock_f = mock.Mock()
        mock_hdfs.open.return_value.__enter__.return_value = mock_f

        test_df = pd.DataFrame({"col": ["data"]})

        with mock.patch.object(test_df, "to_csv") as to_csv_mock:
            write_hdfs_csv("file/path/filename.csv", test_df)
            to_csv_mock.assert_called_with(mock_f, index=False)
