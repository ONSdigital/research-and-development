"""Unit testing module."""
# Import testing packages
from unittest import mock
import pandas as pd

# Import module to test
from src.utils.hdfs_mods import read_hdfs_csv


class Test_read_hdfs_csv:
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
    def test_expected(self, mock_hdfs, mock_pd_csv):
        """Test the expected functionality."""

        mock_f = mock.Mock()
        mock_hdfs.open.return_value.__enter__.return_value = mock_f

        mock_pd_csv.read_csv.return_value = self.input_data()

        df_result = read_hdfs_csv("file/path/filename.csv")

        mock_pd_csv.read_csv.assert_called_with(mock_f)

        df_expout = self.expout_data()
        pd.testing.assert_frame_equal(df_result, df_expout)
