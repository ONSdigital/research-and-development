"""Unit testing module."""
# Import testing packages
from unittest import mock
import pandas as pd

# Import modules to test
from src.data_ingest.loading import hdfs_load_json


class TestLoadJson:
    """Tests for hdfs json loader."""

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

    @mock.patch("src.data_ingest.loading.json")
    @mock.patch("src.data_ingest.loading.hdfs")
    def test_hdfs_load_json(self, mock_hdfs, mock_json):
        """Test the expected functionality."""

        mock_f = mock.Mock()
        mock_hdfs.open.return_value.__enter__.return_value = mock_f

        mock_json.load.return_value = self.input_data()

        json_result = hdfs_load_json("file/path/filename.json")

        mock_json.load.assert_called_with(mock_f)

        json_expout = self.expout_data()
        pd.testing.assert_frame_equal(json_result, json_expout)
