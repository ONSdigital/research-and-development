"""Unit testing module."""
# Import testing packages
from unittest import mock

# Import modules to test
from src.data_ingest.loading import hdfs_load_json


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

        assert json_result == json_expout
