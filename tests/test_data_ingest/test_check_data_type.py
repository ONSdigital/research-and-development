from unittest import mock
import pandas as pd
from pandas.testing import assert_frame_equal
from src.data_ingest.check_data_type import validate_json_shcema


class TestValidateJsonToDf:
    """Unite test the validate_json_shcema"""

    @mock.patch("src.data_ingest.check_data_type.open")
    @mock.patch("src.data_ingest.check_data_type.json.load")
    @mock.patch("src.data_ingest.check_data_type.toml.load")
    @mock.patch("src.data_ingest.check_data_type.jsonschema.validate")
    def test_validate_json_schema(
        self, mock_validate, mock_toml_load, mock_json_load, mock_open
    ):
        """Test the validate_json_shcema  to data types are correct in
        the source data
        """

        mock_open.return_value.read.return_value = mock.MagicMock()
        mock_json_load.return_value = {
            "col1": [2, 4, 6],
            "col2": ["Z", "Y", "V"],
            "col3": [2.6, 3.8, 4.9],
        }

        mock_toml_load.return_value = {"col1": "int", "col2": "str", "col3": "float"}

        expected_data = {
            "col1": [2, 4, 6],
            "col2": ["Z", "Y", "V"],
            "col3": [2.6, 3.8, 4.9],
        }
        expected_data = pd.DataFrame(expected_data)
        actual_data = validate_json_shcema(
            "file/truepath/filename.json", "file/truepath/schema.toml"
        )

        assert_frame_equal(actual_data, expected_data)

        mock_validate.assert_called_once_with(
            mock_json_load.return_value, mock_toml_load.return_value
        )
