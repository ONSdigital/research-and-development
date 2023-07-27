from unittest import mock

# from unittest.mock import MagicMock, patch
import pandas as pd
from pandas.testing import assert_frame_equal
from src.data_ingest.check_data_type import validate_data_schema


class TestValidateDataSchema:
    """Unite test the validate_data_schema"""

    @mock.patch("src.data_ingest.check_data_type.open")
    # @mock.patch("src.data_ingest.check_data_type.json.load")
    @mock.patch("src.data_ingest.check_data_type.toml.load")
    # @mock.patch("src.data_ingest.check_data_type.jsonschema.validate")
    def test_validate_json_schema(self, mock_toml, mock_open):
        """Test the validate_json_shcema  to data types are correct in
        the source data
        """

        mock_open.return_value.read.return_value = mock.MagicMock()

        dumy_data = pd.DataFrame(
            {
                "col1": [2, 4, 6],
                "col2": ["Z", "Y", "V"],
                "col3": [2.6, 3.8, 4.9],
            }
        )

        mock_toml_load = {"col1": "int", "col2": "str", "col3": "float"}
        mock_toml.return_value = mock_toml_load

        expected_data = pd.DataFrame(
            {
                "col1": [2, 4, 6],
                "col2": ["Z", "Y", "V"],
                "col3": [2.6, 3.8, 4.9],
            }
        )

        actual_data = validate_data_schema(dumy_data, "mock_toml_load.toml")

        assert_frame_equal(actual_data, expected_data)

        # pd.testing.assert_frame_equal(actual_data, expected_data)
