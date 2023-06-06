import pandas as pd
from typing import Tuple

# Import modules to test
from data_ingest.spp_parser import parse_snap_data


class TestParseSPP:
    """Test for Parse Snap data function"""

    def input_data(self) -> dict:
        dummy_snapdata = {
            "snapshot_id": "",
            "contributors": [
                {"ref": "123", "con": "789"},
                {"ref": "456", "con": "910"},
            ],
            "responses": [{"ref": "123", "res": "789"}, {"ref": "456", "res": "910"}],
        }

        return dummy_snapdata

    def exp_output(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        contributor_df = pd.DataFrame(
            [{"ref": "123", "con": "789"}, {"ref": "456", "con": "910"}]
        )

        responses_df = pd.DataFrame(
            [{"ref": "123", "res": "789"}, {"ref": "456", "res": "910"}]
        )

        return contributor_df, responses_df

    def test_parse_snap_data(self):
        """Tests for full_responses function."""

        inputdata = self.input_data()
        df_result1, df_result2 = parse_snap_data(inputdata)

        expected_output_data1, expected_output_data2 = self.exp_output()

        pd.testing.assert_frame_equal(df_result1, expected_output_data1)
        pd.testing.assert_frame_equal(df_result2, expected_output_data2)
