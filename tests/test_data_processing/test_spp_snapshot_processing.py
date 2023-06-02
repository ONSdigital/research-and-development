"""Unit testing module."""
# Import testing packages
import pandas as pd


class TestFullResponses:
    """Tests for full_responses function."""

    def test_full_responses(self):

        # Import modules to test
        from src.data_processing.spp_snapshot_processing import full_responses

        contributor_data = pd.DataFrame(
            {
                "reference": [101, 102],
                "period": [202012, 202012],
                "survey": [1, 1],
                "createdby": ["A", "A"],
                "createddate": [2020, 2020],
                "lastupdatedby": ["A", "A"],
                "lastupdateddate": [2020, 2020],
            }
        )

        responses_data = pd.DataFrame(
            {
                "reference": [101, 101, 101, 102, 102, 102],
                "period": [202012, 202012, 202012, 202012, 202012, 202012],
                "survey": [1, 1, 1, 1, 1, 1],
                "createdby": ["A", "A", "A", "A", "A", "A"],
                "createddate": [2020, 2020, 2020, 2020, 2020, 2020],
                "lastupdatedby": ["A", "A", "A", "A", "A", "A"],
                "lastupdateddate": [2020, 2020, 2020, 2020, 2020, 2020],
                "questioncode": [200, 201, 202, 200, 201, 202],
                "response": [0, 50, 100, 75, 25, 65],
                "adjustedresponse": ["", "", "", "", "", ""],
            }
        )

        expected_output = pd.DataFrame(
            {
                "reference": [101, 102],
                "period": [202012, 202012],
                "survey": [1, 1],
                200: [0, 75],
                201: [50, 25],
                202: [100, 65],
            }
        )

        df_result = full_responses(contributor_data, responses_data)

        pd.testing.assert_frame_equal(df_result, expected_output)
