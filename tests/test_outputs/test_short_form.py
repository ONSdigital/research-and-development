import numpy as np
import pandas as pd

from pandas._testing import assert_frame_equal

from src.outputs.short_form import create_headcount_cols


class TestCreateHeadcountCols:
    """Test for create_headcount_cols function."""

    def create_input_data(self):
        """Create input data as list of columns and list of rows."""
        columns = [
            "reference", "701", "702", "703", "704", "705", "706", "707", "709", "710", "711", "formtype", "status"
        ]
        data = [
            [1, np.nan, np.nan, np.nan, np.nan, 100.0, 20.0, 10.0, np.nan, np.nan, np.nan, "0006", "Clear"],
            [2, np.nan, np.nan, np.nan, np.nan, 0.0, 0.0, 0.0, np.nan, np.nan, np.nan, "0006", "Clear"],
            [3, np.nan, np.nan, np.nan, np.nan, 200.0, 80.0, 0.0, np.nan, np.nan, np.nan, "0006", "Clear"],
            [4, np.nan, np.nan, np.nan, np.nan, 300.0, 0.0, 0.0, np.nan, np.nan, np.nan, "0006", "Clear"],
            [5, np.nan, np.nan, np.nan, np.nan, np.nan, 10.0, np.nan, np.nan, np.nan, np.nan, "0006", "Clear"],
            [6, np.nan, np.nan, np.nan, np.nan, 400, 20, np.nan, np.nan, np.nan, np.nan, "0006", "Clear"]
        ]
        input_data_df = pd.DataFrame(data=data, columns=columns)
        return input_data_df

    def create_expected_df(self):
        """Create expected data as list of columns and list of rows."""
        columns = [
            "reference", "701", "702", "703", "704", "705", "706", "707", "709", "710", "711", "formtype", "status", "headcount_civil", "headcount_defence"
        ]
        data = [
            [1, 0.0, 0.0, 0.0, 0.0, 100.0, 20.0, 10.0, 0.0, 0.0, 0.0, "0006", "Clear", 66.6667, 33.3333],
            [2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "0006", "Clear", 0.0, 0.0],
            [3, 0.0, 0.0, 0.0, 0.0, 200.0, 80.0, 0.0, 0.0, 0.0, 0.0, "0006", "Clear", 200.0, 0.0],
            [4, 0.0, 0.0, 0.0, 0.0, 300.0, 0.0, 0.0, 0.0, 0.0, 0.0, "0006", "Clear", 0.0, 0.0],
            [5, 0.0, 0.0, 0.0, 0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 0.0, "0006", "Clear", 0.0, 0.0],
            [6, 0.0, 0.0, 0.0, 0.0, 400, 20, 0.0, 0.0, 0.0, 0.0, "0006", "Clear", 400, 0]
        ]
        expected_data_df = pd.DataFrame(data=data, columns=columns)
        return expected_data_df

    def test_create_headcount_cols(self):
        """Test create_headcount_cols function.

        The test checks that the value in col 705 is correctly proportioned
        using the values in columns 706 and 707.
        Zero values should also be returned if both 706 and 707 are zero.
        Behaviour for nulls is checked.
        Rounding is also checked.
        """
        input_df = self.create_input_data()
        expected_df = self.create_expected_df()

        result_df = create_headcount_cols(input_df, 4)

        assert_frame_equal(result_df, expected_df)
