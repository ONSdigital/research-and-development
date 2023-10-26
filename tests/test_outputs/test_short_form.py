import numpy as np
import pandas as pd
import unittest
from pandas._testing import assert_frame_equal

from src.outputs.short_form import create_period_year, create_headcount_cols


class TestCreateNewCols(unittest.TestCase):
    """Unittest for create news columns"""

    def data_frame(self):
        # create sample data
        data = {"period": ["202201", "202206", "202109", "202112"]}

        df = pd.DataFrame(data)
        return df

    def test_create_new_cols(self):
        # Call the create_new_cols funtion
        df_input = self.data_frame()
        actual_result = create_period_year(df_input)

        expected_result = pd.DataFrame(
            {
                "period": ["202201", "202206", "202109", "202112"],
                "period_year": ["2022", "2022", "2021", "2021"],
            }
        )

        assert_frame_equal(actual_result, expected_result)


class TestCreateHeadcountCols:
    """Test for create_headcount_cols function."""

    def create_input_df(self):
        """Create input dataframe."""
        data = {
            "reference": [1, 2, 3, 4, 5, 6],
            "705": [100, 0, 200, 300, np.nan, 400],
            "706": [20, 0, 80, 0, 10, 20],
            "707": [10, 0, 0, 0, np.nan, np.nan],
        }

        input_data_df = pd.DataFrame(data)
        return input_data_df

    def create_expected_df(self):
        """Create expected output dataframe."""
        data = {
            "reference": [1, 2, 3, 4, 5, 6],
            "705": [100, 0, 200, 300, np.nan, 400],
            "706": [20, 0, 80, 0, 10, 20],
            "707": [10, 0, 0, 0, np.nan, np.nan],
            "headcount_civil": [66.6667, 0, 200, 0, np.nan, np.nan],
            "headcount_defence": [33.3333, 0, 0, 0, np.nan, np.nan],
        }

        expected_data_df = pd.DataFrame(data)
        return expected_data_df

    def test_create_headcount_cols(self):
        """Test create_headcount_cols function.

        The test checks that the value in col 705 is correctly proportioned
        using the values in columns 706 and 707.
        Zero values should also be returned if both 706 and 707 are zero.
        Behaviour for nulls is checked.
        Rounding is also checked.
        """
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        result_df = create_headcount_cols(input_df, 4)

        assert_frame_equal(result_df, expected_df)
