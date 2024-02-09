import numpy as np
import pandas as pd
import unittest
from pandas._testing import assert_frame_equal

from src.outputs.short_form import create_headcount_cols


class TestCreateHeadcountCols:
    """Test for create_headcount_cols function."""

    def create_input_df(self):
        """Create input dataframe."""
        data = {
            "reference": [1, 2, 3, 4, 5, 6],
            "701": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "702": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "703": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "704": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "705": [100.0, 0.0, 200.0, 300.0, np.nan, 400],
            "706": [20.0, 0.0, 80.0, 0.0, 10.0, 20],
            "707": [10.0, 0.0, 0.0, 0.0, np.nan, np.nan],
            "709": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "710": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "711": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "formtype": ["0006", "0006", "0006", "0006", "0006", "0006"],
            "status": ["Clear", "Clear", "Clear", "Clear", "Clear", "Clear"],
        }

        input_data_df = pd.DataFrame(data)
        return input_data_df

    def create_expected_df(self):
        """Create expected output dataframe."""
        data = {
            "reference": [1, 2, 3, 4, 5, 6],
            "701": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "702": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "703": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "704": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "705": [100.0, 0.0, 200.0, 300.0, 0.0, 400],
            "706": [20.0, 0.0, 80.0, 0.0, 10.0, 20],
            "707": [10.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "709": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "710": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "711": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "formtype": ["0006", "0006", "0006", "0006", "0006", "0006"],
            "status": ["Clear", "Clear", "Clear", "Clear", "Clear", "Clear"],
            "headcount_civil": [66.6667, 0.0, 200.0, 0.0, 0.0, 400],
            "headcount_defence": [33.3333, 0.0, 0.0, 0.0, 0.0, 0],
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
