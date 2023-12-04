import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF
from pandas._testing import assert_series_equal, assert_frame_equal

from src.imputation.imputation_helpers import (
    copy_first_to_group, 
    fix_604_error, 
    create_r_and_d_instance,
)

class TestCopyFirstToGroup:
    """Unit tests for copy_first_to_group function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "604",
        ]

        data = [
            [1001, 0, None, "No"],
            [1001, 1, "C", np.nan],
            [1001, 2, "C", np.nan],
            [1001, 3, "D", np.nan],
            [2002, 0, None, "Yes"],
            [3003, 0, None, np.nan],
            [3003, 1, "C", np.nan],
            [3003, 2, "C", "Haha"],
            [3003, 3, "D", np.nan],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df
    
    def test_copy_first_to_group(self):
        """Test for function copy_first_to_group."""
        input_df = self.create_input_df()

        expected_output = pd.Series(
            [
                "No",
                "No",
                "No",
                "No",
                "Yes",
                "Haha",
                "Haha",
                "Haha",
                "Haha",
            ], name = "604"
        )

        result_df = copy_first_to_group(input_df, "604")
        assert_series_equal(result_df, expected_output)


class TestFix604Error:
    """Unit tests for fix_604_error function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "604",
            "formtype",
        ]

        data = [
            [1001, 0, None, "No", "0001"],
            [1001, 1, "C", np.nan, "0001"],
            [1001, 2, "C", np.nan, "0001"],
            [1001, 3, "D", np.nan, "0001"],
            [2002, 0, None, "Yes", "0001"],
            [3003, 0, None, np.nan, "0001"],
            [3003, 1, "C", np.nan, "0001"],
            [3003, 2, "C", "Haha", "0001"],
            [3003, 3, "D", np.nan, "0001"],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df
    
    def create_expected_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "604",
            "formtype",
        ]

        data = [
            [1001, 0, None, "No", "0001"],
            [2002, 0, None, "Yes", "0001"],
            [3003, 0, None, "Haha", "0001"],
            [3003, 1, "C", "Haha", "0001"],
            [3003, 2, "C", "Haha", "0001"],
            [3003, 3, "D", "Haha", "0001"],
        ]

        expected_df = pandasDF(data=data, columns=input_cols)
        return expected_df
    
    def test_fix_604_error(self):
        """Test for function fix_604_error."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        result_df = fix_604_error(input_df)
        assert_frame_equal(result_df.reset_index(drop=True), expected_df)


class TestFix604Error:
    """Unit tests for create_r_and_d_instance function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "604",
            "formtype",
        ]

        data = [
            [1001, 0, None, "No", "0001"],
            [1001, 1, "C", np.nan, "0001"],
            [1001, 2, "C", np.nan, "0001"],
            [1001, 3, "D", np.nan, "0001"],
            [2002, 0, None, "Yes", "0001"],
            [2002, 1, "C", "Yes", "0001"],
            [3003, 0, None, "No", "0001"],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df
    
    def create_expected_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "604",
            "formtype",
        ]

        data = [
            [1001, 0, None, "No", "0001"],
            [1001, 1, None, "No", "0001"],
            [2002, 0, None, "Yes", "0001"],
            [2002, 1, "C", "Yes", "0001"],
            [3003, 0, None, "No", "0001"],
            [3003, 1, None, "No", "0001"],
        ]

        expected_df = pandasDF(data=data, columns=input_cols)
        return expected_df
    
    def test_fix_604_error(self):
        """Test for function fix_604_error."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        result_df = create_r_and_d_instance(input_df)
        assert_frame_equal(result_df.reset_index(drop=True), expected_df)
