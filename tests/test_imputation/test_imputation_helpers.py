import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF
from pandas._testing import assert_series_equal, assert_frame_equal

from src.imputation.imputation_helpers import (
    copy_first_to_group,
    fix_604_error,
    create_r_and_d_instance,
    check_604_fix
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
            [1001, 1, "C", None],
            [1001, 2, "C", None],
            [1001, 3, "D", None],
            [2002, 0, None, "Yes"],
            [3003, 0, None, None],
            [3003, 1, "C", None],
            [3003, 2, "C", "Haha"],
            [3003, 3, "D", None],
            [4004, 0, None, None],
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
                None,
            ],
            name="604",
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
            [4004, 0, None, None, "0001"],
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
            [4004, 0, None, None, "0001"],
        ]

        expected_df = pandasDF(data=data, columns=input_cols)
        return expected_df

    def test_fix_604_error(self):
        """Test for function fix_604_error."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        result_df, qa_df = fix_604_error(input_df)
        assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    def test_check_604_fix(self):
        """Test for function check 604 fix"""
        # Create an input dataframe for the test
        input_cols = [
            "reference",
            "instance",
            "200",
            "604",
            "formtype",
        ]    
        input_data = [
            [1001, 0, None, "No", "0001"],
            [2002, 0, None, "Yes", "0001"],
            [3003, 0, None, "No", "0001"],
            [3003, 1, "C", "No", "0001"],
            [3003, 1, "C", "No", "0001"],
            [4004, 0, None, None, "0001"],
        ]
   
        exp_data = [
            [1001, 0, None, "No", "0001"],
            [2002, 0, None, "Yes", "0001"],
            [3003, 0, None, "No", "0001"],
            [3003, 1, "C", "No", "0001"],
            [4004, 0, None, None, "0001"],
        ]

        expected_check_cols = [
            "reference",
            "instance",
            "ref_count"
        ]   
        expected_check_data = [
            [3003, 1, 2],
            [3003, 1, 2],
        ]

        input_df = pandasDF(data=input_data, columns=input_cols)
        expected_df = pandasDF(data=exp_data, columns=input_cols)
        exp_check_df = pandasDF(data=expected_check_data, columns=expected_check_cols)

        result_df, check_df = check_604_fix(input_df)

        assert_frame_equal(result_df.reset_index(drop=True), expected_df)
        assert_frame_equal(check_df.reset_index(drop=True), exp_check_df)


class TestCreateRAndDInstance:
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
            [4004, 0, None, None, "0001"],
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
            [4004, 0, None, None, "0001"],
        ]

        expected_df = pandasDF(data=data, columns=input_cols)
        return expected_df

    def test_create_r_and_d_instance(self):
        """Test for function fix_604_error."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        result_df, qa_df = create_r_and_d_instance(input_df)
        assert_frame_equal(result_df.reset_index(drop=True), expected_df)
