import pandas as pd
import numpy as np
import pytest
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal, assert_series_equal

from src.site_apportionment.site_apportionment import (
    create_notnull_mask,
    set_percentages,
    split_sites_df,
)

@pytest.fixture
def create_exp_percent_test_output_df():
    """Create a dataframe for expected output of the for the set_percentages function test.
    
    NOTE: this dataframe will also be the input for the split_sites function test.
    """
    exp_output_columns = [
        "reference",
        "instance",
        "formtype",
        "601",
        "602",
        "601_count",
        "status",
        "imp_marker",
        "postcodes_harmonised",
    ]
        
    data = [
        [1, 0, "0006", np.nan, 100.0, np.nan, "Clear", "R", "NP10 5XX"],
        [1, 1, "0006", np.nan, 100.0, np.nan, "Clear", "R", "NP10 5XX"],
        [1, 2, "0006", np.nan, 100.0, np.nan, "Clear", "R", "NP10 5XX"],
        [2, 0, "0001", np.nan, np.nan, 2.0, "Clear", "R", "NP20 6YY"],
        [2, 1, "0001", "CB1 3NF", 60.0, 2.0, "Clear", "R", "CB1 3NF"],
        [2, 2, "0001", "BA1 5DA", 40.0, 2.0, "Clear", "R", "BA1 5DA"],
        [3, 0, "0001", np.nan, np.nan, 1.0, "Check needed", "TMI", "NP30 7ZZ"],
        [3, 1, "0001", "DE72 3AU", 100.0, 1.0, "Check needed", "TMI", "DE72 3AU"],
        [3, 2, "0001", np.nan, np.nan, 1.0, "Check needed", "No mean found", "NP30 7ZZ"],
        [4, 1, "0001", "CF10 BZZ", 100.0, 1.0, "Form sent out", "TMI", "CF10 BZZ"],
        [5, 1, "0001", "SA50 5BE", 100.0, 1.0, "Form sent out", "No mean found", "SA50 5BE"],
    ]   

    exp_output_df = pandasDF(data=data, columns=exp_output_columns)
    return exp_output_df
    

class TestSetPercentages():
    """Tests for the set_percentages function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "reference",
            "instance",
            "formtype",
            "601",
            "602",
            "601_count",
            "status",
            "imp_marker",
            "postcodes_harmonised",
        ]
            
        data = [
            [1, 0, "0006", np.nan, np.nan, np.nan, "Clear", "R", "NP10 5XX"],
            [1, 1, "0006", np.nan, np.nan, np.nan, "Clear", "R", "NP10 5XX"],
            [1, 2, "0006", np.nan, np.nan, np.nan, "Clear", "R", "NP10 5XX"],
            [2, 0, "0001", np.nan, np.nan, 2.0, "Clear", "R", "NP20 6YY"],
            [2, 1, "0001", "CB1 3NF", 60.0, 2.0, "Clear", "R", "CB1 3NF"],
            [2, 2, "0001", "BA1 5DA", 40.0, 2.0, "Clear", "R", "BA1 5DA"],
            [3, 0, "0001", np.nan, np.nan, 1.0, "Check needed", "TMI", "NP30 7ZZ"],
            [3, 1, "0001", "DE72 3AU", np.nan, 1.0, "Check needed", "TMI", "DE72 3AU"],
            [3, 2, "0001", np.nan, np.nan, 1.0, "Check needed", "No mean found", "NP30 7ZZ"],
            [4, 1, "0001", np.nan, np.nan, np.nan, "Form sent out", "TMI", "CF10 BZZ"],
            [5, 1, "0001", np.nan, np.nan, np.nan, "Form sent out", "No mean found", "SA50 5BE"],
        ]   

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df
    
    def test_set_percentage(self, create_exp_percent_test_output_df):
        input_df = self.create_input_df()
        expected_output_df = create_exp_percent_test_output_df

        result_df = set_percentages(input_df)
        assert_frame_equal(result_df, expected_output_df)


class TestSplitSitesDf():
    """Tests for the split_sites_df function."""

    def create_exp_to_apportion_output(self):
        """Expected output for the to_apportion_df dataframe."""
        exp_output_cols1 = [
            "reference",
            "instance",
            "formtype",
            "601",
            "602",
            "601_count",
            "status",
            "imp_marker",
            "postcodes_harmonised",
        ]
            
        data1 = [
            [2, 1, "0001", "CB1 3NF", 60.0, 2.0, "Clear", "R", "CB1 3NF"],
            [2, 2, "0001", "BA1 5DA", 40.0, 2.0, "Clear", "R", "BA1 5DA"],
            [3, 1, "0001", "DE72 3AU", 100.0, 1.0, "Check needed", "TMI", "DE72 3AU"],
            [3, 2, "0001", np.nan, np.nan, 1.0, "Check needed", "No mean found", "NP30 7ZZ"],
            [4, 1, "0001", "CF10 BZZ", 100.0, 1.0, "Form sent out", "TMI", "CF10 BZZ"],
            [5, 1, "0001", "SA50 5BE", 100.0, 1.0, "Form sent out", "No mean found", "SA50 5BE"],
    ]   

        exp_output_df1 = pandasDF(data=data1, columns=exp_output_cols1)
        return exp_output_df1
    
    def create_exp_remaining_output(self):
        """Expected output for the df_out dataframe.
        
        This should contain all the records not in the to_apportion dataframe,
        except those with imp_marker = "No mean found" or "no_imputation"
        """
        exp_output_cols2 = [
            "reference",
            "instance",
            "formtype",
            "601",
            "602",
            "601_count",
            "status",
            "imp_marker",
            "postcodes_harmonised",
        ]
            
        data2 = [
            [1, 0, "0006", np.nan, 100.0, np.nan, "Clear", "R", "NP10 5XX"],
            [1, 1, "0006", np.nan, 100.0, np.nan, "Clear", "R", "NP10 5XX"],
            [1, 2, "0006", np.nan, 100.0, np.nan, "Clear", "R", "NP10 5XX"],
            [2, 0, "0001", np.nan, np.nan, 2.0, "Clear", "R", "NP20 6YY"],
            [3, 0, "0001", np.nan, np.nan, 1.0, "Check needed", "TMI", "NP30 7ZZ"],
        ]   
        exp_output_df2 = pandasDF(data=data2, columns=exp_output_cols2)
        exp_output_df2 = exp_output_df2.astype({"601": object})
        return exp_output_df2
    
    def test_split_sites_df(self, create_exp_percent_test_output_df):
        """Test for the split_sites_df function."""
        input_df = create_exp_percent_test_output_df
        exp_to_apportion_df = self.create_exp_to_apportion_output()
        exp_df_out = self.create_exp_remaining_output()

        imp_markers_to_keep: list = ["R", "TMI", "CF", "MoR", "constructed"]
        result_to_apportion, result_df_out = split_sites_df(
            input_df, 
            imp_markers_to_keep
        )

        assert_frame_equal(result_to_apportion.reset_index(drop=True), exp_to_apportion_df)

        assert_frame_equal(result_df_out.reset_index(drop=True), exp_df_out)


class TestCreateNotnullMask():
    """Tests for the function create_not_null_mask."""
    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = ["reference", "col"]
            
        data = [
            [0, 1],
            [111, "a"],
            [222, ""],
            [333, pd.NA],
            [4, np.nan],
            [5, " "],
        ]   

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df 


    def test_create_notnull_mask(self):
        """Test for create_notnull_mask function."""
        self.input_df = self.create_input_df()

        exp_output = pd.Series([False, True, False, False, False, True], name="col")

        result = create_notnull_mask(self.input_df, "col")
        assert_series_equal(result, exp_output)


    def create_demo_expected_df(self):
        """Create an input dataframe to show how other nulls work."""
        expected_columns = [
            "reference",
            "col",
            "this_test",
            "flag_is_null",
            "flag_is_emty",
            "flag_is_notnull",
            "flag_not_is_null",
            "len",
            "flag_length_greater_than_zero",
        ]
            
        data = [
            [0, 1, False, np.nan, np.nan, True, True, np.nan, np.nan],
            [111, "a", True, np.nan, np.nan, True, True, 1.0, True],
            [222, "", False, np.nan, True, True, True, 0.0, np.nan],
            [333, pd.NA, False, True, np.nan, np.nan, np.nan, np.nan, np.nan],
            [4, np.nan, False, True, np.nan, np.nan, np.nan, np.nan, np.nan],
            [5, " ", True, np.nan, np.nan, True, True, 1.0, True],    
        ]   

        expected_df = pandasDF(data=data, columns=expected_columns)
        return expected_df  

    def test_other_nulls_demo(self):
        """Demonstrate how this function works and compare with other tests for null.
        
        The column under test has the following values:
        [111, "a", "", pd.NA, np.nan, " "]
        """
        demo_df = self.create_input_df()

        # here's the original test again
        result = create_notnull_mask(demo_df, "col")
        exp_output = pd.Series([False, True, False, False, False, True], name="col")
        assert_series_equal(result, exp_output)

        # test the effect of apply applying .isnull() to column col
        result_not_isnull = ~demo_df["col"].isnull()
        # note that "" does not register as null
        exp_isnull = pd.Series([True, True, True, False, False, True], name="col")
        assert_series_equal(result_not_isnull, exp_isnull)
 
        # test the effect of apply applying .notnull() to column col
        result_notnull = demo_df["col"].notnull()
        # note that "" registers as not null
        exp_notnull = pd.Series([True, True, True, False, False, True], name="col")
        assert_series_equal(result_notnull, exp_notnull)

        # test applying str.len() to column col
        result_len = demo_df["col"].str.len()
        exp_len = pd.Series([np.nan, 1.0, 0.0, np.nan, np.nan, 1.0], name="col")
        assert_series_equal(result_len, exp_len)

    def test_other_nulls_dataframe_demo(self):
        """Dataframe visualisation of tests for nulls.
        
        The column under test has the following values:
        [111, "a", "", pd.NA, np.nan, " "]
        """
        # set up an input dataframe 
        demo_df = self.create_input_df()

        # create new mask columns in demo_df for various null-checking functions
        demo_df.loc[demo_df["col"].isnull(), "flag_is_null"] = True
        demo_df.loc[demo_df["col"] == "", "flag_is_emty"] = True
        demo_df.loc[demo_df["col"].notnull(), "flag_is_notnull"] = True
        demo_df.loc[~demo_df["col"].isnull(), "flag_not_is_null"] = True
        demo_df["len"] = demo_df["col"].str.len()
        demo_df.loc[demo_df["len"] > 0, "flag_length_greater_than_zero"] = True

        # create the expected dataframe for these operations
        expected_columns = [
            "reference",
            "col",
            "flag_is_null",
            "flag_is_emty",
            "flag_is_notnull",
            "flag_not_is_null",
            "len",
            "flag_length_greater_than_zero",
        ]
            
        data = [
            [0, 1,      np.nan, np.nan, True,   True,   np.nan, np.nan],  #noqa
            [111, "a",    np.nan, np.nan, True,   True,   1.0,    True],  #noqa
            [222, "",     np.nan, True,   True,   True,   0.0,    np.nan],  #noqa
            [333, pd.NA,  True,   np.nan, np.nan, np.nan, np.nan, np.nan],  #noqa
            [4, np.nan, True,   np.nan, np.nan, np.nan, np.nan, np.nan],  #noqa
            [5, " ",    np.nan, np.nan, True,   True,   1.0,    True],  #noqa
        ]   

        expected_df = pandasDF(data=data, columns=expected_columns)

        # test that the operations in this function give the expected output
        assert_frame_equal(demo_df, expected_df)
