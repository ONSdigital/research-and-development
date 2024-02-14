import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal, assert_series_equal

from src.site_apportionment.site_apportionment import create_notnull_mask


class TestCreateNotnullMask():
    """Tests for the function create_not_null_mask."""
    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = ["reference", "col"]
            
        data = [
            [0, 1],
            [1, "a"],
            [2, ""],
            [3, pd.NA],
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
            [1, "a", True, np.nan, np.nan, True, True, 1.0, True],
            [2, "", False, np.nan, True, True, True, 0.0, np.nan],
            [3, pd.NA, False, True, np.nan, np.nan, np.nan, np.nan, np.nan],
            [4, np.nan, False, True, np.nan, np.nan, np.nan, np.nan, np.nan],
            [5, " ", True, np.nan, np.nan, True, True, 1.0, True],    
        ]   

        expected_df = pandasDF(data=data, columns=expected_columns)
        return expected_df  

    def test_other_nulls_demo(self):
        """Demonstrate how this function works and compare with other tests for null.
        
        The column under test has the following values:
        [1, "a", "", pd.NA, np.nan, " "]
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
        [1, "a", "", pd.NA, np.nan, " "]
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
            [0, 1,      np.nan, np.nan, True,   True,   np.nan, np.nan],
            [1, "a",    np.nan, np.nan, True,   True,   1.0,    True],
            [2, "",     np.nan, True,   True,   True,   0.0,    np.nan],
            [3, pd.NA,  True,   np.nan, np.nan, np.nan, np.nan, np.nan],
            [4, np.nan, True,   np.nan, np.nan, np.nan, np.nan, np.nan],
            [5, " ",    np.nan, np.nan, True,   True,   1.0,    True],
        ]   

        expected_df = pandasDF(data=data, columns=expected_columns)

        # test that the operations in this function give the expected output
        assert_frame_equal(demo_df, expected_df)
