import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal, assert_series_equal

from src.site_apportionment.site_apportionment import create_notnull_mask


class TestCreateNotnullMask():

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
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
            [0, 1, np.nan, np.nan, True, True, np.nan, np.nan],
            [1, "a", np.nan, np.nan, True, True, 1.0, True],
            [2, "", np.nan, True, True, True, 0.0, np.nan],
            [3, pd.NA, True, np.nan, np.nan, np.nan, np.nan, np.nan],
            [4, np.nan, True, np.nan, np.nan, np.nan, np.nan, np.nan],
        ]   

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df  

    def create_exp_output(self):
        """Create a series for the expected output.""" 
        name = "col"
        data = [False, True, False, False, False] 
        return pd.Series(data, name=name)
    
    def test_create_notnull_mask(self):
        """Test for create_notnull_mask function."""
        df=pd.DataFrame(
            {"reference": [0, 1, 2, 3, 4],
            "col":[1, "a", "", pd.NA, np.nan]})

        # add extra cols to the df to illustrate how logic of nulls
        df.loc[df["col"].isnull(), "flag_is_null"] = True
        df.loc[df["col"] == "", "flag_is_emty"] = True
        df.loc[df["col"].notnull(), "flag_is_notnull"] = True
        df.loc[~df["col"].isnull(), "flag_not_is_null"] = True
        df["len"] = df["col"].str.len()
        df.loc[df["len"] > 0, "flag_length_greater_than_zero"] = True

        input_df = self.create_input_df()
        assert_frame_equal(df, input_df)

        result = create_notnull_mask(input_df, "col")
        expected_output = self.create_exp_output()
        assert_series_equal(result, expected_output)
