import pandas as pd
import numpy as np
import pytest
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal, assert_series_equal

from src.site_apportionment.site_apportionment import create_notnull_mask, count_unique_postcodes_in_col


# Global variables
postcode_col = "601"
groupby_cols = ['reference', 'period']


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
        
# Function to create a sample dataframe
def create_sample_df():
        np.random.seed(43)  # for reproducibility
        data = {
            'reference': [4990000000 + i for i in range(0, 42*10, 42)],
            'period': [202212 for _ in range(0, 42*10, 42)],
            '601': ['AB1 2CD', 'AB1 2CD', 'EF3 4GH', 'IJ5 6KL', 'MN7 8OP', 'QR9 10ST', 'UV1 2WX', 'YZ3 4AB', 'CD5 6EF', 'GH7 8IJ'],
            'headcount_dummy': np.random.randint(1, 100, size=10),
            '211_dummy': np.random.randint(1, 100, size=10)
        } # dummy columns will be dropped in the count_unique_postcodes_in_col function
        return pd.DataFrame(data)

def test_count_unique_postcodes_in_col_positive():
    df = create_sample_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Expected output
    expected_data = {
        'reference': [4990000000 + i for i in range(0, 42*10, 42)],
        'period': [202212 for _ in range(0, 42*10, 42)],
        postcode_col: ['AB1 2CD', 'AB1 2CD', 'EF3 4GH', 'IJ5 6KL', 'MN7 8OP', 'QR9 10ST', 'UV1 2WX', 'YZ3 4AB', 'CD5 6EF', 'GH7 8IJ'],
        'postcode_col_count': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    }
    expected_df = pandasDF(expected_data)

    # Check if the output is as expected
    pd.testing.assert_frame_equal(result_df, expected_df)

def test_count_unique_postcodes_in_col_negative():
    df = create_sample_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Expected output
    expected_data = {
        'reference': [4990000000 + i for i in range(10)],
        'period': [202212 for _ in range(10)],
        postcode_col: ['AB1 2CD', 'AB1 2CD', 'EF3 4GH', 'IJ5 6KL', 'MN7 8OP', 'QR9 10ST', 'UV1 2WX', 'YZ3 4AB', 'CD5 6EF', 'GH7 8IJ'],
        'postcode_col_count': [2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    }
    expected_df = pandasDF(expected_data)

    # Check if the output is not as expected
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(result_df, expected_df)


# Function to create a sample dataframe
def create_df_with_missing_postcodes_df():
    np.random.seed(0)  # for reproducibility
    data = {
        'reference': [4990000000 + i for i in range(10)],
        'period': [202212 for _ in range(10)],
        postcode_col: ['AB1 2CD', '', ' ', np.nan, 'MN7 8OP', 'QR9 10ST', 'UV1 2WX', 'YZ3 4AB', 'CD5 6EF', 'GH7 8IJ'],
        'headcount_dummy': np.random.randint(1, 100, size=10),
        '211_dummy': np.random.randint(1, 100, size=10)
    }
    return pd.DataFrame(data)

def test_count_unique_postcodes_null_codes():
    df = create_df_with_missing_postcodes_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Expected output
    expected_data = {
        'reference': [4990000000, 4990000004, 4990000005, 4990000006, 4990000007, 4990000008, 4990000009],
        'period': [202212, 202212, 202212, 202212, 202212, 202212, 202212],
        postcode_col: ['AB1 2CD', 'MN7 8OP', 'QR9 10ST', 'UV1 2WX', 'YZ3 4AB', 'CD5 6EF', 'GH7 8IJ'],
        'postcode_col_count': [1, 1, 1, 1, 1, 1, 1]
    }
    expected_df = pd.DataFrame(expected_data)

    # Check if the output is as expected
    pd.testing.assert_frame_equal(result_df, expected_df)