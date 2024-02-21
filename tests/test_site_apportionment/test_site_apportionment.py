import pandas as pd
import numpy as np
import pytest
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal, assert_series_equal

from src.site_apportionment.site_apportionment import (
    create_notnull_mask,
    count_unique_postcodes_in_col,
)


# Global variables
postcode_col = "601"
groupby_cols = ["reference", "period"]


class TestCreateNotnullMask:
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
        df = pd.DataFrame(
            {"reference": [0, 1, 2, 3, 4], "col": [1, "a", "", pd.NA, np.nan]}
        )

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
    """
    Creates a sample DataFrame using predefined data and columns.

    Returns:
        pd.DataFrame: The created DataFrame.
    """
    sample_cols = ["reference", "period", "601", "headcount_dummy", "211_dummy"]

    sample_data = [
    [4990000000, 202212, "AB1 2CD", 5663, 8855030],
    [4990000000, 202212, "B27 2CD", 242, 5949501],
    [4990000084, 202212, "EF3 4GH", 8020, 5085659],
    [4990000126, 202212, "IJ5 6KL", 3877, 5808144],
    [4990000126, 202212, "MN7 8OP", 2756, 8889091],
    [4990000126, 202212, "QR9 10ST", 8832, 4722201],
    [4990000252, 202212, "UV1 2WX", 7084, 7058606],
    [4990000294, 202212, "YZ3 4AB", 3188, 1828472],
    [4990000336, 202212, "CD5 6EF", 62, 3262548],
    [4990000378, 202212, "GH7 8IJ", 1180, 9348647]
    ]

    
    return pd.DataFrame(data=sample_data, columns=sample_cols)


def test_count_unique_postcodes_in_col_positive():
    df = create_sample_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Expected output
    expected_cols = ['reference', 'period', '601', 'headcount_dummy', '211_dummy', '601_count']
    
    
    expected_data = [
        [4990000000, 202212, 'AB1 2CD', 5663, 8855030, 2],
        [4990000000, 202212, 'B27 2CD', 242, 5949501, 2],
        [4990000084, 202212, 'EF3 4GH', 8020, 5085659, 1],
        [4990000126, 202212, 'IJ5 6KL', 3877, 5808144, 3],
        [4990000126, 202212, 'MN7 8OP', 2756, 8889091, 3],
        [4990000126, 202212, 'QR9 10ST', 8832, 4722201, 3],
        [4990000252, 202212, 'UV1 2WX', 7084, 7058606, 1],
        [4990000294, 202212, 'YZ3 4AB', 3188, 1828472, 1],
        [4990000336, 202212, 'CD5 6EF', 62, 3262548, 1],
        [4990000378, 202212, 'GH7 8IJ', 1180, 9348647, 1]]
    
    expected_df = pd.DataFrame(data=expected_data, columns=expected_cols)

    # Check if the output is as expected
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def create_sample_missing_postcodes_df():
    """
    Creates a sample DataFrame using predefined data and columns.

    Returns:
        pd.DataFrame: The created DataFrame.
    """
    sample_cols = ["reference", "period", "601", "headcount_dummy", "211_dummy"]

    sample_data = [
    [4990000000, 202212, "B27 2CD", 5663, 8855030],
    [4990000000, 202212, "B27 2CD", 242, 5949501], #repeated postcode
    [4990000084, 202212, "EF3 4GH", 8020, 5085659],
    [4990000252, 202212, "UV1 2WX", 7084, 7058606],
    [4990000294, 202212, "YZ3 4AB", 3188, 1828472],
    [4990000336, 202212, "CD5 6EF", 62, 3262548],
    [4990000378, 202212, "GH7 8IJ", 1180, 9348647]
    ]

    
    return pd.DataFrame(data=sample_data, columns=sample_cols)

def test_count_unique_postcodes_in_col_negative_1():
    """
    Test the function count_unique_postcodes_in_col."""
    df = create_sample_missing_postcodes_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Expected output
    expected_cols = ['reference', 'period', '601', "headcount_dummy", "211_dummy", '601_count']
    
    expected_data = [
        [4990000000, 202212, "B27 2CD", 5663, 8855030, 2],
        [4990000001, 202212, "B27 2CD", 242, 5949501, 2],
        [4990000002, 202212, "EF3 4GH", 8020, 5085659, 2],
        [4990000006, 202212, 'UV1 2WX', 7084, 7058606, 2],
        [4990000007, 202212, 'YZ3 4AB', 3188, 1828472, 2],
        [4990000008, 202212, 'CD5 6EF', 62, 3262548, 2],
        [4990000009, 202212, 'GH7 8IJ', 1180, 9348647, 2]
    ]
    
    expected_df = pd.DataFrame(data=expected_data, columns=expected_cols)

    # Check if the output is not as expected
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(result_df, expected_df)


def create_sample_blank_postcodes_df():
    
    sample_cols = ["reference", "period", "601", "headcount_dummy", "211_dummy"]
    
    sample_data =    [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144],
        [4990000126, 202212, "",        3877, 5808144], #blank string as postcode
        [4990000126, 202212, "   ",     3877, 5808144], # whitespace as postcode
        [4990000126, 202212, None,      3877, 5808144] 
        ]
    
    # Create the DataFrame
    return pd.DataFrame(data=sample_data, columns=sample_cols)
    
def test_count_unique_postcodes_in_col_blank_postcodes():
    """This test checks that the function count_unique_postcodes_in_col can handle blank postcodes.
    
    The blanks are represented by an empty string and a string with only whitespace.
    
    Typical representations of missing data in Pandas 'object' columns are empty strings 
    and None, so None is also tested for.
    """

    sample_df = create_sample_blank_postcodes_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(sample_df)

    # Expected output
    expected_cols = ['reference', 'period', '601', "headcount_dummy", "211_dummy", '601_count']
    
    expected_data = [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144, 1],
        [4990000126, 202212, "",        3877, 5808144, 1],
        [4990000126, 202212, "   ",     3877, 5808144, 1],
        [4990000126, 202212, None,      3877, 5808144, 1] 
    ]
    
    # Make sure the expected data is in a DataFrame
    expected_df = pd.DataFrame(data=expected_data, columns=expected_cols)
    
    # Check if the output is as expected
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

def create_sample_blank_postcodes_df():
    
    sample_cols = ["reference", "period", "601", "headcount_dummy", "211_dummy"]
    
    nan_as_str = str(np.nan)
    
    sample_data =    [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144],
        [4990000126, 202212, np.nan,        3877, 5808144], # Numpy nan as postcode
        [4990000126, 202212, nan_as_str,     3877, 5808144], # string representation of numpy nan as postcode
        [4990000126, 202212, None,           3877, 5808144], # None as postcode
        ]
    
    # Create the DataFrame
    return pd.DataFrame(data=sample_data, columns=sample_cols)

def test_count_unique_postcodes_in_col_nan_postcodes():
    """This test checks that the function count_unique_postcodes_in_col can handle NaN postcodes.
    
    The NaNs are represented by np.nan and the string representation of np.nan, 'nan'.
    
    As the data is read from a file, received from SPP, missing values may be 
     represented as the string 'nan' so this might be worth testing for.
    """

    sample_df = create_sample_blank_postcodes_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(sample_df)

    # Expected output
    expected_cols = ['reference', 'period', '601', "headcount_dummy", "211_dummy", '601_count']
    
    nan_as_str = str(np.nan)
    
    expected_data = [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144, 1],
        [4990000126, 202212, np.nan,        3877, 5808144, 1],
        [4990000126, 202212, nan_as_str,     3877, 5808144, 1],
    ]
    
    # Make sure the expected data is in a DataFrame
    expected_df = pd.DataFrame(data=expected_data, columns=expected_cols)
    
    # Check if the output is as expected
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
    
def create_sample_missing_columns_df():
    """
    Creates a sample DataFrame using predefined data and columns.

    Returns:
        pd.DataFrame: The created DataFrame.
    """
    # Create the columns missing the postcode column "601"
    sample_cols = ["reference", "period", "headcount_dummy", "211_dummy"]

    # This data is missing the postcode column
    sample_data = [
    [4990000000, 202212, 5663, 8855030],
    [4990000000, 202212, 242, 5949501],
    [4990000084, 202212, 8020, 5085659],
    [4990000126, 202212, 3877, 5808144],
    [4990000126, 202212, 2756, 8889091],
    [4990000126, 202212, 8832, 4722201],
    [4990000252, 202212, 7084, 7058606],
    [4990000294, 202212, 3188, 1828472],
    [4990000336, 202212, 62, 3262548],
    [4990000378, 202212, 1180, 9348647]
    ]

    return pd.DataFrame(data=sample_data, columns=sample_cols)


def test_count_unique_postcodes_in_col_negative_2():
    """Checks that an error is raised when the postcode column is missing."""

    df = create_sample_missing_columns_df()
    
    # Apply the function
    with pytest.raises(KeyError):
        result_df = count_unique_postcodes_in_col(df)


# # Function to create a sample dataframe
# def create_df_with_missing_postcodes_df():
#     np.random.seed(0)  # for reproducibility
#     data = {
#         'reference': [4990000000 + i for i in range(10)],
#         'period': [202212 for _ in range(10)],
#         postcode_col: ['AB1 2CD', '', ' ', np.nan, 'MN7 8OP', 'QR9 10ST', 'UV1 2WX', 'YZ3 4AB', 'CD5 6EF', 'GH7 8IJ'],
#         'headcount_dummy': np.random.randint(1, 100, size=10),
#         '211_dummy': np.random.randint(1, 100, size=10)
#     }
#     return pd.DataFrame(data)

# def test_count_unique_postcodes_null_codes():
#     df = create_df_with_missing_postcodes_df()

#     # Apply the function
#     result_df = count_unique_postcodes_in_col(df)

#     # Expected output
#     expected_data = {
#         'reference': [4990000000, 4990000004, 4990000005, 4990000006, 4990000007, 4990000008, 4990000009],
#         'period': [202212, 202212, 202212, 202212, 202212, 202212, 202212],
#         postcode_col: ['AB1 2CD', 'MN7 8OP', 'QR9 10ST', 'UV1 2WX', 'YZ3 4AB', 'CD5 6EF', 'GH7 8IJ'],
#         'postcode_col_count': [1, 1, 1, 1, 1, 1, 1]
#     }
#     expected_df = pd.DataFrame(expected_data)

#     # Check if the output is as expected
#     pd.testing.assert_frame_equal(result_df, expected_df)
