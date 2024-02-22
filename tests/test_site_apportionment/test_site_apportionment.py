import pandas as pd
import numpy as np
import pytest
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal, assert_series_equal
from typing import List

from src.site_apportionment.site_apportionment import (
    create_notnull_mask,
    count_unique_postcodes_in_col,
    deduplicate_codes_values,
)  # create_category_df, keep_good_markers,


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
def create_category_test_df_2():
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
        [4990000378, 202212, "GH7 8IJ", 1180, 9348647],
    ]

    return pd.DataFrame(data=sample_data, columns=sample_cols)


def test_count_unique_postcodes_in_col_positive():
    df = create_category_test_df_2()

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Expected output
    expected_cols = [
        "reference",
        "period",
        "601",
        "headcount_dummy",
        "211_dummy",
        "601_count",
    ]

    expected_data = [
        [4990000000, 202212, "AB1 2CD", 5663, 8855030, 2],
        [4990000000, 202212, "B27 2CD", 242, 5949501, 2],
        [4990000084, 202212, "EF3 4GH", 8020, 5085659, 1],
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144, 3],
        [4990000126, 202212, "MN7 8OP", 2756, 8889091, 3],
        [4990000126, 202212, "QR9 10ST", 8832, 4722201, 3],
        [4990000252, 202212, "UV1 2WX", 7084, 7058606, 1],
        [4990000294, 202212, "YZ3 4AB", 3188, 1828472, 1],
        [4990000336, 202212, "CD5 6EF", 62, 3262548, 1],
        [4990000378, 202212, "GH7 8IJ", 1180, 9348647, 1],
    ]

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
        [4990000000, 202212, "B27 2CD", 242, 5949501],  # repeated postcode
        [4990000084, 202212, "EF3 4GH", 8020, 5085659],
        [4990000252, 202212, "UV1 2WX", 7084, 7058606],
        [4990000294, 202212, "YZ3 4AB", 3188, 1828472],
        [4990000336, 202212, "CD5 6EF", 62, 3262548],
        [4990000378, 202212, "GH7 8IJ", 1180, 9348647],
    ]

    return pd.DataFrame(data=sample_data, columns=sample_cols)


def test_count_unique_postcodes_in_col_negative_1():
    """
    Test the function count_unique_postcodes_in_col."""
    df = create_sample_missing_postcodes_df()

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Expected output
    expected_cols = [
        "reference",
        "period",
        "601",
        "headcount_dummy",
        "211_dummy",
        "601_count",
    ]

    expected_data = [
        [4990000000, 202212, "B27 2CD", 5663, 8855030, 2],
        [4990000001, 202212, "B27 2CD", 242, 5949501, 2],
        [4990000002, 202212, "EF3 4GH", 8020, 5085659, 2],
        [4990000006, 202212, "UV1 2WX", 7084, 7058606, 2],
        [4990000007, 202212, "YZ3 4AB", 3188, 1828472, 2],
        [4990000008, 202212, "CD5 6EF", 62, 3262548, 2],
        [4990000009, 202212, "GH7 8IJ", 1180, 9348647, 2],
    ]

    expected_df = pd.DataFrame(data=expected_data, columns=expected_cols)

    # Check if the output is not as expected
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(result_df, expected_df)


def create_sample_blank_postcodes_df():

    sample_cols = ["reference", "period", "601", "headcount_dummy", "211_dummy"]

    sample_data = [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144],
        [4990000126, 202212, "", 3877, 5808144],  # blank string as postcode
        [4990000126, 202212, "   ", 3877, 5808144],  # whitespace as postcode
        [4990000126, 202212, None, 3877, 5808144],
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
    expected_cols = [
        "reference",
        "period",
        "601",
        "headcount_dummy",
        "211_dummy",
        "601_count",
    ]

    expected_data = [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144, 1],
        [4990000126, 202212, "", 3877, 5808144, 1],
        [4990000126, 202212, "   ", 3877, 5808144, 1],
        [4990000126, 202212, None, 3877, 5808144, 1],
    ]

    # Make sure the expected data is in a DataFrame
    expected_df = pd.DataFrame(data=expected_data, columns=expected_cols)

    # Check if the output is as expected
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def create_sample_blank_postcodes_df():

    sample_cols = ["reference", "period", "601", "headcount_dummy", "211_dummy"]

    nan_as_str = str(np.nan)

    sample_data = [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144],
        [4990000126, 202212, np.nan, 3877, 5808144],  # Numpy nan as postcode
        [
            4990000126,
            202212,
            nan_as_str,
            3877,
            5808144,
        ],  # string representation of numpy nan as postcode
        [4990000126, 202212, None, 3877, 5808144],  # None as postcode
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
    expected_cols = [
        "reference",
        "period",
        "601",
        "headcount_dummy",
        "211_dummy",
        "601_count",
    ]

    nan_as_str = str(np.nan)

    expected_data = [
        [4990000126, 202212, "IJ5 6KL", 3877, 5808144, 1],
        [4990000126, 202212, np.nan, 3877, 5808144, 1],
        [4990000126, 202212, nan_as_str, 3877, 5808144, 1],
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
        [4990000378, 202212, 1180, 9348647],
    ]

    return pd.DataFrame(data=sample_data, columns=sample_cols)


def test_count_unique_postcodes_in_col_negative_2():
    """Checks that an error is raised when the postcode column is missing."""

    df = create_sample_missing_columns_df()

    # Apply the function
    with pytest.raises(KeyError):
        result_df = count_unique_postcodes_in_col(df)


def test_drop_duplicates():
    # Define the input DataFrame
    df = pd.DataFrame(
        {
            "groupby_col1": ["A", "A", "B", "B", "B"],
            "groupby_col2": ["X", "X", "Y", "Y", "Y"],
            "postcode_col": ["123", "123", "456", "456", "789"],
        }
    )

    # Define the expected output DataFrame
    expected_df = pd.DataFrame(
        {
            "groupby_col1": ["A", "B", "B"],
            "groupby_col2": ["X", "Y", "Y"],
            "postcode_col": ["123", "456", "789"],
        }
    )

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Check that the resulting DataFrame matches the expected output
    pd.testing.assert_frame_equal(result_df, expected_df)


def create_repitious_dataframe():
    # Define the data and columns separately
    sample_data = [
        [4990000000, 202212, "SW1A 1AA"],
        [4990000000, 202212, "SW1A 1AA"],
        [4990000126, 202212, "EC1A 1BB"],
        [4990000126, 202212, "EC1A 1BB"],
        [4990000126, 202212, "W1A 0AX"],
    ]
    sample_cols = ["reference", "period", "601"]

    # Create the input DataFrame
    df = pd.DataFrame(sample_data, columns=sample_cols)
    return df


def test_count_unique_postcodes_drop_duplicates():
    """This tests that the function can handle a DataFrame with repitious data.

    Within the function the repitious records are dropped, but later merged back on.
    The counting operation is tested here, and the ability of the function to add the
    original dropped data back onto the returned dataframe.
    """
    repitious_dataframe = create_repitious_dataframe()

    # Define the expected output data and columns
    expected_data = [
        [4990000000, 202212, "SW1A 1AA", 1],
        [4990000000, 202212, "SW1A 1AA", 1],
        [4990000126, 202212, "EC1A 1BB", 2],
        [4990000126, 202212, "EC1A 1BB", 2],
        [4990000126, 202212, "W1A 0AX", 2],
    ]
    expected_columns = ["reference", "period", "601", "601_count"]

    # Create the expected output DataFrame
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)

    # Apply the function
    result_df = count_unique_postcodes_in_col(repitious_dataframe)

    # Check that the resulting DataFrame matches the expected output
    pd.testing.assert_frame_equal(result_df, expected_df)


def create_not_uniformly_repitious_df():
    # Define the data and columns separately
    sample_data = [
        [4990000000, 202212, "SW1A 1AA", 10, 100],
        [4990000000, None, "SW1A 1AA", 10, 100],  # missing period
        [4990000126, 202212, "EC1A 1BB", 20, 200],
        [4990000126, 202212, "EC1A 1BB", None, 200],  # missing headcount
        [4990000126, 202212, "W1A 0AX", 20, 200],
        [4990000126, 202212, "W1A 0AX", None, None],  # missing headcount and 211
        [None, 202212, "W1A 0AX", 20, 200],  # missing reference
    ]
    sample_cols = ["reference", "period", "601", "dummy_headcount", "dummy_211"]

    # Create the input DataFrame
    df = pd.DataFrame(sample_data, columns=sample_cols)
    return df


def test_count_unique_postcodes_drop_duplicates_with_missing_values():
    """
    This test checks that the function count_unique_postcodes_in_col can handle missing values in the input DataFrame.
    """
    # Create the input DataFrame
    df = create_not_uniformly_repitious_df()

    # Define the expected output data and columns
    expected_data = [
        [4990000000, 202212, "SW1A 1AA", 10, 100, 1],
        [
            4990000000,
            np.nan,
            "SW1A 1AA",
            10,
            100,
            np.nan,
        ],  # same reference but different period
        [4990000126, 202212, "EC1A 1BB", 20, 200, 2],
        [4990000126, 202212, "EC1A 1BB", None, 200, 2],
        [4990000126, 202212, "W1A 0AX", 20, 200, 2],
        [4990000126, 202212, "W1A 0AX", None, None, 2],
        [
            None,
            202212,
            "W1A 0AX",
            20,
            200,
            np.nan,
        ],  # even though the postcode is repeated there is no reference
    ]
    expected_columns = [
        "reference",
        "period",
        "601",
        "dummy_headcount",
        "dummy_211",
        "601_count",
    ]

    # Create the expected output DataFrame
    expected_df = pd.DataFrame(expected_data, columns=expected_columns)

    # Apply the function
    result_df = count_unique_postcodes_in_col(df)

    # Check that the resulting DataFrame matches the expected output
    pd.testing.assert_frame_equal(result_df, expected_df)


# def create_category_test_df_3():
#     # Define the data and columns separately
#     sample_data = [
#         [4990000000, 202212, 'SW1A 1AA', 'P1', 'C1', 'good', 10, 100],
#         [4990000000, 202212, 'SW1A 1AA', 'P1', 'C1', 'good', 10, 100], # duplicate, that should get deduped
#         [4990000000, 202212, 'SW1A 1AA', None, 'C1', 'bad', 10, 100], # bad status that should get dropped
#         [4990000126, 202212, 'EC1A 1BB', 'P2', 'C2', 'good', 20, 200],
#         [4990000126, 202212, 'EC1A 1BB', 'P2', 'C2', 'no mean found', 20, 200], # bad status that should get dropped
#         [4990000126, 202212, 'W1A 0AX', 'P2', None, 'good', 20, 200],
#     ]
#     sample_cols = ['reference', 'period', 'postcode_col', 'product_col', 'civdef_col', 'imp_marker', 'dummy_headcount', 'dummy_211']

#     # Create the DataFrame
#     df = pd.DataFrame(sample_data, columns=sample_cols)

#     return df


# def test_create_category_df():
#     # Create the input DataFrame
#     sample_df = create_category_test_df_3()

#     # Define the expected output data and columns
#     expected_data = [
#         [4990000000, 202212, 'SW1A 1AA', 'P1', 'C1', 'good', 10, 100],
#         [4990000126, 202212, 'EC1A 1BB', 'P2', 'C2', 'good', 20, 200],
#     ]
#     expected_cols = ['reference', 'period', 'postcode_col', 'product_col', 'civdef_col', 'imp_marker', 'dummy_headcount', 'dummy_211']

#     # Create the expected output DataFrame
#     expected_df = pd.DataFrame(expected_data, columns=expected_cols)

#     # Define the parameters for the function
#     imp_markers_to_keep = ['good']
#     orig_cols = sample_df.columns.tolist()

#     # Include the definitions
#     code_cols = ['product_col', 'civdef_col']
#     site_cols = ['postcode_col']

#     # load config in order to get the value columns
#     value_cols = ["dummy_headcount", "dummy_211"]

#     # Apply the function
#     result_df = create_category_df(
#         sample_df, imp_markers_to_keep, orig_cols, groupby_cols, code_cols, site_cols, value_cols
#     )

#     # Check that the resulting DataFrame matches the expected output
#     pd.testing.assert_frame_equal(result_df, expected_df)

# Create a class to test all componenets of create_category_df function,
# including `create_notnull_mask`, `keep_good_markers` and `deduplicate_codes_values` functions


class TestCreateCategoryDF:
    def test_deduplicate_codes_values(self):
        # Define the data and columns separately
        sample_data = [
            ["A", 1, 10, "x"],
            ["A", 1, 20, "y"],
            ["B", 2, 30, "z"],
            ["B", 2, 40, "w"],
            ["C", 3, 50, "v"],
        ]
        sample_cols = ["reference", "group_col2", "value_col", "textual_col"]

        # Create the DataFrame
        df = pd.DataFrame(sample_data, columns=sample_cols)

        # Define the expected output data and columns
        expected_data = [
            ["A", 1, 30, "x"],
            ["B", 2, 70, "z"],
            ["C", 3, 50, "v"],
        ]
        expected_cols = ["group_col1", "period", "211", "textual_col"]

        # Create the expected output DataFrame
        expected_df = pd.DataFrame(expected_data, columns=expected_cols)

        # Column names redefined for convenience
        instance_col: str = "instance"
        postcodes_harmonised_col: str = "postcodes_harmonised"
        percent_col: str = "602"
        product_col: str = "201"
        pg_num_col: str = "pg_numeric"
        civdef_col: str = "200"

        # Define the parameters for the function
        value_cols = ["value_col"]
        code_cols: List[str] = [product_col, civdef_col, pg_num_col]
        site_cols: List[str] = [
            instance_col,
            postcode_col,
            percent_col,
            postcodes_harmonised_col,
        ]
        orig_cols: List[str] = df.columns.tolist()
        category_cols = [x for x in orig_cols if x not in site_cols]
        textual_cols = [
            x for x in category_cols if x not in (groupby_cols + code_cols + value_cols)
        ]
        methods = ["sum", "first"]

        # Apply the function
        result_df = deduplicate_codes_values(
            df, groupby_cols, value_cols, textual_cols, methods
        )

        # Check that the resulting DataFrame matches the expected output
        pd.testing.assert_frame_equal(result_df, expected_df)

    # def create_category_test_df_3():
    #     # Define the data and columns separately
    #     sample_data = [
    #         [4990000000, 202212, 'SW1A 1AA', 'P1', 'C1', 'good', 10, 100],
    #         [4990000000, 202212, 'SW1A 1AA', 'P1', 'C1', 'good', 10, 100], # duplicate, that should get deduped
    #         [4990000000, 202212, 'SW1A 1AA', None, 'C1', 'bad', 10, 100], # bad status that should get dropped
    #         [4990000126, 202212, 'EC1A 1BB', 'P2', 'C2', 'good', 20, 200],
    #         [4990000126, 202212, 'EC1A 1BB', 'P2', 'C2', 'no mean found', 20, 200], # bad status that should get dropped
    #         [4990000126, 202212, 'W1A 0AX', 'P2', None, 'good', 20, 200],
    #     ]
    #     sample_cols = ['reference', 'period', 'postcode_col', 'product_col', 'civdef_col', 'imp_marker', 'dummy_headcount', 'dummy_211']

    #     # Create the DataFrame
    #     df = pd.DataFrame(sample_data, columns=sample_cols)

    #     return df

    # def test_create_category_df():
    #     # Create the input DataFrame
    #     sample_df = create_category_test_df_3()

    #     # Define the expected output data and columns
    #     expected_data = [
    #         [4990000000, 202212, 'SW1A 1AA', 'P1', 'C1', 'good', 10, 100],
    #         [4990000126, 202212, 'EC1A 1BB', 'P2', 'C2', 'good', 20, 200],
    #     ]
    #     expected_cols = ['reference', 'period', 'postcode_col', 'product_col', 'civdef_col', 'imp_marker', 'dummy_headcount', 'dummy_211']

    #     # Create the expected output DataFrame
    #     expected_df = pd.DataFrame(expected_data, columns=expected_cols)

    #     # Define the parameters for the function
    #     imp_markers_to_keep = ['good']
    #     orig_cols = sample_df.columns.tolist()

    #     # Include the definitions
    #     code_cols = ['product_col', 'civdef_col']
    #     site_cols = ['postcode_col']

    # Create fake value columns
    value_cols = ["dummy_headcount", "dummy_211"]


#     # Apply the function
#     result_df = create_category_df(
#         sample_df, imp_markers_to_keep, orig_cols, groupby_cols, code_cols, site_cols, value_cols
#     )

#     # Check that the resulting DataFrame matches the expected output
#     pd.testing.assert_frame_equal(result_df, expected_df)

# Create a class to test all componenets of create_category_df function,
# including `create_notnull_mask`, `keep_good_markers` and `deduplicate_codes_values` functions

# class TestCreateCategoryDF:

#     def test_deduplicate_codes_values(self):
#         # Define the data and columns separately
#         sample_data = [
#             ['A', 1, 10, 'x'],
#             ['A', 1, 20, 'y'],
#             ['B', 2, 30, 'z'],
#             ['B', 2, 40, 'w'],
#             ['C', 3, 50, 'v'],
#         ]
#         sample_cols = ['reference', 'group_col2', 'value_col', 'textual_col']

#         # Create the DataFrame
#         df = pd.DataFrame(sample_data, columns=sample_cols)

#         # Define the expected output data and columns
#         expected_data = [
#             ['A', 1, 30, 'x'],
#             ['B', 2, 70, 'z'],
#             ['C', 3, 50, 'v'],
#         ]
#         expected_cols = ['group_col1', 'period', '211', 'textual_col']

#         # Create the expected output DataFrame
#         expected_df = pd.DataFrame(expected_data, columns=expected_cols)

#         # Column names redefined for convenience
#         instance_col: str = "instance"
#         postcodes_harmonised_col: str = "postcodes_harmonised"
#         percent_col: str = "602"
#         product_col: str = "201"
#         pg_num_col: str = "pg_numeric"
#         civdef_col: str = "200"

#         # Define the parameters for the function
#         value_cols = ['value_col']
#         code_cols: List[str] = [product_col, civdef_col, pg_num_col]
#         site_cols: List[str] = [instance_col, postcode_col, percent_col, postcodes_harmonised_col]
#         orig_cols: List[str] = df.columns.tolist()
#         category_cols = [x for x in orig_cols if x not in site_cols]
#         textual_cols = [x for x in category_cols if x not in (
#                 groupby_cols + code_cols + value_cols
#             )]
#         methods = ["sum", "first"]

#         # Apply the function
#         result_df = deduplicate_codes_values(
#             df, groupby_cols, value_cols, textual_cols, methods
#         )

#         # Check that the resulting DataFrame matches the expected output
#         pd.testing.assert_frame_equal(result_df, expected_df)
