"""Unit testing module."""
# Import testing packages
import pandas as pd
import pytest
import numpy as np

from src.imputation.pg_conversion import pg_to_pg_mapper, sic_to_pg_mapper


@pytest.fixture
def sic_dummy_data() -> pd.DataFrame:
    # Set up the dummyinput  data
    columns = ["201", "rusic"]
    data = [
        [53, 2500],
        [np.nan, 1600],
        [np.nan, 4300],
    ]

    return pd.DataFrame(data, columns=columns)


@pytest.fixture
def sic_mapper():
    columns = ["sic", "pg"]
    mapper_rows = [
        [1600, 36],
        [2500, 95],
        [7300, 45],
        [2500, 53],
    ]

    # Create the DataFrame
    return pd.DataFrame(mapper_rows, columns=columns)


@pytest.fixture
def sic_expected_output() -> pd.DataFrame:
    # Set up the dummy output data
    columns = ["201", "rusic"]
    data = [
        [53, 2500],
        [36, 1600],
        [np.nan, 4300],
    ]

    return pd.DataFrame(data, columns=columns)


def test_sic_mapper(sic_dummy_data, sic_expected_output, sic_mapper):
    """Tests for pg mapper function."""

    expected_output_data = sic_expected_output

    df_result = sic_to_pg_mapper(
        sic_dummy_data, 
        sic_mapper,
        pg_column="201",
        from_col="sic",
        to_col="pg",
        )

    pd.testing.assert_frame_equal(df_result, expected_output_data)


@pytest.fixture
def mapper():
    mapper_rows = [
        [36, "N"],
        [37, "Y"],
        [45, "AC"],
        [47, "AD"],
        [49, "AD"],
        [50, "AD"],
        [58, "AH"],
    ]
    columns = ["pg_numeric", "pg_alpha"]

    # Create the DataFrame
    mapper_df = pd.DataFrame(mapper_rows, columns=columns)

    # Return the DataFrame
    return mapper_df


def test_pg_to_pg_mapper_with_many_to_one(mapper):

    columns = ["formtype", "201", "other_col"]
    row_data = [
        ["0001", 45, "2020"], 
        ["0001", 49, "2020"], 
        ["0002", 50, "2020"]
    ]

    test_df = pd.DataFrame(row_data, columns=columns)

    expected_columns = ["formtype", "201", "other_col", "pg_numeric"]

    expected_data = [
        ["0001", "AC", "2020", 45],
        ["0001", "AD", "2020", 49],
        ["0002", "AD", "2020", 50]
    ]

    type_dict = {"201": "category", "pg_numeric": "category"}

    # Build the expected result dataframe. Set the dtype of prod group to cat, like the result_df
    expected_result_df = pd.DataFrame(expected_data, columns=expected_columns)
    expected_result_df = expected_result_df.astype(type_dict)

    result_df = pg_to_pg_mapper(test_df.copy(), mapper.copy())

    pd.testing.assert_frame_equal(result_df, expected_result_df, check_dtype=False)


def test_pg_to_pg_mapper_success(mapper):
    columns = ["formtype", "201", "other_col"] 
    row_data = [
        ["0001", 36, "2020"],
        ["0001", 45, "2020"],
        ["0002", 58, "2020"],
        ["0001", 49, "2020"],
    ]

    test_df = pd.DataFrame(row_data, columns=columns)

    expected_columns = ["formtype", "201", "other_col", "pg_numeric"]
    expected_data = [
        ["0001", "N", "2020", 36],
        ["0001", "AC", "2020", 45],
        ["0002", "AH", "2020", 58],
        ["0001", "AD", "2020", 49],
    ]

    expected_result_df = pd.DataFrame(
        expected_data, columns=expected_columns)

    type_dict = {"201": "category", "pg_numeric": "category"}
    expected_result_df = expected_result_df.astype(type_dict)

    result_df = pg_to_pg_mapper(test_df.copy(), mapper.copy())

    pd.testing.assert_frame_equal(result_df, expected_result_df)
