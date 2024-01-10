"""Unit testing module."""
# Import testing packages
import pandas as pd
import pytest
import numpy as np

from src.staging.pg_conversion import pg_to_pg_mapper, sic_to_pg_mapper


@pytest.fixture
def dummy_data() -> pd.DataFrame:
    # Set up the dummyinput  data
    data = pd.DataFrame(
        {"201": [0, 1, 2, 3, 4], "formtype": ["0001", "0001", "0001", "0001", "0001"]}
    )
    return data


@pytest.fixture
def mapper() -> pd.DataFrame:
    # Set up the dummy mapper data
    mapper = {
        "pg_numeric": [0, 1, 2, 3, 4],
        "pg_alpha": [np.nan, "A", "B", "C", "C"],
    }
    return pd.DataFrame(mapper)


@pytest.fixture
def expected_output() -> pd.DataFrame:
    # Set up the dummy output data
    expected_output = pd.DataFrame(
        {
            "201": [np.nan, "A", "B", "C", "C"],
            "formtype": ["0001", "0001", "0001", "0001", "0001"],
        }
    )

    expected_output["201"] = expected_output["201"].astype("category")
    return expected_output


@pytest.fixture
def sic_dummy_data() -> pd.DataFrame:
    # Set up the dummyinput  data
    data = pd.DataFrame(
        {"rusic": [1110, 10101], "201": [np.nan, np.nan], "formtype": ["0006", "0006"]}
    )
    return data


@pytest.fixture
def sic_mapper() -> pd.DataFrame:
    # Set up the dummy mapper data
    mapper = {
        "sic": [1110, 10101],
        "pg_alpha": ["A", "B"],
    }
    return pd.DataFrame(mapper)


@pytest.fixture
def sic_expected_output() -> pd.DataFrame:
    # Set up the dummy output data
    expected_output = pd.DataFrame(
        {"rusic": [1110, 10101], "201": ["A", "B"], "formtype": ["0006", "0006"]}
    )
    expected_output["201"] = expected_output["201"].astype("category")
    return expected_output


def test_sic_mapper(sic_dummy_data, sic_expected_output, sic_mapper):
    """Tests for pg mapper function."""

    expected_output_data = sic_expected_output

    df_result = sic_to_pg_mapper(sic_dummy_data, sic_mapper, target_col="201")

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
    row_data = [["0001", 47, "A"], ["0001", 49, "B"], ["0002", 50, "C"]]
    columns = ["formtype", "201", "other_col"]

    test_df = pd.DataFrame(row_data, columns=columns)

    expected_data = [["0001", 47, "A", "AD"], ["0001", 49, "B", "AD"]]
    expected_columns = ["formtype", "201", "other_col", "product_group"]
    expected_result_df = pd.DataFrame(expected_data, columns=expected_columns)
    expected_result_df["product_group"] = expected_result_df["product_group"].astype(
        "category"
    )

    result = pg_to_pg_mapper(test_df.copy(), mapper.copy())
    pd.testing.assert_frame_equal(result, expected_result_df, check_dtype=False)


def test_pg_to_pg_mapper_success(mapper):
    row_data = [
        ["0001", 36, "A"],
        ["0001", 45, "B"],
        ["0002", 58, "C"],
        ["0001", 49, "D"],
    ]
    columns = ["formtype", "201", "other_col"]

    test_df = pd.DataFrame(row_data, columns=columns)

    result_df = pg_to_pg_mapper(test_df.copy(), mapper.copy())

    expected_data = [
        ["0001", 36, "A", "N"],
        ["0001", 45, "B", "AC"],
        ["0001", 49, "D", "AD"],
    ]

    expected_columns = ["formtype", "201", "other_col", "product_group"]

    expected_result_df = pd.DataFrame(
        expected_data, columns=expected_columns, index=result_df.index
    )

    expected_result_df["product_group"] = expected_result_df["product_group"].astype(
        "category"
    )

    # Set indexes to match

    pd.testing.assert_frame_equal(result_df, expected_result_df)
