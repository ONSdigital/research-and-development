"""Unit testing module."""
# Import testing packages
import pandas as pd
import pytest
import numpy as np

from src.imputation.pg_conversion import pg_to_pg_mapper, sic_to_pg_mapper


@pytest.fixture
def dummy_data() -> pd.DataFrame:
    # Set up the dummyinput  data
    data = pd.DataFrame(
        {
            "201": [0, 1, 2, 3, 4, 5],
        }
    )
    data.astype("category")
    return data


@pytest.fixture
def mapper() -> pd.DataFrame:
    # Set up the dummy mapper data
    mapper = {
        "2016 > Form PG": [0, 1, 2, 3, 4, 5],
        "2016 > Pub PG": [np.nan, "A", "B", "C", "C", "D"],
    }
    return pd.DataFrame(mapper)


@pytest.fixture
def expected_output() -> pd.DataFrame:
    # Set up the dummy output data
    expected_output = pd.DataFrame(
        {
            "201": [np.nan, "A", "B", "C", "C", "D"],
        }
    )
    return expected_output


def test_pg_mapper(dummy_data, expected_output, mapper):
    """Tests for pg mapper function."""

    target_col = dummy_data.columns[0]
    expected_output_data = expected_output.astype("category")

    df_result = pg_to_pg_mapper(dummy_data, mapper, target_col)

    pd.testing.assert_frame_equal(df_result, expected_output_data)


@pytest.fixture
def sic_dummy_data() -> pd.DataFrame:
    # Set up the dummyinput  data
    data = pd.DataFrame(
        {
            "rusic": [1110, 10101, 15410, 17401, 21215],
            "201": [np.nan, np.nan, np.nan, np.nan, np.nan],
        }
    )
    return data


@pytest.fixture
def sic_mapper() -> pd.DataFrame:
    # Set up the dummy mapper data
    mapper = {
        "SIC 2007_CODE": [1110, 10101, 15410, 17401, 21215],
        "2016 > Pub PG": ["A", "B", "C", "D", "E"],
    }
    return pd.DataFrame(mapper)


@pytest.fixture
def sic_expected_output() -> pd.DataFrame:
    # Set up the dummy output data
    expected_output = pd.DataFrame(
        {
            "rusic": [1110, 10101, 15410, 17401, 21215],
            "201": ["A", "B", "C", "D", "E"],
        }
    )
    expected_output["201"] = expected_output["201"].astype("category")
    return expected_output


def test_sic_mapper(sic_dummy_data, sic_expected_output, sic_mapper):
    """Tests for pg mapper function."""

    expected_output_data = sic_expected_output

    df_result = sic_to_pg_mapper(sic_dummy_data, sic_mapper)

    pd.testing.assert_frame_equal(df_result, expected_output_data)
