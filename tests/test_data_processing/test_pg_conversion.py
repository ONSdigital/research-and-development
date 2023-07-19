"""Unit testing module."""
# Import testing packages
import pandas as pd
import pytest
import numpy as np

from src.data_processing.pg_conversion import pg_mapper


@pytest.fixture
def dummy_data() -> pd.DataFrame:
    # Set up the dummy data
    data = pd.DataFrame(
        {
            "201": [0, 1, 2, 3, 4, 5],
        }
    )
    data.astype("category")
    return data


@pytest.fixture
def expected_output() -> pd.DataFrame:
    # Set up the dummy data
    expected_output = pd.DataFrame(
        {
            "201": [np.nan, "A", "B", "C", "C", "D"],
        }
    )
    return expected_output


def test_pg_mapper(dummy_data, expected_output):
    """Tests for full_responses function."""
    # Import modules to test

    target_col = dummy_data.columns[0]
    expected_output_data = expected_output.astype("category")

    df_result = pg_mapper(dummy_data, target_col)

    pd.testing.assert_frame_equal(df_result, expected_output_data)
