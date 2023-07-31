"""Unit testing module."""
# Import testing packages
import pandas as pd
import numpy as np
import pytest
from typing import Tuple


from src.staging.spp_snapshot_processing import (
    create_response_dataframe,
    full_responses,
    response_rate,
    create_contextual_dataframe,
)


@pytest.fixture
def dummy_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Set up the dummy data
    contributor_schema = {
        "reference": "int64",
        "period": "int64",
        "survey": "int64",
        "createdby": "str",
        "createddate": "Int64",
        "lastupdatedby": "str",
        "lastupdateddate": "Int64",
    }
    contributor_data = [
        [101, 202012, 1, "James", 2020, "Vondy", 2020],
        [102, 202012, 1, "Ilyas", 2020, "George", 2020],
        [103, 202012, 1, "Muttesir", 2020, "Anne", 2020],
    ]
    contributor_input = pd.DataFrame(
        data=contributor_data, columns=contributor_schema.keys()
    ).astype(contributor_schema)

    responses_schema = {
        "reference": "int64",
        "instance": "Int64",
        "period": "int64",
        "survey": "int64",
        "createdby": "str",
        "createddate": "Int64",
        "lastupdatedby": "str",
        "lastupdateddate": "Int64",
        "questioncode": "Int64",
        "response": "Int64",
        "adjustedresponse": "str",
    }

    responses_data = [
        [101, 0, 202012, 1, "A", 2020, "A", 2020, 200, 0, ""],
        [101, 0, 202012, 1, "A", 2020, "A", 2020, 201, 50, ""],
        [101, 0, 202012, 1, "A", 2020, "A", 2020, 202, 100, ""],
        [101, 1, 202012, 1, "A", 2020, "A", 2020, 200, 10, ""],
        [101, 1, 202012, 1, "A", 2020, "A", 2020, 201, 510, ""],
        [101, 1, 202012, 1, "A", 2020, "A", 2020, 202, 110, ""],
        [102, 0, 202012, 1, "A", 2020, "A", 2020, 200, 75, ""],
        [102, 0, 202012, 1, "A", 2020, "A", 2020, 201, 25, ""],
        [102, 0, 202012, 1, "A", 2020, "A", 2020, 202, 65, ""],
    ]

    responses_input = pd.DataFrame(
        data=responses_data, columns=responses_schema.keys()
    ).astype(responses_schema)
    return contributor_input, responses_input


@pytest.fixture
def expected_output():
    expected_schema = {
        "reference": "int64",
        "instance": "Int64",
        200: "Int64",
        201: "Int64",
        202: "Int64",
        "period": "int64",
        "survey": "int64",
    }

    expected_data = [
        [101, 0, 0, 50, 100, 202012, 1],
        [101, 1, 10, 510, 110, 202012, 1],
        [102, 0, 75, 25, 65, 202012, 1],
        [103, np.nan, np.nan, np.nan, np.nan, 202012, 1],
    ]

    expected_output = pd.DataFrame(
        data=expected_data, columns=expected_schema.keys()
    ).astype(expected_schema)

    return expected_output


def test_full_responses(dummy_data, expected_output):
    """Tests for full_responses function."""
    # Import modules to test

    contributor_data, responses_data = dummy_data
    expected_output_data = expected_output

    df_result = full_responses(contributor_data, responses_data)

    pd.testing.assert_frame_equal(
        df_result, expected_output_data, check_like=True, check_dtype=False
    )


def test_response_rate(dummy_data):
    # Import the module to test

    contributor_data, responses_data = dummy_data

    response_rate_value = response_rate(contributor_data, responses_data)

    expected_response_rate = 2 / 3  # 2 respondents out of 3 contributors
    assert expected_response_rate == response_rate_value


def test_create_response_dataframe(dummy_data):

    contributor_data, responses_data = dummy_data
    unique_id_cols = ["reference", "instance"]
    expected_columns = ["reference", "instance", 200, 201, 202]
    expected_data = [
        [101, 0, 0, 50, 100],
        [101, 1, 10, 510, 110],
        [102, 0, 75, 25, 65],
    ]

    response_df = create_response_dataframe(responses_data, unique_id_cols)

    # Assert the columns
    assert response_df.columns.tolist() == expected_columns

    # Assert the data
    assert response_df.values.tolist() == expected_data


def test_create_contextual_dataframe(dummy_data):
    contributor_data, responses_data = dummy_data
    unique_id_cols = ["reference", "instance"]
    expected_columns = [
        "reference",
        "instance",
        "period",
        "survey",
        "createdby",
        "createddate",
        "lastupdatedby",
        "lastupdateddate",
        "adjustedresponse",
    ]
    expected_data = [
        [101, 0, 202012, 1, "A", 2020, "A", 2020, ""],
        [101, 1, 202012, 1, "A", 2020, "A", 2020, ""],
        [102, 0, 202012, 1, "A", 2020, "A", 2020, ""],
    ]

    contextual_df = create_contextual_dataframe(responses_data, unique_id_cols)

    # Assert the columns
    assert contextual_df.columns.tolist() == expected_columns

    # Assert the data
    assert contextual_df.values.tolist() == expected_data
