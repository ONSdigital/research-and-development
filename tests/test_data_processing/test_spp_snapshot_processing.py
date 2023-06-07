"""Unit testing module."""
# Import testing packages
import pandas as pd
import pytest
from typing import Tuple


@pytest.fixture
def dummy_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Set up the dummy data
    contributor_data = pd.DataFrame(
        {
            "reference": [101, 102, 103],
            "period": [202012, 202012, 202012],
            "survey": [1, 1, 1],
            "createdby": ["James", "Ilyas", "Roddy"],
            "createddate": [2020, 2020, 2020],
            "lastupdatedby": ["Vondy", "Charl", "Gareth"],
            "lastupdateddate": [2020, 2020, 2020],
        }
    )

    responses_data = pd.DataFrame(
        {
            "reference": [101, 101, 101, 102, 102, 102],
            "period": [202012, 202012, 202012, 202012, 202012, 202012],
            "survey": [1, 1, 1, 1, 1, 1],
            "createdby": ["A", "A", "A", "A", "A", "A"],
            "createddate": [2020, 2020, 2020, 2020, 2020, 2020],
            "lastupdatedby": ["A", "A", "A", "A", "A", "A"],
            "lastupdateddate": [2020, 2020, 2020, 2020, 2020, 2020],
            "questioncode": [200, 201, 202, 200, 201, 202],
            "response": [0, 50, 100, 75, 25, 65],
            "adjustedresponse": ["", "", "", "", "", ""],
        }
    )
    return contributor_data, responses_data


@pytest.fixture
def expected_output():
    expected_output = pd.DataFrame(
        {
            "reference": [101, 102],
            "period": [202012, 202012],
            "survey": [1, 1],
            200: [0, 75],
            201: [50, 25],
            202: [100, 65],
        }
    )

    return expected_output


def test_full_responses(dummy_data, expected_output):
    """Tests for full_responses function."""
    # Import modules to test
    from src.data_processing.spp_snapshot_processing import full_responses

    contributor_data, responses_data = dummy_data
    expected_output_data = expected_output

    df_result = full_responses(contributor_data, responses_data)

    pd.testing.assert_frame_equal(df_result, expected_output_data)


def test_response_rate(dummy_data):
    # Import the module to test
    from src.data_processing.spp_snapshot_processing import response_rate

    contributor_data, responses_data = dummy_data

    response_rate_value = response_rate(contributor_data, responses_data)

    expected_response_rate = 2 / 3  # 2 respondents out of 3 contributors
    assert expected_response_rate == response_rate_value


def test_create_response_dataframe(dummy_data):
    
    from src.data_processing.spp_snapshot_processing import create_response_dataframe
    
    contributor_data, responses_data = dummy_data
    unique_id_cols = ["reference", "period", "survey"]
    expected_columns = ["reference", "period", "survey", 200, 201, 202]
    expected_data = [
        [101, 202012, 1, 0, 50, 100],
        [102, 202012, 1, 75, 25, 65],
    ]

    response_df = create_response_dataframe(responses_data, unique_id_cols)

    # Assert the columns
    assert response_df.columns.tolist() == expected_columns

    # Assert the data
    assert response_df.values.tolist() == expected_data
    
def test_create_contextual_dataframe(dummy_data):
    contributor_data, responses_data = dummy_data
    unique_id_cols = ["reference", "period", "survey"]
    expected_columns = [
        "reference",
        "period",
        "survey",
        "createdby",
        "createddate",
        "lastupdatedby",
        "lastupdateddate",
    ]
    expected_data = [
        [101, 202012, 1, "James", 2020, "Vondy", 2020],
        [102, 202012, 1, "Ilyas", 2020, "Charl", 2020],
        [103, 202012, 1, "Roddy", 2020, "Gareth", 2020],
    ]

    contextual_df = create_contextual_dataframe(contributor_data, unique_id_cols)

    # Assert the columns
    assert contextual_df.columns.tolist() == expected_columns

    # Assert the data
    assert contextual_df.values.tolist() == expected_data