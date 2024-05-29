import pandas as pd
from pandas._testing import assert_frame_equal
import numpy as np
import pytest
import unittest

from src.staging.postcode_validation import (
    run_full_postcode_process,
    validate_postcode_pattern,
    format_postcodes,
    check_pcs_real,
)


class TestFormatPostcode(object):
    """Tests for postcode_topup."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """Input data for postcode_topup tests."""
        columns = ["key", "postcode"]
        data = [
            [1, "NP44 2NZ"], # normal
            [2, "np44 2nz"], # lower case
            [3, "NP4 2NZ"], # only 7 chars
            [4, "NP44 2NZ 7Y"], # extra parts
            [5, "NP44 2NZZ"], # 9 chars (extra)
            [6, "NP442NZ"], # one part, 7 chars
            [7, ""], #empty str
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df


    def test_format_postcodes(self, input_data):
        """General tests for postcode_topup."""
        output = input_data.copy()
        output["postcode"] = output["postcode"].apply(
            lambda x: format_postcodes(x)
            )
        print(output)


# Get the config
def generate_config(val):
    """Generate a dummy config file"""
    config = {"global": {"postcode_csv_check": val}}

    return config


@pytest.fixture
def test_data_dict():
    return {
        "reference": [1, 2, 3, 4],
        "instance": [0, 0, 0, 0],
        "formtype": [0, 0, 0, 0],
        "601": ["NP10 8XG", "SW1P 4DF", "HIJ 789", "KL1M 2NO"],
        "referencepostcode": ["NP10 8XG", "SW1P 4DF", "HIJ 789", "KL1M 2NO"],
        # "postcodes_harmonised": ["NP10 8XG", "SW1P 4DF", "HIJ 789", "KL1M 2NO"],
    }


@pytest.fixture  # noqa
def test_data_df(test_data_dict):
    """'NP10 8XG', 'SW1P 4DF' are valid and real postcodes.
    'HIJ 789' is neither valid nor real
    and 'KL1M 2NO' is a valid pattern but not real"""
    return pd.DataFrame(test_data_dict)


# Mock the get_masterlist function
def mock_get_masterlist(postcode_masterlist):
    # Return a mock masterlist series - actual postcodes of ONS offices
    return pd.Series(["NP10 8XG", "SW1P 4DF", "PO15 5RR"])


# Test case for run_full_postcode_process
def test_run_full_postcode_process(test_data_df, monkeypatch, caplog):
    # Monkeypatch the get_masterlist function to use the mock implementation
    monkeypatch.setattr("src.staging.postcode_validation.get_masterlist", mock_get_masterlist)

    # Make a fake path to the masterlist
    fake_path = "path/to/missing_masterlist.csv"

    config = generate_config(True)

    # Call the function under test
    run_full_postcode_process(test_data_df, fake_path, config)

    # Using caplog to check the logged warning messages
    if config["global"]["postcode_csv_check"]:

        assert (
            "These postcodes are not found in the ONS postcode list: ['KL1M 2NO']"
            in caplog.text
        )

    else:
        assert "Invalid pattern postcodes found: ['HIJ 789']" in caplog.text

    # Valid AND real postcodes
    df_valid = pd.DataFrame(
        {
            "reference": [1, 2, 3],
            "instance": [0, 0, 0],
            "formtype": [0, 0, 0],
            "601": ["NP10 8XG", "PO15 5RR", "SW1P 4DF"],
            "referencepostcode": ["NP10 8XG", "PO15 5RR", "SW1P 4DF"],
            "postcodes_harmonised": ["NP10 8XG", "PO15 5RR", "SW1P 4DF"],
        }
    )
    df_result = run_full_postcode_process(df_valid, fake_path, config)
    exp_output1 = pd.DataFrame(
        columns=[
            "reference",
            "instance",
            "formtype",
            "postcode_issue",
            "incorrect_postcode",
            "postcode_source",
        ]
    )

    pd.testing.assert_frame_equal(
        df_result, exp_output1, check_dtype=False, check_index_type=False
    )

    # Invalid postcodes
    df_invalid = pd.DataFrame(
        {
            "reference": [1, 2],
            "instance": [0, 0],
            "formtype": [0, 0],
            "601": ["EFG 456", "HIJ 789"],
            "referencepostcode": ["EFG 456", "HIJ 789"],
            "postcodes_harmonised": ["EFG 456", "HIJ 789"],
        }
    )
    run_full_postcode_process(df_invalid, fake_path, config)
    assert (
        "Total list of unique invalid postcodes found: ['EFG 456', 'HIJ 789']"
        in caplog.text
    )

    # Mixed valid and invalid postcodes - as is in the test_data

    run_full_postcode_process(test_data_df, fake_path, config)
    if config["global"]["postcode_csv_check"]:
        assert (
            "Total list of unique invalid postcodes found: ['KL1M 2NO', 'HIJ 789']"
            in caplog.text
        )
    else:
        assert (
            "Total list of unique invalid postcodes found: ['HIJ 789']" in caplog.text
        )


def test_validate_postcode():
    # Valid postcodes
    assert validate_postcode_pattern("AB12 3CD") is True
    assert (
        validate_postcode_pattern("AB123CD") is True
    )  # Missing space - othewise valid
    assert validate_postcode_pattern("DE34 5FG") is True
    assert validate_postcode_pattern("HI67 8JK") is True

    # Invalid postcodes
    assert validate_postcode_pattern("EFG 456") is False
    assert validate_postcode_pattern("HIJ 789") is False
    assert validate_postcode_pattern("B27 OAG") is False  # Zero is actually an "O"

    # Edge cases
    assert validate_postcode_pattern(None) is False  # None value should fail
    assert validate_postcode_pattern("") is False  # Empty string
    assert validate_postcode_pattern(" ") is False  # Whitespace
    assert validate_postcode_pattern("ABC XYZ") is False  # All letters but right length
    assert validate_postcode_pattern("123 456") is False  # All numbers but right length


def test_check_pcs_real_with_invalid_postcodes(test_data_df, monkeypatch):

    # Monkeypatch the get_masterlist function to use the mock implementation
    monkeypatch.setattr("src.staging.postcode_validation.get_masterlist", mock_get_masterlist)

    # Use the fake path
    postcode_masterlist = "path/to/mock_masterlist.csv"

    config = generate_config(True)

    check_real_df = test_data_df.copy()
    check_real_df["postcodes_harmonised"] = check_real_df["postcodes_harmonised"].apply(format_postcodes)

    # Call the function under test
    result_df = check_pcs_real(test_data_df, check_real_df, postcode_masterlist, config)
    result_df = result_df.reset_index(drop=True)
    if config["global"]["postcode_csv_check"]:

        expected_unreal_postcodes = pd.Series(
            ["HIJ 789", "KL1M 2NO"], name="postcodes_harmonised"
        )
    else:
        expected_unreal_postcodes = pd.Series(
            [], name="postcodes_harmonised", dtype=object
        )

    pd.testing.assert_series_equal(
        result_df, expected_unreal_postcodes
    )  # Assert that the unreal postcodes match the expected ones


def test_check_pcs_real_with_valid_postcodes(test_data_df, monkeypatch):
    # Monkeypatch the get_masterlist function to use the mock implementation
    monkeypatch.setattr("src.staging.postcode_validation.get_masterlist", mock_get_masterlist)

    # Use the fake path
    postcode_masterlist = "path/to/masterlist.csv"

    config = generate_config(True)

    check_real_df = test_data_df.copy()
    check_real_df["postcodes_harmonised"] = check_real_df["postcodes_harmonised"].apply(format_postcodes)

    # Call the function under test
    unreal_postcodes = check_pcs_real(
        test_data_df, check_real_df, postcode_masterlist, config
    )
    # NP10 8XG and SW1P 4DF are real. Should not be presentin unreal_postcode
    assert (
        bool(unreal_postcodes.isin(["NP10 8XG", "SW1P 4DF"]).any()) is False
    )  # Assert that the real postcodes are not in the unreal postcodes
