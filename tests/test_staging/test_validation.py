import pandas as pd
import numpy as np
import pytest

# from unittest.mock import MagicMock, patch
from src.staging.validation import (
    validate_post_col,
    validate_postcode_pattern,
    check_pcs_real,
    validate_data_with_schema,
    combine_schemas_validate_full_df,
)
from src.utils.helpers import Config_settings

# Get the config
conf_obj = Config_settings()
config = conf_obj.config_dict


@pytest.fixture
def test_data_dict():
    return {
        "reference": [1, 2, 3, 4],
        "instance": [0, 0, 0, 0],
        "referencepostcode": ["NP10 8XG", "SW1P 4DF", "HIJ 789", "KL1M 2NO"],
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
    return pd.Series(["NP108XG", "SW1P4DF", "PO155RR"])


# Test case for validate_post_col
def test_validate_post_col(test_data_df, monkeypatch, caplog):
    # Monkeypatch the get_masterlist function to use the mock implementation
    monkeypatch.setattr("src.staging.validation.get_masterlist", mock_get_masterlist)

    # Make a fake path to the masterlist
    fake_path = "path/to/missing_masterlist.csv"

    # Call the function under test
    validate_post_col(test_data_df, fake_path)

    # Using caplog to check the logged warning messages
    if config["global"]["postcode_csv_check"]:

        assert (
            "These postcodes are not found in the ONS postcode list: ['HIJ 789', 'KL1M 2NO']"  # noqa
            in caplog.text
        )

    else:
        assert "Invalid pattern postcodes found: ['HIJ 789']" in caplog.text

    # Valid AND real postcodes
    df_valid = pd.DataFrame(
        {
            "reference": [1, 2, 3],
            "instance": [0, 0, 0],
            "referencepostcode": ["NP10 8XG", "PO15 5RR", "SW1P 4DF"],
        }
    )
    df_result = validate_post_col(df_valid, fake_path)
    exp_output1 = pd.DataFrame(
        columns=["reference", "instance", "invalid_pattern_pcodes"]
    )
    exp_output2 = pd.DataFrame(columns=["reference", "instance", "not_real_pcodes"])
    pd.testing.assert_frame_equal(
        df_result[0], exp_output1, check_dtype=False, check_index_type=False
    )
    pd.testing.assert_frame_equal(
        df_result[1], exp_output2, check_dtype=False, check_index_type=False
    )

    # Invalid postcodes
    df_invalid = pd.DataFrame(
        {
            "reference": [1, 2],
            "instance": [0, 0],
            "referencepostcode": ["EFG 456", "HIJ 789"],
        }
    )
    validate_post_col(df_invalid, fake_path)
    assert "Total list of unique invalid postcodes found: ['EFG 456', 'HIJ 789']" in caplog.text

    # Mixed valid and invalid postcodes - as is in the test_data

    validate_post_col(test_data_df, fake_path)
    if config["global"]["postcode_csv_check"]:
        assert "Total list of unique invalid postcodes found: ['HIJ 789', 'KL1M 2NO']" in caplog.text
    else:
        assert "Total list of unique invalid postcodes found: ['HIJ 789']" in caplog.text


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
    monkeypatch.setattr("src.staging.validation.get_masterlist", mock_get_masterlist)

    # Use the fake path
    postcode_masterlist = "path/to/mock_masterlist.csv"

    # Call the function under test
    result_df = check_pcs_real(test_data_df, postcode_masterlist)
    result_df = result_df.reset_index(drop=True)
    if config["global"]["postcode_csv_check"]:

        expected_unreal_postcodes = pd.Series(
            ["HIJ 789", "KL1M 2NO"], name="referencepostcode"
        )
    else:
        expected_unreal_postcodes = pd.Series(
            [], name="referencepostcode", dtype=object
        )

    pd.testing.assert_series_equal(
        result_df, expected_unreal_postcodes
    )  # Assert that the unreal postcodes match the expected ones


def test_check_pcs_real_with_valid_postcodes(test_data_df, monkeypatch):
    # Monkeypatch the get_masterlist function to use the mock implementation
    monkeypatch.setattr("src.staging.validation.get_masterlist", mock_get_masterlist)

    # Use the fake path
    postcode_masterlist = "path/to/masterlist.csv"

    # Call the function under test
    unreal_postcodes = check_pcs_real(test_data_df, postcode_masterlist)
    # NP10 8XG and SW1P 4DF are real. Should not be presentin unreal_postcode
    assert (
        bool(unreal_postcodes.isin(["NP10 8XG", "SW1P 4DF"]).any()) is False
    )  # Assert that the real postcodes are not in the unreal postcodes


def test_check_data_shape():
    """Test the check_data_shape function."""
    # Arrange
    from src.staging.validation import check_data_shape

    # Dataframe for test function to use
    dummy_dict = {"col1": [1, 2], "col2": [3, 4]}
    dummy_df = pd.DataFrame(data=dummy_dict)

    # Act: use pytest to assert the result
    result_1 = check_data_shape(dummy_df)

    # Assert
    assert isinstance(result_1, bool)
    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert not isinstance(result_1, bool)
    # Assert: test that add fails when the arguments are wrong type
    pytest.raises(ValueError, check_data_shape, 1)


def test_load_schema():
    """Test the load_schema function."""
    # Arrange
    from src.staging.validation import load_schema

    # Act: use pytest to assert the result
    result_1 = load_schema()

    # Assert
    assert isinstance(result_1, dict)
    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert not isinstance(result_1, dict)
    # Assert: test that add fails when the arguments are wrong type
    pytest.raises(TypeError, load_schema, 2)
    pytest.raises(TypeError, load_schema, True)


# Mock the schema data
def mock_load_data(filepath):
    data_type_schema = {
        "col1": {"Deduced_Data_Type": "int"},
        "col2": {"Deduced_Data_Type": "str"},
        "col3": {"Deduced_Data_Type": "float"},
        "col4": {"Deduced_Data_Type": "datetime64[ns]"},
    }
    return data_type_schema


@pytest.fixture
def mock_load_schema(monkeypatch):

    monkeypatch.setattr("src.staging.validation.load_schema", mock_load_data)


def test_validate_data_with_schema(mock_load_schema):
    """Test the validate_data_with_shcema  to data types are correct in
    the source data
    """
    # Dumy data for testing
    dumy_data = pd.DataFrame(
        {
            "col1": [2, 4, 6],
            "col2": ["Z", "Y", "V"],
            "col3": [2.6, 3.8, 4.6],
            "col4": ["2023-07-23", "2023-07-24", "2023-07-25"],
        }
    )
    # convert col4 datetime type
    dumy_data["col4"] = pd.to_datetime(dumy_data["col4"])

    # Call the function to be tested
    validate_data_with_schema(dumy_data, "mock_schema.toml")

    # Check data types after validation
    assert dumy_data["col1"].dtypes == np.int
    assert dumy_data["col2"].dtypes == np.object
    assert dumy_data["col3"].dtypes == np.float
    assert pd.api.types.is_datetime64_any_dtype(dumy_data["col4"].dtypes)


# Mock the schemas data
def mock_load_both_data(filepath):
    data_type_schema1 = {
        "reference": {"Deduced_Data_Type": "int"},
        "createdby": {"Deduced_Data_Type": "str"},
        "instance": {"Deduced_Data_Type": "float"},
        "date": {"Deduced_Data_Type": "datetime64[ns]"},
    }
    data_type_schema2 = {
        "q200": {"Deduced_Data_Type": "str"},
        "q201": {"Deduced_Data_Type": "int"},
        "q203": {"Deduced_Data_Type": "float"},
        "q307": {"Deduced_Data_Type": "bool"},
    }
    data_type_schema = {**data_type_schema1, **data_type_schema2}

    return data_type_schema


@pytest.fixture
def mock_load_schemas(monkeypatch):

    monkeypatch.setattr("src.staging.validation.load_schema", mock_load_both_data)


def test_combine_schemas_validate_full_df(mock_load_schemas):
    """Test the validate_data_with_shcema  to data types are correct in
    the source data
    """
    # Dumy data for testing
    dumy_data = pd.DataFrame(
        {
            "reference": [2, 4, 6],
            "createdby": ["Z", "Y", "V"],
            "instance": [2.6, 3.8, 4.6],
            "date": ["2023-07-23", "2023-07-24", "2023-07-25"],
            "q200": ["C", "D", "C"],
            "q201": [5, 7, 9],
            "q203": [2.6, 3.8, 4.6],
            "q307": [True, False, True],
        }
    )
    # convert date datetime type
    dumy_data["date"] = pd.to_datetime(dumy_data["date"])

    # Call the function to be tested
    combine_schemas_validate_full_df(
        dumy_data, "mock_schema1.toml", "mock_schema2.toml"
    )

    # Check data types after validation
    assert dumy_data[["reference", "q201"]].dtypes.all() == np.int
    assert dumy_data[["createdby", "q200"]].dtypes.all() == pd.StringDtype()
    assert dumy_data[["instance", "q203"]].dtypes.all() == np.float
    assert pd.api.types.is_datetime64_any_dtype(dumy_data["date"].dtypes)
    assert dumy_data["q307"].dtypes == np.bool
