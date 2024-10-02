import pandas as pd
from pandas._testing import assert_frame_equal
import numpy as np
import pytest
import unittest


# from unittest.mock import MagicMock, patch
from src.staging.validation import (
    validate_data_with_schema,
    combine_schemas_validate_full_df,
    validate_many_to_one,
)


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
        "col4": {"Deduced_Data_Type": "datetime64"},
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
    assert dumy_data["col1"].dtypes == int
    assert dumy_data["col2"].dtypes == pd.StringDtype()
    assert dumy_data["col3"].dtypes == float
    assert pd.api.types.is_datetime64_any_dtype(dumy_data["col4"].dtypes)


# Mock the schemas data
def mock_load_both_data(filepath):
    data_type_schema1 = {
        "reference": {"Deduced_Data_Type": "int"},
        "createdby": {"Deduced_Data_Type": "str"},
        "instance": {"Deduced_Data_Type": "float"},
        "date": {"Deduced_Data_Type": "datetime64"},
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

    #TODO: This test doesn't really do anything as the function is not returning anything
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
    # Check if the columns "reference" and "q201" are of integer type
    are_int_columns = dumy_data[["reference", "q201"]].apply(pd.api.types.is_integer_dtype).all()
    # Check if the columns "createdby" and "q200" are of string type
    are_str_columns = dumy_data[["createdby", "q200"]].apply(pd.api.types.is_string_dtype).all()
    # Check if the columns "instance" and "q203" are of float type
    are_float_columns = dumy_data[["instance", "q203"]].apply(pd.api.types.is_float_dtype).all()
    # Check if the columns "date" is of datetime type
    is_datetime_column = pd.api.types.is_datetime64_any_dtype(dumy_data["date"].dtypes)
    # Check if the columns "q307" is of boolean type
    is_bool_column = dumy_data["q307"].dtypes == bool

    assert are_int_columns
    assert are_str_columns
    assert are_float_columns
    assert is_datetime_column
    assert is_bool_column


class TestManyToOne(unittest.TestCase):
    """Unittest for checking that the mapper is many to one"""

    def mapper_good(self):
        # Good mapper
        return pd.DataFrame(
            {
                "child": ["AA", "AB", "AC"],
                "parent": ["A", "A", "A"],
            }
        )

    def mapper_duplicates(self):
        # Mapper with dulicates, but it should pass validation
        return pd.DataFrame(
            {
                "child": ["AA", "AB", "AC", "AA"],
                "parent": ["A", "A", "A", "A"],
            }
        )

    def mapper_many(self):
        # Many-to-many mapper. should fail
        return pd.DataFrame(
            {
                "child": ["AA", "AB", "AC", "AA"],
                "parent": ["A", "A", "A", "B"],
            }
        )

    def test_good_mapper(self):
        # Call the create_output_df funtion
        df_input = self.mapper_good()
        actual_result = validate_many_to_one(df_input, "child", "parent")
        expected_result = df_input
        assert_frame_equal(actual_result, expected_result)

    def test_duplicates(self):
        # Call the create_output_df funtion
        df_input = self.mapper_duplicates()
        actual_result = validate_many_to_one(df_input, "child", "parent")
        expected_result = self.mapper_good()
        assert_frame_equal(actual_result, expected_result)

    def test_many(self):
        # Validation should fail if the mapper is many to many
        df_input = self.mapper_many()
        with self.assertRaises(ValueError):
            validate_many_to_one(df_input, "child", "parent")

    def test_names(self):
        # Validation should fail if column names are wrong
        df_input = self.mapper_good()
        with self.assertRaises(ValueError):
            validate_many_to_one(df_input, "dad", "parent")
