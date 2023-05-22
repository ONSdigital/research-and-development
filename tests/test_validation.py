"""Create a test suite for the validation module."""

import pytest


def test_check_data_shape():
    """Test the check_data_shape function."""
    # Arrange
    from src.data_validation.validation import check_data_shape

    # Act: use pytest to assert the result
    result_1 = check_data_shape()

    # Assert
    assert isinstance(result_1, bool)
    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert not isinstance(result_1, bool)
    # Assert: test that add fails when the arguments are wrong type
    pytest.raises(TypeError, check_data_shape, 1, "2")
    pytest.raises(TypeError, check_data_shape, "1", 2)


def test_load_schema():
    """Test the load_schema function."""
    # Arrange
    from src.data_validation.validation import load_schema

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
