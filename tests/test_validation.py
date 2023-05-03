"""Create a test suite for the validation module."""

import pytest


def test_add():
    """Test the add function."""
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
    pytest.raises(TypeError, check_data_shape, 1, "2", 3)
    pytest.raises(TypeError, check_data_shape, "1", 2, 3)
    pytest.raises(TypeError, check_data_shape, "1", "2", "3")
