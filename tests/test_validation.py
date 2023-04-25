"""Create a test suite for the validation module."""

import pytest


def test_check_file_exists():
    """Test the check_file_exists function."""
    # Arrange
    from src.data_validation import check_file_exists

    # Act: use pytest to assert the result
    result_1 = check_file_exists()
    # Assert
    assert isinstance(result_1, bool)
    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert not isinstance(result_1, bool)
