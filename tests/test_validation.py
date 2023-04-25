"""Create a test suite for the validation module."""

import pytest
import os


def test_check_file_exists():
    """Test the check_file_exists function."""
    # Arrange
    from src.data_validation.validation import check_file_exists

    # Act: use pytest to assert the result
    empty_file = open("./emptyfile.py", "x")

    result_1 = check_file_exists()
    result_2 = check_file_exists("Non_existant_file.txt")
    result_3 = check_file_exists(empty_file.name)

    os.remove(empty_file.name)

    # Assert
    assert isinstance(result_1, bool)
    assert result_1
    assert not result_2
    assert not result_3

    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert not isinstance(result_1, bool)
