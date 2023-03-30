"""Create a test suite for the main module."""

import pytest


def test_add():
    """Test the add function."""
    # Arrange
    from src.utils.testfunctions import add

    # Act: use pytest to assert the result
    result_1_2 = add(1, 2)
    result_10_222 = add(10, 222)

    # Assert
    assert result_1_2 == 3
    assert result_10_222 == 232
    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert add(1, 2) == 4
    # Assert: test that add fails when the arguments are not integers
    pytest.raises(TypeError, add, "1", "2")
    pytest.raises(TypeError, add, 1, "2")
    pytest.raises(TypeError, add, "Spam", "Eggs")


def test_user_config_reader():
    """Test the user_config_reader function."""
    # Arrange
    from src.utils.helpers import user_config_reader

    # Act: use pytest to assert the result
    result_1 = user_config_reader()
    # Assert
    assert isinstance(result_1, dict)
    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert not isinstance(result_1, dict)


def test_period_select():
    """Test the preiod_select function."""
    # Arrange
    from src.utils.helpers import period_select

    # Act: use pytest to assert the result
    result_1 = period_select()
    # Assert
    assert isinstance(result_1, tuple)
    # Assert: Negative test. Should fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert not isinstance(result_1, tuple)
