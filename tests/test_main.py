"""Create a test suite for the main module."""

import pytest


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
