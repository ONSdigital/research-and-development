"""Create a test suite for the main module."""

import pytest

def test_add():
    """Test the add function."""
    from src.utils.testfunctions import add
    # use pytest to assert the result
    assert add(1, 2) == 3
    assert add(10, 222) == 232
    # test that add fails when the answer is wrong
    with pytest.raises(AssertionError):
        assert add(1, 2) == 4
    # test that add fails when the arguments are not integers
    pytest.raises(TypeError, add, "1", "2")
    pytest.raises(TypeError, add, 1, "2")
    pytest.raises(TypeError, add, "Spam", "Eggs")
    
