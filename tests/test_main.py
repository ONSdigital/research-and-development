"""Create a test suite for the main module."""

# test the add function from testfunctions.py which is imported into main.py
def test_add():
    """Test the add function."""
    from src.utils.testfunctions import add
    assert add(1, 2) == 3
