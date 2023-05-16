import pytest
from src.data_validation import validate_postcode

def test_validate_postcode():
    # Valid postcodes
    assert validate_postcode("AB12 3CD") == True
    assert validate_postcode("DE34 5FG") == True
    assert validate_postcode("HI67 8JK") == True

    # Invalid postcodes
    assert validate_postcode("EFG 456") == False
    assert validate_postcode("HIJ 789") == False
    assert validate_postcode("KL1M 2NO") == False
    assert validate_postcode("B27 OAG") == False  # Zero is actually an "O"

    # Edge cases
    assert validate_postcode(None) == False  # None value should fail
    assert validate_postcode("") == False  # Empty string
    assert validate_postcode(" ") == False  # Whitespace
    assert validate_postcode("AB123CD") == False  # Missing space - othewise valid
    assert validate_postcode("ABC XYZ") == False  # All letters but right length
    assert validate_postcode("123 456") == False  # All numbers but right length

