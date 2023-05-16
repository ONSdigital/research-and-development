import pytest
from src.data_validation import validate_postcode


import pytest
import pandas as pd
from your_module import validate_post_col

def test_validate_post_col():
    # Valid postcodes
    df_valid = pd.DataFrame({"referencepostcode": ["AB12 3CD", "DE34 5FG", "HI67 8JK"]})
    assert validate_post_col(df_valid) == True

    # Invalid postcodes
    df_invalid = pd.DataFrame({"referencepostcode": ["EFG 456", "HIJ 789"]})
    with pytest.raises(ValueError) as error:
        validate_post_col(df_invalid)
    assert str(error.value) == "Invalid postcodes found: ['EFG 456', 'HIJ 789']"

    # Mixed valid and invalid postcodes
    df_mixed_valid_invalid = pd.DataFrame({"referencepostcode": ["AB12 3CD", "EFG 456", "HI67 8JK"]})
    with pytest.raises(ValueError) as error:
        validate_post_col(df_mixed_valid_invalid)
    assert str(error.value) == "Invalid postcodes found: ['EFG 456']"  # Mixed valid and invalid postcodes

    # Edge cases: invalid column names
    df_invalid_column_name = pd.DataFrame({"postcode": ["AB12 3CD", "EFG 456", "HI67 8JK"]})
    with pytest.raises(KeyError) as error:
        validate_post_col(df_invalid_column_name)
    assert str(error.value) == "'referencepostcode'"  # Invalid column name

    # Edge cases: missing column
    df_missing_column = pd.DataFrame({"other_column": ["value1", "value2", "value3"]})
    with pytest.raises(KeyError) as error:
        validate_post_col(df_missing_column)
    assert str(error.value) == "'referencepostcode'"  # Missing column

    # Edge cases: missing DataFrame
    df_missing_dataframe = None
    with pytest.raises(AttributeError):
        validate_post_col(df_missing_dataframe)  # Missing DataFrame

    # Edge cases: empty reference postcode column
    df_no_postcodes = pd.DataFrame({"referencepostcode": [""]})
    assert validate_post_col(df_no_postcodes) == False  # Empty postcode


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

