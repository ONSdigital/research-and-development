"""Tests for utils.helpers."""

import pytest
import pandas as pd

from src.utils.helpers import convert_formtype, values_in_column

def test_convert_formtype():
    """Test for convert_formtype()."""
    msg = "Converted formtype not as expected"
    assert convert_formtype("1") == "0001", msg
    assert convert_formtype("1.0") == "0001", msg
    assert convert_formtype("0001") == "0001", msg
    assert convert_formtype("6") == "0006", msg
    assert convert_formtype("6.0") == "0006", msg
    assert convert_formtype("0006") == "0006", msg
    assert convert_formtype(1) == "0001", msg
    assert convert_formtype("2") is None, msg
    assert convert_formtype("") is None, msg
    assert convert_formtype(None) is None, msg


class TestValuesInColumn(object):
    """Tests for values_in_column."""

    @pytest.fixture(scope="function")
    def dummy_df(self):
        """A dummy dataframe for testing."""
        df = pd.DataFrame(
            {"col": [1, 2, 3, 5, 6, 8, 0]}
        )
        return df
    

    @pytest.mark.parametrize(
            "values, expected",
            [
                ([1, 2, 3], True),#True/List
                (pd.Series([0, 5, 8]), True),#True/Series
                ([1, 2, 4], False),#False/List
                (pd.Series([15]), False),#False/Series
            ]
    )
    def test_valuies_in_column(self, dummy_df, values, expected):
        """General tests for values_in_column."""
        result = values_in_column(
            df=dummy_df,
            col_name="col",
            values=values
        )
        assert result == expected, (
            "Retruned value from values_in_column not as expected."
        )
