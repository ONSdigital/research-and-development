"""Tests for utils.helpers."""

import pytest
import pandas as pd

from src.utils.helpers import (
    convert_formtype, values_in_column, tree_to_list
)

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
            "Returned value from values_in_column not as expected."
        )


class TestTreeToList:
    """Test for tree_to_list()"""

    # Tests that a tree is correctly converted to a list.
    # Tests that it raises a TypeError if the input is not a dictionary.

    # Create a good input tree
    def create_input_tree(self):
        tree = {
            "BERD": {
                "01": {},
                "02": {},
            },
            "PNP": {
                "03": {},
                "04": {"qa": {}},
            },
        }
        return tree

    # Create an expected dataframe for the test
    def create_expected_list(self):
        exp_output_list = [
            'R:/2023/BERD', 'R:/2023/BERD/01', 'R:/2023/BERD/02', 'R:/2023/PNP',
            'R:/2023/PNP/03', 'R:/2023/PNP/04', 'R:/2023/PNP/04/qa'
        ]
        return exp_output_list

    def test_tree_to_list(self):
        """Test for atree_to_list()"""
        inp_tree = self.create_input_tree()
        exp_output_list = self.create_expected_list()

        result_list = tree_to_list(inp_tree, prefix="R:/2023")
        assert result_list == exp_output_list
