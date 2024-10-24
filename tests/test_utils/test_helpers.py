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
            ([1, 2, 3], True),  # True/List
            (pd.Series([0, 5, 8]), True),  # True/Series
            ([1, 2, 4], False),  # False/List
            (pd.Series([15]), False),  # False/Series
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
    """Test for tree_to_list()."""

    # Tests that a tree is correctly converted to a list.
    # Tests that it raises a TypeError if the input is not a dictionary.

    def create_input_tree(self) -> dict:
        '''
        Create a reegular ("good") directory tree for a positive unit-test.
        Args:
            self (class): An instance of TestTreeToList unit-test class.

        Returns:
            tree (dict): An example of regular directory tree as a dictionary,
                with lowest-level directories having empty directpries assigned
                to them. BERD has one level of sub-directories, and PNP has two.
        '''
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

    def create_wrong_type_tree(self) -> dict:
        '''
        Create an "bad" input tree with a type error for negative unit-test.
        
        Args:
            self (class): An instance of TestTreeToList unit-test class.

        Returns:
            bad_tree (dict): An example of  directory tree as a dictionary with
                a type error. The value must be an empty dictionary, but an
                empty list is given.
        '''

        bad_tree = {"BERD": []}
        return bad_tree

    # 
    def create_expected_list(self) -> list:
        '''
        Create an expected output list for the test.
        
        Args:
            self (class): An instance of TestTreeToList unit-test class.

        Returns:
            exp_output_list (list): Expected list of full directory paths.
        '''
        exp_output_list = [
            'R:/2023/BERD', 'R:/2023/BERD/01', 'R:/2023/BERD/02', 'R:/2023/PNP',
            'R:/2023/PNP/03', 'R:/2023/PNP/04', 'R:/2023/PNP/04/qa'
        ]
        return exp_output_list

    def test_tree_to_list_positive(self):
        '''
        Test for tree_to_list(). Runs a positive test to assert that the actual 
        result equls the expected result.        

        Args:
            self (class): An instance of TestTreeToList unit-test class.

        Returns:
            None
        '''
        # Prepare inputs and expected output
        inp_tree = self.create_input_tree()
        exp_output_list = self.create_expected_list()

        # Run positive test
        result_list = tree_to_list(inp_tree, prefix="R:/2023")
        assert result_list == exp_output_list


    def test_tree_to_list_negative(self):
            '''
            Test for tree_to_list(). Runs a negative test to demonstrate that
            the function would raise a TypeError if the input dictionary has
            values other than dictionaries and shows a correct error message.        

            Args:
                self (class): An instance of TestTreeToList unit-test class.

            Returns:
                None
            '''
            # Prepare an "bad" input with deliberate type error
            bad_tree = self.create_wrong_type_tree()

            # Run negative test
            with pytest.raises(TypeError) as excinfo:
                tree_to_list(bad_tree, prefix="R:/2023")
            assert (
                str(excinfo.value) ==
                "Input must be a dictionary, but <class 'list'> is given"
            )
