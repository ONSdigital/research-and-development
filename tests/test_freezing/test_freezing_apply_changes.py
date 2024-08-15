"""Tests for freezing_apply_changes.py."""

### DEV NOTE: Excluding tests for validate_additions_df and validate_amendment_df
###           due to them carrying out functionality from other functions, with 
###           only additional logs being added.

import pytest
import pandas as pd

from src.freezing.freezing_apply_changes import (
    validate_any_refinst_in_frozen,
    validate_all_refinst_in_frozen,
    apply_amendments,
    apply_additions
)

def create_refinst_df(data: list) -> pd.DataFrame:
    """Create a dataframe with reference/instance columns.

    Args:
        data (list): The data for the dataframe

    Returns:
        pd.DataFrame: The created dataframe.
    """
    columns = ["reference", "instance", "value"]
    df = pd.DataFrame(columns=columns, data=data)
    return df


@pytest.fixture(scope="function")
def dummy_refinst_df() -> pd.DataFrame:
    """A dummy dataframe containing reference+instance."""
    data = [
        [0, 1, True],
        [0, 2, False],
        [1, 1, False],
        [2, 0, True],
        [3, 1, False],
    ]
    df = create_refinst_df(data)
    return df


class TestValidateAnyRefinstInFrozen(object):
    """Tests for validate_any_refinst_in_frozen."""

    def test_validate_any_refinst_in_frozen_true(self, dummy_refinst_df):
        """A test for validate_any_refinst_in_frozen returning 'True'."""
        df2 = create_refinst_df(data=[
                [0, 1, True],#present
                [5, 1, True],#not present
            ]
        )
        result = validate_any_refinst_in_frozen(dummy_refinst_df, df2)
        assert result == True, (
            "validate_any_refinst_in_frozen (true) not behaving as expected."
            )


    def test_validate_any_refinst_in_frozen_false(self, dummy_refinst_df):
        """A test for validate_any_refinst_in_frozen returning 'False'."""
        df2 = create_refinst_df(data=[
                [0, 3, True],#not present
                [5, 1, True],#not present
            ]
        )
        result = validate_any_refinst_in_frozen(dummy_refinst_df, df2)
        assert result == False, (
            "validate_any_refinst_in_frozen (False) not behaving as expected."
            )


class TestValidateAllRefinstInFrozen(object):
    """Tests for validate_all_refinst_in_frozen."""

    def test_validate_all_refinst_in_frozen_true(self, dummy_refinst_df):
        """A test for validate_all_refinst_in_frozen returning 'True'."""
        df2 = create_refinst_df(data=[
                [0, 1, True],#present
                [2, 0, True],#present
            ]
        )
        result = validate_all_refinst_in_frozen(dummy_refinst_df, df2)
        assert result == True, (
            "validate_all_refinst_in_frozen (true) not behaving as expected."
            )

    def test_validate_all_refinst_in_frozen_false(self, dummy_refinst_df):
        """A test for validate_all_refinst_in_frozen returning 'False'."""
        df2 = create_refinst_df(data=[
                [0, 1, True],#present
                [5, 1, True],#not present
            ]
        )
        result = validate_all_refinst_in_frozen(dummy_refinst_df, df2)
        assert result == False, (
            "validate_all_refinst_in_frozen (False) not behaving as expected."
            )
        