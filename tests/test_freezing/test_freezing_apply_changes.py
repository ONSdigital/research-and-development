"""Tests for freezing_apply_changes.py."""

### DEV NOTE: Excluding tests for validate_additions_df and validate_amendment_df
###           due to them carrying out functionality from other functions, with 
###           only additional logs being added.

import logging

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

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
        

@pytest.fixture(scope="function")
def frozen_df() -> pd.DataFrame:
    """A dummy frozen_df for testing."""
    columns = ["reference", "instance", "num", "non_num"]
    data = [
        [0, 1, 4, True],
        [0, 2, 5, False],
        [1, 1, 8, True],
        [1, 2, 9, True],
        [2, 1, 10, False]
    ]
    df = pd.DataFrame(columns=columns, data=data)
    return df

# create a test logger to pass to functions
test_logger = logging.getLogger(__name__)


class TestApplyAmendments(object):
    """Tests for apply_amendments."""

    @pytest.fixture(scope="function")
    def dummy_amendments(self) -> pd.DataFrame:
        """A dummy amendments dataframe."""
        columns = ["reference", "instance", "num_updated", "non_num_updated", "accept_changes"]
        data = [
            [0, 1, 3, True, True],
            [0, 2, 4, True, False],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def expected_amended(self) -> pd.DataFrame:
        """The expected dataframe after amendments are applied."""
        columns = ["reference", "instance", "num", "non_num"]
        data = [
            [0, 1, 3, True],
            [0, 2, 4, True],
            [1, 1, 8, True],
            [1, 2, 9, True],
            [2, 1, 10, False]
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_apply_amendments(self, frozen_df, dummy_amendments):
        """General tests for apply_amendments"""
        amended = apply_amendments(frozen_df, dummy_amendments, 1, test_logger)
        amended.drop("last_frozen", axis=1, inplace=True)
        amended.sort_values(by=["reference", "instance"], ascending=True, inplace=True)
        expected = self.expected_amended()
        assert_frame_equal(amended, expected), (
            "Amendments not applied as expected."
        )
        
    def test_apply_amendments_invalid(self, frozen_df, dummy_amendments, caplog):
        """Tests for apply_amendments when amendments_df is invalid."""
        with caplog.at_level(logging.INFO):
            # alter additions
            dummy_amendments["reference"] = 6
            result = apply_amendments(frozen_df, dummy_amendments, 1, test_logger)
            assert_frame_equal(result, frozen_df), (
                "Original df not returned when amendments are invalid."
            )
            # check logger messages
            expected_logs = [
                "Skipping amendments since the amendments csv is invalid..."

            ]
            records = [rec.msg for rec in caplog.records]
            for log in expected_logs:
                assert (log in records), ("error")

    def test_apply_amendments_no_amendments(self, frozen_df, dummy_amendments, caplog):
        """Tests for apply_amendments when the amendments df is empty."""
        with caplog.at_level(logging.INFO):
            # alter additions
            dummy_amendments["accept_changes"] = False
            result = apply_amendments(frozen_df, dummy_amendments, 1, test_logger)
            assert_frame_equal(result, frozen_df), (
                "Original df not returned when no amendments are found."
            )
            # check logger messages
            expected_logs = [
                "Amendments file contained no records marked for inclusion"

            ]
            records = [rec.msg for rec in caplog.records]
            for log in expected_logs:
                assert (log in records), ("error")


class TestApplyAdditions(object):
    """Tests for apply_additions."""

    @pytest.fixture(scope="function")
    def dummy_additions(self) -> pd.DataFrame:
        """A dummy amendments dataframe."""
        columns = ["reference", "instance", "num", "non_num", "accept_changes"]
        data = [
            [3, 0, 10, True, True],
            [3, 1, 11, False, False],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def expected_additions(self) -> pd.DataFrame:
        """The expected dataframe after amendments are applied."""
        columns = ["reference", "instance", "num", "non_num"]
        data = [
            [0, 1, 4, True],
            [0, 2, 5, False],
            [1, 1, 8, True],
            [1, 2, 9, True],
            [2, 1, 10, False],
            [3, 0, 10, True],
            [3, 1, 11, False],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_apply_additions(self, frozen_df, dummy_additions):
        """General tests for apply_additions"""
        amended = apply_additions(frozen_df, dummy_additions, 1, test_logger)
        amended.drop("last_frozen", axis=1, inplace=True)
        amended.sort_values(by=["reference", "instance"], ascending=True, inplace=True)
        expected = self.expected_additions()
        print(amended)
        print(expected)
        assert_frame_equal(amended, expected), (
            "Additions not applied as expected."
        )

    def test_apply_additions_invalid(self, frozen_df, dummy_additions, caplog):
        """Tests for apply_additions when additions_df is invalid."""
        with caplog.at_level(logging.INFO):
            # alter amendments
            dummy_additions["reference"] = 0
            result = apply_additions(frozen_df, dummy_additions, 1, test_logger)
            print(result)
            assert_frame_equal(result, frozen_df), (
                "Original df not returned when additions are invalid"
            )
            # check logger messages
            expected_logs = [
                "Skipping additions since the additions csv is invalid..."

            ]
            records = [rec.msg for rec in caplog.records]
            for log in expected_logs:
                assert (log in records), ("error")

    def test_apply_additions_no_amendments(self, frozen_df, dummy_additions, caplog):
        """Tests for apply_additions when the additions df is empty."""
        with caplog.at_level(logging.INFO):
            # alter amendments
            dummy_additions["accept_changes"] = False
            result = apply_additions(frozen_df, dummy_additions, 1, test_logger)
            assert_frame_equal(result, frozen_df), (
                "Original df not returned when additions are invalid..."
            )
            # check logger messages
            expected_logs = [
                "Additions file contained no records marked for inclusion"

            ]
            records = [rec.msg for rec in caplog.records]
            for log in expected_logs:
                assert (log in records), ("error")
