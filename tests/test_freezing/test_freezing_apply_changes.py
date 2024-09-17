"""Tests for freezing_apply_changes.py."""

### DEV NOTE: Excluding tests for validate_additions_df and validate_amendment_df
###           due to them carrying out functionality from other functions, with
###           only additional logs being added.

import logging

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import datetime

from src.freezing.freezing_apply_changes import (
    validate_any_refinst_in_frozen,
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
                [0, 1, True], # present
                [5, 1, True], # not present
            ]
        )
        result = validate_any_refinst_in_frozen(dummy_refinst_df, df2)
        assert result == True, (
            "validate_any_refinst_in_frozen (true) not behaving as expected."
            )


    def test_validate_any_refinst_in_frozen_false(self, dummy_refinst_df):
        """A test for validate_any_refinst_in_frozen returning 'False'."""
        df2 = create_refinst_df(data=[
                [0, 3, True], # not present
                [5, 1, True], # not present
            ]
        )
        result = validate_any_refinst_in_frozen(dummy_refinst_df, df2)
        assert result == False, (
            "validate_any_refinst_in_frozen (False) not behaving as expected."
            )

@pytest.fixture(scope="function")
def frozen_df() -> pd.DataFrame:
    """A dummy frozen_df for testing."""
    columns = ["reference", "instance", "num", "non_num", "last_frozen", "status"]
    data = [
        [0, 1.0, 4, True, "previous_run", "clear"],
        [0, 2.0, 5, False, "previous_run", "clear"],
        [1, 1.0, 8, True, "previous_run", "clear"],
        [1, 2.0, 9, True, "previous_run", "clear"],
        [2, 1.0, 10, False, "previous_run", "clear"],
        [6, None, 10, False, "previous_run", "Form sent out"]
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
        columns = ["reference", "instance", "num_updated", "non_num_updated", "accept_changes", "status"]
        data = [
            [0, 1.0, 3, True, True, "clear"],
            [0, 2.0, 4, True, False, "clear"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def expected_amended(self) -> pd.DataFrame:
        """The expected dataframe after amendments are applied."""
        # get date
        today = datetime.datetime.now().strftime("%y-%m-%d")
        columns = ["reference", "instance", "num", "non_num", "last_frozen", "status"]
        data = [
            [0, 1.0, 3, True, f"{today}_v1", "clear"],
            [0, 2.0, 4, True, f"{today}_v1", "clear"],
            [1, 1.0, 8, True, "previous_run", "clear"],
            [1, 2.0, 9, True, "previous_run", "clear"],
            [2, 1.0, 10, False, "previous_run", "clear"],
            [6, None, 10, False, "previous_run", "Form sent out"]
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_apply_amendments(self, frozen_df, dummy_amendments):
        """General tests for apply_amendments"""
        amended = apply_amendments(frozen_df, dummy_amendments, 1, test_logger)
        amended.sort_values(by=["reference", "instance"], ascending=True, inplace=True)
        expected = self.expected_amended()
        print(amended)
        print(expected)
        assert_frame_equal(amended, expected), (
            "Amendments not applied as expected."
        )

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
        columns = ["reference", "instance", "num", "non_num", "accept_changes", "status"]
        data = [
            [3, 0.0, 10, True, True, "clear"],
            [3, 1.0, 11, False, False, "clear"],
            [6, 0.0, 10, False, True, "clear"]
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def expected_additions(self) -> pd.DataFrame:
        """The expected dataframe after amendments are applied."""
        columns = ["reference", "instance", "num", "non_num", "status"]
        data = [
            [0, 1.0, 4, True, "clear"],
            [0, 2.0, 5, False, "clear"],
            [1, 1.0, 8, True, "clear"],
            [1, 2.0, 9, True, "clear"],
            [2, 1.0, 10, False, "clear"],
            [3, 0.0, 10, True, "clear"],
            [3, 1.0, 11, False, "clear"],
            [6, 0.0, 10, False, "clear"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_apply_additions(self, frozen_df, dummy_additions):
        """General tests for apply_additions"""
        amended = apply_additions(frozen_df, dummy_additions, 1, test_logger)
        amended.drop("last_frozen", axis=1, inplace=True)
        amended.sort_values(by=["reference", "instance"], ascending=True, inplace=True)
        expected = self.expected_additions()
        assert_frame_equal(amended, expected), (
            "Additions not applied as expected."
        )

    def test_apply_additions_invalid(self, frozen_df, dummy_additions, caplog):
        """Tests for apply_additions when additions_df is invalid."""
        with caplog.at_level(logging.INFO):
            # alter amendments
            dummy_additions["reference"] = 1
            result = apply_additions(frozen_df, dummy_additions, 1, test_logger)
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
