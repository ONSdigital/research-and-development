"""Tests for construction_validation.py"""
import logging

import pytest
import pandas as pd
import numpy as np

from src.construction.construction_validation import (
    check_for_duplicates,
    validate_construction_references,
    concat_construction_dfs,
    validate_columns_not_empty,
    validate_short_to_long,
)

test_logger = logging.getLogger(__name__)

class TestCheckForDuplicates(object):
    """Tests for check_for_duplicates."""

    @pytest.fixture(scope="function")
    def test_df(self):
        """A test dataframe for check_for_duplicates tests."""
        columns = ["col1", "col2", "col3"]
        data = [
            [1, "a", True],
            [2, "b", True],
            [3, "c", False],
            [3, "c", True],
            [3, "d", False]
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_check_for_duplicates_defence(self, test_df):
        """Defensive tests for check_for_duplicates."""
        msg = f"Column not_a_column is not in the passed dataframe.*"
        with pytest.raises(IndexError , match=msg):
            check_for_duplicates(test_df, columns="not_a_column")

    def test_check_for_duplicates_raises(self, test_df):
        """Check that check_for_duplicates raises when duplicates are present."""
        msg = "Duplicates found in construction file.*"
        with pytest.raises(ValueError, match=msg):
            check_for_duplicates(test_df, columns=["col1", "col2"])

    def test_check_for_duplicates_on_pass(self, test_df, caplog):
        """Test check_for_duplicates logs passing messages."""
        test_df = test_df.drop_duplicates(subset=["col1", "col2"])
        expected_logs = [
            "Checking construction dataframe for duplicates...",
            "No duplicates found in construction files. Continuing..."

        ]
        with caplog.at_level(logging.INFO):
            check_for_duplicates(test_df, ["col1", "col2"], test_logger)
            records = [rec.msg for rec in caplog.records]
            for log in expected_logs:
                assert (log in records), ("Log {log} not found after duplicate check.")


class TestValidateConstructionReferences(object):
    """Tests for validate_construction_references."""

    @pytest.fixture(scope="function")
    def snapshot_df(self) -> pd.DataFrame:
        """Fixture for a dummy snapshot df."""
        columns = ["reference", "instance", "col1"]
        data = [
            [1, 1, True],
            [2, 1, False],
            [2, 2, False],
            [3, 1, True]
        ]
        snap_df = pd.DataFrame(data=data, columns=columns)
        return snap_df

    @pytest.fixture(scope="function")
    def construction_df(self) -> pd.DataFrame:
        """Fixture for a dummy construction df."""
        columns = ["reference", "instance", "col1", "construction_type"]
        data = [
            [1, 1, False, ""],
            [2, 1, False, ""],
            [2, 2, False, ""],
            [3, 1, True, ""],
            [4, 1, True, "new"],
        ]
        const_df = pd.DataFrame(data=data, columns=columns)
        return const_df

    def test_validate_construction_references(
            self,
            snapshot_df,
            construction_df,
            caplog
        ):
        """Tests for passing all constructions (new + non new)."""
        expected_logs = [
            "All passed references from construction file in snapshot",
            "References not marked as new constructions are all in the original dataset",
            "All reference/instance combinations marked as 'new' have been checked against the snapshot and validated.",
        ]
        with caplog.at_level(logging.INFO):
            validate_construction_references(
                construction_df=construction_df,
                snapshot_df=snapshot_df,
                logger=test_logger
            )
            records = [rec.msg for rec in caplog.records]
            for log in expected_logs:
                assert (log in records), ("Log {log} not found.")


    def test_regular_constructions_raises(self, snapshot_df, construction_df):
        """Tests for failing regular constructions (non new)."""
        msg = (
            r"References in construction file are not included in the original "
            rf"data: .*4.*"
        )
        # remove 'new' rows
        construction_df["construction_type"] = ""
        with pytest.raises(ValueError, match=msg):
            validate_construction_references(
                construction_df=construction_df,
                snapshot_df=snapshot_df,
            )


    def test_new_constructions_raises(self, snapshot_df, construction_df):
        """Tests for failing new constructions."""
        msg = (
            r"Reference/instance combinations marked as"
            r" 'new' are already in the dataset: ['4: 1']*"
        )
        # add the 'new' ref to the snapshot df
        snapshot_df = pd.concat(
            [
                snapshot_df, pd.DataFrame(
                    columns=["reference", "instance", "col1"],
                    data=[[4, 1, False]],
                )
            ]
        )
        with pytest.raises(ValueError, match=msg):
            validate_construction_references(
                construction_df=construction_df,
                snapshot_df=snapshot_df,
            )

# test fixtures for re-use
@pytest.fixture(scope="function")
def all_construction_df() -> pd.DataFrame:
    """Small example construction dataframe."""
    columns = ["reference", "instance", "construction_type"]
    data = [
        [0, 1, "new"],
        [0, 2, "new"],
        [1, 1, "short_to_long"],
    ]
    df = pd.DataFrame(data=data, columns=columns)
    return df

@pytest.fixture(scope="function")
def postcode_construction_df() -> pd.DataFrame:
    """Small example postcode construction dataframe."""
    columns = ["reference", "instance", "601"]
    data = [
        [2, 1, "CE11"],
        [2, 2, "CE11"],
        [3, 1, "NP44"],
    ]
    df = pd.DataFrame(data=data, columns=columns)
    return df

class TestConcatConstructionDfs(object):
    """Tests for concat_construction_dfs."""

    def expected_output(self) -> pd.DataFrame:
        """Expected output from concat_construction_dfs."""
        columns = ["reference", "instance", "601", "construction_type"]
        data = [
            [2, 1, "CE11", np.NaN],
            [2, 2, "CE11", np.NaN],
            [3, 1, "NP44", np.NaN],
            [0, 1, np.NaN, "new"],
            [0, 2, np.NaN, "new"],
            [1, 1, np.NaN, "short_to_long"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_concat_construction_dfs(self, all_construction_df, postcode_construction_df):
        """A passing tests for concat_construction_dfs, without duplicate checks."""
        output = concat_construction_dfs(
            postcode_construction_df, all_construction_df
        )
        pd.testing.assert_frame_equal(output, self.expected_output()), (
            "Dataframe not as expected after concating dfs."
        )

    def test_concat_construction_dfs_dupes(
            self,
            all_construction_df,
            postcode_construction_df
    ):
        """Test for duplicates in two construction dataframes."""
        # convert references for ref=0 to ref=2
        all_construction_df.loc[
            all_construction_df.reference==0, "reference"
        ] = 2
        with pytest.raises(
            ValueError, match=r"Duplicates found in construction file.*"
        ):
            concat_construction_dfs(
                all_construction_df,
                postcode_construction_df,
                validate_dupes=True
            )


class TestValidateColumnsNotEmpty(object):
    """Tests for validate_columns_not_empty."""

    def test_validate_columns_not_empty(self, all_construction_df):
        """Passing tests for validate_columns_not_empty."""
        # one column - passed as string
        validate_columns_not_empty(all_construction_df, "reference")
        # two columns - passed as list
        validate_columns_not_empty(all_construction_df, ["reference", "instance"])
        # one column - passed as list
        validate_columns_not_empty(all_construction_df, ["instance"])

    @pytest.mark.parametrize(
            "columns",
            [
                "test",
                ["test"],
                ["test", "test2"],
            ]
    )
    def test_validate_columns_not_empty_col_missing(
            self,
            all_construction_df,
            columns,
        ):
        """Failing tests for validate_columns_not_empty when a column is missing."""
        # normalies var for error message
        if isinstance(columns, str):
            columns = [columns]
        # test if errors are correctly raised
        msg = rf"Column.* test missing from dataframe.*"
        with pytest.raises(IndexError, match=msg):
            validate_columns_not_empty(
                all_construction_df, columns
            )

    def test_validate_columns_not_empty_raises(self, all_construction_df):
        """Test that an error is raised when columns have missing values."""
        # create nan col
        all_construction_df.loc[
            all_construction_df.reference==0, "reference"
        ] = np.NaN
        all_construction_df.loc[
            all_construction_df.instance==1, "instance"
        ] = np.NaN
        msg = "Column.*'reference', 'instance'.* are all empty"
        with pytest.raises(ValueError, match=msg):
            validate_columns_not_empty(
                all_construction_df,
                columns=["reference", "instance"]
            )


class TestValidateShortToLong(object):
    """Tests for validate_short_to_long."""

    @pytest.fixture(scope="function")
    def short_to_long_df(self) -> pd.DataFrame:
        """A small construction df for testing."""
        columns = ["reference", "instance", "period", "formtype"]
        data = [
            [1, 0, 0, "0006"],
            [1, 1, 0, "0006"],
            [2, 0, 0, "0006"],
            [3, 0, 0, "0006"],
            [3, 1, 0, "0006"],
            [3, 0, 1, "0006"],
            [4, 0, 2, "0006"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        df["construction_type"] = "short_to_long"
        return df

    def test_validate_short_to_long(self, short_to_long_df):
        """Passing tests for short_to_long."""
        returned = validate_short_to_long(short_to_long_df)
        assert isinstance(returned, type(None)), (
            "validate_short_to_long returned an unexpected value."
        )

    def test_validate_short_to_long_raises(self, short_to_long_df):
        """Test that validate_short_to_long raises when there is no instance=0."""
        # set up an error condition
        short_to_long_df.loc[short_to_long_df.reference==4, "instance"] = 1
        # test that the error is raised
        msg = (
            "Short to long construction requires a record where instance=0 for "
            "each reference/period.*"
        )
        with pytest.raises(ValueError, match=msg):
            validate_short_to_long(short_to_long_df)


