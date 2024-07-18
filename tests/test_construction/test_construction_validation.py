"""Tests for construction_validation.py"""
import logging

import pytest
import pandas as pd

from src.construction.construction_validation import (
    check_for_duplicates,
    validate_construction_references,
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
        """Tests for passing all constructors (new + non new)."""
        expected_logs = [
            "All passed references from construction file in snapshot",
            "References not marked as new constructors are all in the original dataset",
            "Reference(s) in construction file not in snapshot: [4]",
            "All references marked as 'new' are validated as new.",
        ]
        with caplog.at_level(logging.INFO):
            validate_construction_references(
                df=construction_df,
                snapshot_df=snapshot_df,
                logger=test_logger
            )
            records = [rec.msg for rec in caplog.records]
            for log in expected_logs:
                assert (log in records), ("Log {log} not found.")
        

    def test_regular_constructors_raises(self, snapshot_df, construction_df):
        """Tests for failing regular constructors (non new)."""
        msg = (
            r"References in construction file are not included in the original "
            rf"data: .*4.*"
        )
        # remove 'new' rows
        construction_df["construction_type"] = ""
        with pytest.raises(ValueError, match=msg):
            validate_construction_references(
                df=construction_df,
                snapshot_df=snapshot_df,
            )


    def test_new_constructors_raises(self, snapshot_df, construction_df):
        """Tests for failing new constructors."""
        msg = (
            r"References in the construction file labelled 'new' are already in"
            r" the dataset.*"
        )
        # add the 'new' ref to the snapshot df
        snapshot_df = snapshot_df.append(pd.DataFrame(
            columns=["reference", "instance", "col1"],
            data=[[4, 1, False]]
        ))
        with pytest.raises(ValueError, match=msg):
            validate_construction_references(
                df=construction_df,
                snapshot_df=snapshot_df,
            )
            