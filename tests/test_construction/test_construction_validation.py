"""Tests for construction_validation.py"""
import logging

import pytest
import pandas as pd

from src.construction.construction_validation import (
    check_for_duplicates
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
