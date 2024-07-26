"""Tests for construction_helpers.py"""
import pandas as pd
from pandas._testing import assert_frame_equal
import logging
import pytest
from pytest_mock import mocker

from src.construction.construction_helpers import (
    _convert_formtype,
    prepare_forms_gb,
    prepare_short_to_long,
    clean_construction_type,
    finalise_forms_gb,
)


def test__convert_formtype():
    assert _convert_formtype("1") == "0001"
    assert _convert_formtype("1.0") == "0001"
    assert _convert_formtype("0001") == "0001"
    assert _convert_formtype("6") == "0006"
    assert _convert_formtype("6.0") == "0006"
    assert _convert_formtype("0006") == "0006"
    assert _convert_formtype("2") is None
    assert _convert_formtype("") is None
    assert _convert_formtype(None) is None


class TestPrepareFormGB:
    """Test for prepare_forms_gb()."""

    # Create updated snapshot df
    def create_test_snapshot_df(self):
        """Create a test snapshot df"""
        input_cols = ["reference", "period", "instance", "status", "formtype"]
        data = [
            ["A", 202412, None, "Form sent out", "0001"],
            ["B", 202412, None, "Form sent out", "0006"],
            ["C", 202412, 0, "Other status", "0001"],
            ["D", 202412, 1, "Form sent out", "0006"],
            ["E", 202412, None, "Other status", "0001"],
        ]
        input_snapshot_df = pd.DataFrame(data=data, columns=input_cols)
        return input_snapshot_df

    # Create construction df
    def create_test_construction_df(self):
        """Create a test construction df"""
        input_cols = ["reference", "construction_type", "formtype", "instance", "period"]
        data = [
            ["F", "new", "6", 0, 202412],
            ["G", "short_to_long", "1", 0, 202412],
            ["G", "short_to_long", "1", 1, 202412],
            ["G", "short_to_long", "1", 2, 202412],
            ["H", None, "0001", 1, 202412],
        ]
        input_construction_df = pd.DataFrame(data=data, columns=input_cols)
        return input_construction_df

    # Create an expected dataframe for the test
    def create_expected_snapshot_output(self):
        """Create expected snapshot output df"""
        output_cols = ["reference", "period", "instance", "status", "formtype", "period_year"]
        data = [
            ["A", 202412, 1, "Form sent out", "0001", 2024],
            ["B", 202412, 0, "Form sent out", "0006", 2024],
            ["C", 202412, 0, "Other status", "0001", 2024],
            ["D", 202412, 0, "Form sent out", "0006", 2024],
            ["E", 202412, None, "Other status", "0001", 2024],
        ]
        output_snapshot_df = pd.DataFrame(data=data, columns=output_cols)
        return output_snapshot_df

    # Create construction df
    def create_expected_construction_output(self):
        """Create expected construction output df"""

        output_cols = ["reference", "construction_type", "formtype", "instance", "period", "period_year"]
        data = [
            ["F", "new", "0006", 0, 202412, 2024],
            ["G", "short_to_long", "0001", 0, 202412, 2024],
            ["G", "short_to_long", "0001", 1, 202412, 2024],
            ["G", "short_to_long", "0001", 2, 202412, 2024],
            ["H", None, "0001", 1, 202412, 2024],
        ]
        output_construction_df = pd.DataFrame(data=data, columns=output_cols)
        return output_construction_df

    def test_add_constructed_nonresponders(self):
        """Test for add_constructed_nonresponders()."""
        # Create test dataframes
        input_snapshot_df = self.create_test_snapshot_df()
        input_construction_df = self.create_test_construction_df()
        expected_snapshot_output = self.create_expected_snapshot_output()
        expected_construction_output = self.create_expected_construction_output()

        # Run the function
        snapshot_output, construction_output = prepare_forms_gb(
            input_snapshot_df, input_construction_df
        )

        # Check the output
        assert_frame_equal(snapshot_output.reset_index(drop=True), expected_snapshot_output)
        assert_frame_equal(construction_output.reset_index(drop=True), expected_construction_output)
