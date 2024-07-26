"""Tests for construction_helpers.py"""
import pandas as pd
from pandas._testing import assert_frame_equal

from src.construction.construction_helpers import (
    add_constructed_nonresponders,
)

class TestAddConstructedNonresponders:
    """Test for add_constructed_nonresponders()."""

    # Create updated snapshot df
    def create_test_snapshot_df(self):
        """Create a test snapshot df"""
        input_cols = ["reference"]
        data = [
            ["A"],
            ["B"],
            ["C"],
            ["D"],
            ["E"],
        ]
        input_snapshot_df = pd.DataFrame(data=data, columns=input_cols)
        return input_snapshot_df

    # Create construction df
    def create_test_construction_df(self):
        """Create a test construction df"""
        input_cols = ["reference", "construction_type"]
        data = [
            ["F", "new"],
            ["G", "short_to_long"],
            ["H", None],
            ["I", "new"],
            ["J", None],
        ]
        input_construction_df = pd.DataFrame(data=data, columns=input_cols)
        return input_construction_df

    # Create an expected dataframe for the test
    def create_expected_snapshot_output(self):
        """Create expected snapshot output df"""
        output_cols = ["reference", "construction_type"]
        data = [
            ["A", None],
            ["B", None],
            ["C", None],
            ["D", None],
            ["E", None],
            ["F", "new"],
            ["I", "new"],
        ]
        output_snapshot_df = pd.DataFrame(data=data, columns=output_cols)
        return output_snapshot_df

    # Create construction df
    def create_expected_construction_output(self):
        """Create expected construction output df"""

        output_cols = ["reference", "construction_type"]

        data = [
            ["G", "short_to_long"],
            ["H", None],
            ["J", None],
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
    snapshot_output, construction_output = add_constructed_nonresponders(
        input_snapshot_df, input_construction_df
    )

    # Check the output
    assert_frame_equal(snapshot_output, expected_snapshot_output)
    assert_frame_equal(construction_output, expected_construction_output)
