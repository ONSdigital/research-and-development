"""Tests for construction_helpers.py."""
import pandas as pd
from pandas._testing import assert_frame_equal

from src.construction.construction_helpers import (
    remove_short_to_long_0,
)


class TestRemoveShortToLong0:
    """Test for add_constructed_nonresponders()."""

    # Create updated snapshot df
    def create_test_snapshot_df(self) -> pd.DataFrame:
        """Create a test snapshot df."""
        input_cols = ["reference", "instance"]
        data = [
            ["A", 1],
            ["B", 0],
            ["B", 1],
            ["B", 2],
            ["C", 0],
            ["C", 1],
            ["C", 2],
        ]
        input_snapshot_df = pd.DataFrame(data=data, columns=input_cols)
        return input_snapshot_df

    # Create construction df
    def create_test_construction_df(self) -> pd.DataFrame:
        """Create a test construction df."""
        input_cols = ["reference", "construction_type"]
        data = [
            ["A", "new"],
            ["B", "short_to_long"],
            ["C", None],
        ]
        input_construction_df = pd.DataFrame(data=data, columns=input_cols)
        return input_construction_df

    # Create an expected dataframe for the test
    def create_expected_snapshot_output(self) -> pd.DataFrame:
        """Create expected snapshot output df."""
        output_cols = ["reference", "instance"]
        data = [
            ["A", 1],
            ["B", 1],
            ["B", 2],
            ["C", 0],
            ["C", 1],
            ["C", 2],
        ]
        output_snapshot_df = pd.DataFrame(data=data, columns=output_cols)
        return output_snapshot_df

    def test_remove_short_to_long_0(self):
        """Test for add_constructed_nonresponders()."""
        # Create test dataframes
        input_snapshot_df = self.create_test_snapshot_df()
        input_construction_df = self.create_test_construction_df()
        expected_snapshot_output = self.create_expected_snapshot_output()

        # Run the function
        snapshot_output = remove_short_to_long_0(
            input_snapshot_df, input_construction_df
        )

        # Check the output
        assert_frame_equal(snapshot_output.reset_index(drop=True), expected_snapshot_output), "Output is not as expected"
