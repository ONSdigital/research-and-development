"""Tests for freezing_compare.py."""

import pandas as pd
from pandas.testing import assert_frame_equal
import logging

from src.freezing.freezing_compare import get_amendments, get_additions
from src.freezing.freezing_compare import bring_together_split_cases

# create a test logger to pass to functions
test_logger = logging.getLogger(__name__)

class TestGetAmendments:
    """Tests for get_amendments()."""

    def add_numeric_cols(
        self,
        df,
        expected = False
    ) -> pd.DataFrame:
        """Numerical columns for test dataframes."""
        numeric_cols = [
            "201", "204", "205", "206", "207", "209", "210",
            "211", "212", "214", "216", "218", "219", "220", "221", "222",
            "223", "225", "226", "227", "228", "229", "237", "242", "243",
            "244", "245", "246", "247", "248", "249", "250", "405", "406",
            "407", "408", "409", "410", "411", "412", "501", "502", "503",
            "504", "505", "506", "507", "508", "602", "701", "702", "703",
            "704", "705", "706", "707", "709", "711",
        ]
        for col in numeric_cols:
            df[col] = None
        if expected:
            for col in numeric_cols:
                df[f"{col}_diff"] = None
        return df


    # Create test frozen df
    def create_test_frozen_df(self) -> pd.DataFrame:
        """Create a test frozen df."""
        input_cols = ["reference", "period", "instance", "203", "202", "200", "601"]
        data = [
            ["A", 202412, 2.0, 1.0, 2.0, "A", None],
            ["B", 202412, None, None, 1.0, "B", "C"],
            ["C", 202412, 0.0, 1.0, 2.0, "A", "B"],
            ["D", 202412, 1.0, 2.0, 3.0, "C", "D"],
            ["E", 202412, None, 4.0, 5.0, "E", "F"],
        ]
        input_frozen_df = pd.DataFrame(data=data, columns=input_cols)
        input_frozen_df = self.add_numeric_cols(input_frozen_df)
        return input_frozen_df


    # Create test amendments df
    def create_test_amendments_df(self) -> pd.DataFrame:
        """Create a test amendments df."""
        input_cols = ["reference", "period", "instance", "203", "202", "200", "601"]
        data = [
            ["A", 202412, 2.0, 1.0, 2.0, "A", None], # No diffs
            ["B", 202412, None, None, 1.0, "A", "B"], # 200 diff "A"
            ["C", 202412, 0.0, 2.0, 2.0, "A", "B"], # 201 diff by 1
            ["D", 202412, 1.0, 2.0, 3.0, "E", "D"],  # 200 & 601 diff "E", "D"
            ["E", 202412, None, 10.0, 1.0, "E", "F"], # 201 & 202 by 6, -4
        ]
        input_amendments_df = pd.DataFrame(data=data, columns=input_cols)
        input_amendments_df = self.add_numeric_cols(input_amendments_df)
        return input_amendments_df


    # Create expected outcome df
    def create_test_expected_outcome_df(self) -> pd.DataFrame:
        """Create a test expected_outcome df."""
        input_cols = ["reference", "period", "instance", "203", "202", "200", "601", "203_diff", "202_diff", "200_diff", "601_diff", "accept_changes"]
        data = [
            ["A", 202412, 2.0, 1.0, 2.0, "A", None, 0.0, 0.0, None, None, False],
            ["B", 202412, None, None, 1.0, "A", "B", None, 0.0, "A", "B", False],
            ["C", 202412, 0.0, 2.0, 2.0, "A", "B", 1.0, 0.0, None, None, False],
            ["D", 202412, 1.0, 2.0, 3.0, "E", "D", 0.0, 0.0, "E", None, False],
            ["E", 202412, None, 10.0, 1.0, "E", "F", 6.0, -4.0, None, None, False],
        ]
        input_expected_outcome_df = pd.DataFrame(data=data, columns=input_cols)
        input_expected_outcome_df = self.add_numeric_cols(input_expected_outcome_df, expected = True)
        return input_expected_outcome_df


    def test_get_amendments(self):
        """Test for get_amendments()."""
        # Create test dataframes
        input_frozen_df = self.create_test_frozen_df()
        input_amendments_df = self.create_test_amendments_df()
        expected_outcome_df = self.create_test_expected_outcome_df()

        # Run the function
        result = get_amendments(
            input_frozen_df, input_amendments_df, test_logger
        )

        expected_outcome_df = expected_outcome_df[result.columns]

        # Check the output
        assert_frame_equal(
            expected_outcome_df, result
        )


class TestGetAdditions:
    """Tests for get_additions()."""
    # Create test frozen df
    def create_test_frozen_df(self) -> pd.DataFrame:
        """Create a test frozen df"""
        input_cols = ["reference", "period", "instance", "other"]
        data = [
            ["A", 202412, 2.0, 1.0],
            ["B", 202412, None, None],
            ["C", 202412, 0.0, 1.0],
            ["D", 202412, 1.0, 2.0],
            ["E", 202412, None, 4.0],
        ]
        input_frozen_df = pd.DataFrame(data=data, columns=input_cols)
        return input_frozen_df


    # Create test additions df
    def create_test_additions_df(self) -> pd.DataFrame:
        """Create a test additions df."""
        input_cols = ["reference", "period", "instance", "other"]
        data = [
            ["A", 202412, 2.0, 1.0],
            ["B", 202412, None, None],
            ["C", 202412, 0.0, 1.0],
            ["D", 202412, 1.0, 2.0],
            ["E", 202412, None, None],
            ["F", 202412, 1.0, 4.0],
            ["G", 202412, None, 4.0],
            ["H", 202412, 1.0, None]
        ]
        input_additions_df = pd.DataFrame(data=data, columns=input_cols)
        return input_additions_df


    # Create expected outcome df
    def create_test_expected_outcome_df(self) -> pd.DataFrame:
        """Create a test expected_outcome df."""
        input_cols = ["reference", "period", "instance", "other", "accept_changes"]
        data = [
            ["F", 202412, 1.0, 4.0, False],
            ["G", 202412, None, 4.0, False],
            ["H", 202412, 1.0, None, False]
        ]
        input_expected_outcome_df = pd.DataFrame(data=data, columns=input_cols)
        return input_expected_outcome_df


    def test_get_additions(self):
        """Test for get_additions()."""
        # Create test dataframes
        input_frozen_df = self.create_test_frozen_df()
        input_additions_df = self.create_test_additions_df()
        expected_outcome_df = self.create_test_expected_outcome_df()

        # Run the function
        result = get_additions(
            input_frozen_df, input_additions_df, test_logger
        )

        # Check the output
        assert_frame_equal(
            expected_outcome_df, result.reset_index(drop=True)
        )


class TestBringTogetherSplitCases:
    """Tests for bring_together_split_cases()."""

    def create_test_additions_df(self) -> pd.DataFrame:
        """Create a test additions df."""
        input_cols = ["reference", "period", "instance", "other"]
        data = [
            ["A", 202412, 2.0, 1.0],
            ["B", 202412, None, None],
            ["C", 202412, 0.0, 1.0],
            ["D", 202412, 1.0, 2.0],
            ["E", 202412, None, 4.0],
        ]
        input_additions_df = pd.DataFrame(data=data, columns=input_cols)
        return input_additions_df

    def create_test_amendments_df(self) -> pd.DataFrame:
        """Create a test amendments df."""
        input_cols = ["reference", "period", "instance", "other"]
        data = [
            ["C", 202412, 0.0, 1.0],
            ["D", 202412, 1.0, 2.0],
            ["F", 202412, None, 4.0],
        ]
        input_amendments_df = pd.DataFrame(data=data, columns=input_cols)
        return input_amendments_df

    def create_expected_additions_df(self) -> pd.DataFrame:
        """Create the expected additions df after split cases are handled."""
        input_cols = ["reference", "period", "instance", "other"]
        data = [
            ["A", 202412, 2.0, 1.0],
            ["B", 202412, None, None],
            ["E", 202412, None, 4.0],
        ]
        expected_additions_df = pd.DataFrame(data=data, columns=input_cols)
        return expected_additions_df

    def create_expected_amendments_df(self) -> pd.DataFrame:
        """Create the expected amendments df after split cases are handled."""
        input_cols = ["reference", "period", "instance", "other"]
        data = [
            ["C", 202412, 0.0, 1.0],
            ["D", 202412, 1.0, 2.0],
            ["F", 202412, None, 4.0],
            ["C", 202412, 0.0, 1.0],
            ["D", 202412, 1.0, 2.0],
        ]
        expected_amendments_df = pd.DataFrame(data=data, columns=input_cols)
        return expected_amendments_df

    def test_bring_together_split_cases(self):
        """Test for bring_together_split_cases()."""
        # Create test dataframes
        input_additions_df = self.create_test_additions_df()
        input_amendments_df = self.create_test_amendments_df()
        expected_additions_df = self.create_expected_additions_df()
        expected_amendments_df = self.create_expected_amendments_df()

        # Run the function
        result_additions_df, result_amendments_df = bring_together_split_cases(
            input_additions_df, input_amendments_df, test_logger
        )

        # Check the output
        assert_frame_equal(
            expected_additions_df, result_additions_df.reset_index(drop=True)
        )

        assert_frame_equal(
            expected_amendments_df, result_amendments_df.reset_index(drop=True)
        )



