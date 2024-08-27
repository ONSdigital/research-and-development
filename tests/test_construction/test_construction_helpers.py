"""Tests for construction_helpers.py."""
import pandas as pd
import numpy as np
import pytest
from pandas._testing import assert_frame_equal


from src.construction.construction_helpers import (
    prepare_forms_gb,
    prepare_short_to_long,
    clean_construction_type,
    finalise_forms_gb,
    add_constructed_nonresponders,
    remove_short_to_long_0,
    prep_new_rows,
)

class TestPrepareFormGB:
    """Tests for prepare_forms_gb()."""

    # Create updated snapshot df
    def create_test_snapshot_df(self) -> pd.DataFrame:
        """Create a test snapshot df."""
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
    def create_test_construction_df(self) -> pd.DataFrame:
        """Create a test construction df."""
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
    def create_expected_snapshot_output(self) -> pd.DataFrame:
        """Create expected snapshot output df."""
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
    def create_expected_construction_output(self) -> pd.DataFrame:
        """Create expected construction output df."""

        output_cols = ["reference", "construction_type", "formtype", "instance", "period", "604", "period_year"]
        data = [
            ["F", "new", "0006", 0, 202412, None, 2024],
            ["G", "short_to_long", "0001", 0, 202412, "Yes", 2024],
            ["G", "short_to_long", "0001", 1, 202412, "Yes", 2024],
            ["G", "short_to_long", "0001", 2, 202412, "Yes", 2024],
            ["H", None, "0001", 1, 202412, None, 2024],
        ]
        output_construction_df = pd.DataFrame(data=data, columns=output_cols)
        return output_construction_df

    def test_prepare_forms_gb(self):
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
        assert_frame_equal(
            snapshot_output.reset_index(drop=True), expected_snapshot_output
        ), "Snapshot output is not as expected"
        assert_frame_equal(
            construction_output.reset_index(drop=True), expected_construction_output
        ), "Construction output is not as expected"


class TestPrepareShortToLong:
    """Test for prepare_short_to_long()."""

    # Create updated snapshot df
    def create_test_snapshot_df(self) -> pd.DataFrame:
        """Create a test snapshot df."""
        input_cols = ["reference", "instance", "other"]
        data = [
            ["A", 0, None],
            ["B", 0, "A"],
            ["C", 1, "A"],
        ]
        input_snapshot_df = pd.DataFrame(data=data, columns=input_cols)
        return input_snapshot_df

    # Create construction df
    def create_test_construction_df(self) -> pd.DataFrame:
        """Create a test construction df."""
        input_cols = ["reference", "instance", "construction_type"]
        data = [
            ["A", 0, None],
            ["B", 0, "short_to_long"],
            ["B", 1, "short_to_long"],
            ["B", 2, "short_to_long"],
            ["C", 1, "new"],
        ]
        input_construction_df = pd.DataFrame(data=data, columns=input_cols)
        return input_construction_df

    # Create an expected dataframe for the test
    def create_expected_snapshot_output(self) -> pd.DataFrame:
        """Create expected snapshot output df."""
        output_cols = ["reference", "instance", "other"]
        data = [
            ["A", 0, None],
            ["B", 0, "A"],
            ["B", 1, "A"],
            ["B", 2, "A"],
            ["C", 1, "A"],
        ]
        output_snapshot_df = pd.DataFrame(data=data, columns=output_cols)
        return output_snapshot_df

    def test_prepare_short_to_long(self):
        """Test for prepare_short_to_long()."""
        # Create test dataframes
        input_snapshot_df = self.create_test_snapshot_df()
        input_construction_df = self.create_test_construction_df()
        expected_snapshot_output = self.create_expected_snapshot_output()

        # Run the function
        snapshot_output = prepare_short_to_long(
            input_snapshot_df, input_construction_df
        )

        snapshot_output = snapshot_output.sort_values(
            ["reference", "instance"], ascending=[True, True]
        ).reset_index(drop=True)

        # Check the output
        assert_frame_equal(
            snapshot_output.reset_index(drop=True), expected_snapshot_output
        ), "Snapshot output is not as expected"


def test_clean_construction_type():
    """Test for clean_construction_type()."""
    msg = "Cleaned construction type not as expected"
    assert clean_construction_type("new") == "new", msg
    assert clean_construction_type("Short to Long") == "short_to_long", msg
    assert clean_construction_type("  ") is np.NaN, msg
    assert clean_construction_type("") is np.NaN, msg
    assert clean_construction_type(None) is np.NaN, msg


class TestFinaliseFormsGB:
    """Test for finalise_forms_gb()."""

    # Create updated snapshot df
    def create_test_snapshot_df(self) -> pd.DataFrame:
        """Create a test snapshot df."""
        input_cols = ["reference", "formtype", "instance", "601", "referencepostcode", "postcodes_harmonised", "status", "is_constructed"]
        data = [
            ["A", "0001", 1, "AB12 3CD", None, None, "Form sent out", False],
            ["B", "0006", 0, None, "AB12 3CD", "AB12 3CD", "Form sent out", True],
            ["C", "0001", 1, "X11 1XX", "AB12 3CD", None, "Form sent out", True],
            ["D", "0006", 0, None, "X11 1XX", None, "Other", True],
            ["E", "0006", 0, None, None, None, "Other", False],
            ["F", "0001", 1, "ab12 3cd", None, None, "Other", True],
        ]
        input_snapshot_df = pd.DataFrame(data=data, columns=input_cols)
        return input_snapshot_df

    # Create an expected dataframe for the test
    def create_expected_snapshot_output(self) -> pd.DataFrame:
        """Create expected snapshot output df."""
        output_cols = ["reference", "formtype", "instance", "601", "referencepostcode", "postcodes_harmonised", "status", "is_constructed"]
        data = [
            ["A", "0001", 1, "AB12 3CD", None, None, "Form sent out", False],
            ["B", "0006", None, None, "AB12 3CD", "AB12 3CD", "Form sent out", True],
            ["C", "0001", 1, "X11  1XX", "AB12 3CD", "X11  1XX", "Form sent out", True],
            ["D", "0006", 0, None, "X11  1XX", "X11  1XX", "Other", True],
            ["E", "0006", 0, None, None, None, "Other", False],
            ["F", "0001", 1, "AB12 3CD", None, "AB12 3CD", "Other", True],
        ]
        output_snapshot_df = pd.DataFrame(data=data, columns=output_cols)
        return output_snapshot_df

    def test_finalise_forms_gb(self):
        """Test for finalise_forms_gb()."""
        # Create test dataframes
        input_snapshot_df = self.create_test_snapshot_df()
        expected_snapshot_output = self.create_expected_snapshot_output()

        # Run the function
        snapshot_output = finalise_forms_gb(input_snapshot_df)

        snapshot_output = snapshot_output.sort_values("reference").reset_index(drop=True)

        # Check the output
        assert_frame_equal(
            snapshot_output, expected_snapshot_output
        ), "Snapshot output is not as expected"


class TestAddConstructedNonresponders:
    """Test for add_constructed_nonresponders()."""

    # Create updated snapshot df
    def create_test_snapshot_df(self) -> pd.DataFrame:
        """Create a test snapshot df"""
        input_cols = ["reference", "formtype", "cellnumber"]
        data = [
            ["A", 1, 12],
            ["B", 1, 12],
            ["C", 1, 12],
            ["D", 1, 12],
            ["E", 1, 12],
        ]
        input_snapshot_df = pd.DataFrame(data=data, columns=input_cols)
        return input_snapshot_df

    # Create construction df
    def create_test_construction_df(self) -> pd.DataFrame:
        """Create a test construction df."""
        input_cols = ["reference", "construction_type", "formtype", "cellnumber"]
        data = [
            ["F", "new", 1, 12],
            ["G", "short_to_long", 1, 12],
            ["H", None, 1, 12],
            ["I", "new", 1, 12],
            ["J", None, 1, 12],
        ]
        input_construction_df = pd.DataFrame(data=data, columns=input_cols)
        return input_construction_df

    # Create an expected dataframe for the test
    def create_expected_snapshot_output(self) -> pd.DataFrame:
        """Create expected snapshot output df."""
        output_cols = ["reference", "formtype", "cellnumber"]
        data = [
            ["A", 1, 12],
            ["B", 1, 12],
            ["C", 1, 12],
            ["D", 1, 12],
            ["E", 1, 12],
            ["F", 1, 12],
            ["I", 1, 12],
        ]
        output_snapshot_df = pd.DataFrame(data=data, columns=output_cols)
        return output_snapshot_df

    # Create construction df
    def create_expected_construction_output(self) -> pd.DataFrame:
        """Create expected construction output df."""

        output_cols = ["reference", "construction_type", "formtype", "cellnumber"]

        data = [
            ["G", "short_to_long", 1, 12],
            ["H", None, 1, 12],
            ["J", None, 1, 12],
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
        print(snapshot_output)
        print(expected_snapshot_output)
        assert_frame_equal(snapshot_output.reset_index(drop=True), expected_snapshot_output), "Snapshot output is not as expected"
        assert_frame_equal(construction_output.reset_index(drop=True), expected_construction_output), "Construction output is not as expected"


class TestRemoveShortToLong0:
    """Test for remove_short_to_long_0()."""

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
        """Test for remove_short_to_long_0()."""
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


class TestPrepNewRows:
    """Test for prep_new_rows()."""

    def test_prep_new_rows(self):
        # Create test data
        rows_to_add = pd.DataFrame({
            'reference': ['A', 'B', 'C'],
            'formtype': [np.nan, '0001', np.nan],
            'cellnumber': ['123', np.nan, '456']
        })
        updated_snapshot_df = pd.DataFrame({
            'reference': ['A', 'B', 'C'],
            'formtype': ['0006', '0001', '0006'],
            'cellnumber': ['789', '987', '654']
        })

        # Expected output
        expected_rows_to_add = pd.DataFrame({
            'reference': ['A', 'B', 'C'],
            'formtype': ['0006', '0001', '0006'],
            'cellnumber': ['123', '987', '456']
        })

        # Run the function
        output_rows_to_add = prep_new_rows(rows_to_add, updated_snapshot_df)

        # Check the output
        pd.testing.assert_frame_equal(output_rows_to_add, expected_rows_to_add), "Output is not as expected"

