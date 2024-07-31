"""Tests for map_output_cols.py"""

# Local Standard Library Imports
import pytest

# Third Party Imports
import pandas as pd
import numpy as np

# Local Imports
from src.outputs.map_output_cols import (
    map_sizebands,
    create_cora_status_col,
    map_to_numeric,
)


class TestMapSizebands(object):
    """Tests for map_sizebands."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """input data for map_sizebands tests."""
        columns = ["col1", "employment"]
        data = [[1, 5], [2, 10], [3, 20], [4, 55], [5, 150], [6, 300]]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """expected output data for map_sizebands tests."""
        columns = ["col1", "employment", "sizeband"]
        data = [[1, 5, 1], [2, 10, 2], [3, 20, 3], [4, 55, 4], [5, 150, 5], [6, 300, 6]]
        df = pd.DataFrame(data=data, columns=columns)
        df["sizeband"] = df["sizeband"].astype("Int64")
        return df

    def test_map_sizebands(self, input_data, expected_output):
        """General tests for map_sizebands."""
        output = map_sizebands(input_data)
        assert output.equals(expected_output), "map_sizebands not working as expected."


class TestCreateCoraStatusCol(object):
    """Tests for create_cora_status_col."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """Input data for create_cora_status_col tests."""
        columns = ["col1", "form_status", "statusencoded"]
        data = [
            [1, np.nan, "100"],
            [2, "200", np.nan],  # default, no encoded
            [3, np.nan, np.nan],  # both nan
            [4, "600", "302"],  # both, but encoded points to different
            [5, np.nan, "102"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    @pytest.fixture(scope="function")
    def exp_out(self):
        """Expected output for create_cora_status_col tests."""
        columns = ["col1", "form_status", "statusencoded"]
        data = [
            [1, "200", "100"],
            [2, "200", np.nan],  # default, no encoded
            [3, np.nan, np.nan],  # both nan
            [4, "600", "302"],  # both, but encoded points to different
            [5, "1000", "102"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    @pytest.fixture(scope="function")
    def exp_out_no_fs(self):
        """Expected output for create_cora_status_col tests."""
        columns = ["col1", "stat_enc", "form_status"]
        data = [
            [1, "100", "200"],
            [2, np.nan, np.nan],  # default, no encoded
            [3, np.nan, np.nan],  # both nan
            [4, "302", "1200"],  # both, but encoded points to different
            [5, "102", "1000"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_create_cora_status_col_fs(self, input_data, exp_out, exp_out_no_fs):
        """General tests for create_core_status_col with form_status."""
        # Test with form_status already created
        output = create_cora_status_col(input_data)
        assert output.equals(exp_out), "create_cora_status_col not acting as expected."
        # Test that the function can create form_status and given a different
        # main col
        no_fs = input_data.copy().drop("form_status", axis=1)
        no_fs = no_fs.rename(columns={"statusencoded": "stat_enc"})
        no_fs_out = create_cora_status_col(no_fs, main_col="stat_enc")
        assert "form_status" in no_fs_out, "form_status column not created"
        assert no_fs_out.equals(exp_out_no_fs), (
            "create_cora_status_col not working with no form_status col and "
            "specified main_col."
        )


class TestMapToNumeric(object):
    """Tests for map_to_numeric."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """Input data for map_to_numeric tests."""
        columns = ["col1", "713", "714"]
        data = [[1, "Yes", "Yes"], [2, "No", "No"], [3, "", ""], [4, np.nan, np.nan]]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """Expected output for map_to_numeric tests."""
        columns = ["col1", "713", "714"]
        data = [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 3, 3]]
        df = pd.DataFrame(columns=columns, data=data)
        type_dict = {"713": "Int64", "714": "Int64"}
        df = df.astype(type_dict)
        return df

    def test_map_to_numeric(self, input_data, expected_output):
        """General tests for map_to_numeric."""
        output = map_to_numeric(input_data)
        assert output.equals(
            expected_output
        ), "map_to_numeric not behaving as expected."


class TestMapFGColsToNumeric(object):
    """Tests for map_fg_cols_to_numeric."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """Input data for map_fg_cols_to_numeric tests."""
        columns = ["col1", "FG_col"]
        data = [[1, "Yes"], [2, "No"], [3, ""], [4, np.nan]]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """Input data for map_fg_cols_to_numeric tests."""
        columns = ["col1", "FG_col"]
        data = [
            [1, 1],
            [2, 2],
            [3, 3],
            [4, 3],
        ]
        df["FG_col"] = df["FG_col"].astype("Int64")
        df = pd.DataFrame(columns=columns, data=data)
        return df
