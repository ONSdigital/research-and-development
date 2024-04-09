"""Tests for map_output_cols.py"""

# Local Standard Library Imports
import pytest

# Third Party Imports
import pandas as pd
import numpy as np

# Local Imports
from src.outputs.map_output_cols import (
    join_pg_numeric,
    join_fgn_ownership,
    map_sizebands,
    create_cora_status_col,
    join_itl_regions,
    map_to_numeric,
)


class TestJoinPgNumeric(object):
    """Tests for join_pg_numeric."""
    @pytest.fixture(scope="function")
    def main_input(self):
        """main_df input data."""
        columns = ["test_col", "201"]
        data = [
            [0, "AA"],
            [1, "I"],
            [2, "AA"],
            [3, "G"],
            [4, "C"],
            [5, np.nan]
                ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    @pytest.fixture(scope="function")
    def mapper_input(self):
        """mapper_df input data."""
        columns = ["pg_alpha", "pg_numeric"]
        data = [
            ["AA", 40],
            ["I", 14],
            ["G", 12],
            ["C", 3],
                ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    @pytest.fixture(scope="function")
    def exp_output(self):
        """expected output data."""
        columns = ["test_col", "201", "201_numeric"]
        data = [
            [0, "AA", 40],
            [1, "I", 14],
            [2, "AA", 40],
            [3, "G", 12],
            [4, "C", 3],
            [5, np.nan, np.nan]
                ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    def test_join_pg_numeric(
            self,
            main_input,
            mapper_input,
            exp_output
            ):
        """General tests for join_pg_numeric"""
        output = join_pg_numeric(main_input, mapper_input, ["201"])
        assert output.equals(exp_output), (
            "Output from join_pg_numeric not as expected."
        )


class TestJoinFgnOwnership(object):
    """Tests for join_fgn_ownership."""
    @pytest.fixture(scope="function")
    def main_input(self):
        """main_df input data."""
        columns = ["test_col", "formtype", "reference", "ultfoc"]
        data = [
            [1, "0006", 21, "filler"],
            [2, "0001", 22, "filler"],
            [3, "0001", 23, "filler"],
            [4, np.nan, 24, np.nan], # null
            [5, "0002", 25, "uf4"] # non-accepeted formtype
                ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    @pytest.fixture(scope="function")
    def mapper_input(self):
        """mapper_df input data."""
        columns = ["ruref", "ultfoc"]
        data = [
            [21, "uf1"],
            [22, "uf2"],
            [23, "uf3"]
                ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    @pytest.fixture(scope="function")
    def exp_output(self):
        """expected output data."""
        columns = ["test_col", "formtype", "reference", "ultfoc"]
        data = [
            [1, "0006", 21, "uf1"],
            [2, "0001", 22, "uf2"],
            [3, "0001", 23, "uf3"],
            [4, np.nan, 24, "GB"], # filled with GB
            [5, "0002", 25, "uf4"] # filled with GB
                ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    def test_join_fgn_ownership(
            self,
            main_input,
            mapper_input,
            exp_output
            ):
        """General tests for join_pg_numeric"""
        output = join_fgn_ownership(main_input, mapper_input)
        assert output.equals(exp_output), (
            "Output from join_fgn_ownership not as expected."
        )


class TestMapSizebands(object):
    """Tests for map_sizebands."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """input data for map_sizebands tests."""
        columns = ["col1", "employment"]
        data = [
            [1, 5],
            [2, 15],
            [3, 25],
            [4, 55],
            [5, 150],
            [6, 300]
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df
    

    @pytest.fixture(scope="function")
    def expected_output(self):
        """expected output data for map_sizebands tests."""
        columns = ["col1", "employment", "sizeband"]
        data = [
            [1, 5, 1],
            [2, 15, 2],
            [3, 25, 3],
            [4, 55, 4],
            [5, 150, 5],
            [6, 300, 6]
        ]
        df = pd.DataFrame(data=data, columns=columns)
        df["sizeband"] = df["sizeband"].astype("Int64")
        return df
    

    def test_map_sizebands(self, input_data, expected_output):
        """General tests for map_sizebands."""
        output = map_sizebands(input_data)
        assert output.equals(expected_output), (
            "map_sizebands not working as expected."
        )


class TestCreateCoraStatusCol(object):
    """Tests for create_cora_status_col."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """Input data for create_cora_status_col tests."""
        columns = ["col1", "form_status", "statusencoded"]
        data = [
            [1, np.nan, "100"],
            [2, "200", np.nan], # default, no encoded
            [3, np.nan, np.nan], # both nan
            [4, "600", "302"], # both, but encoded points to different
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
            [2, "200", np.nan], # default, no encoded
            [3, np.nan, np.nan], # both nan
            [4, "600", "302"], # both, but encoded points to different
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
            [2, np.nan, np.nan], # default, no encoded
            [3, np.nan, np.nan], # both nan
            [4, "302", "1200"], # both, but encoded points to different
            [5, "102", "1000"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df
    

    def test_create_cora_status_col_fs(self, input_data, exp_out, exp_out_no_fs):
        """General tests for create_core_status_col with form_status."""
        # Test with form_status already created
        output = create_cora_status_col(input_data)
        assert output.equals(exp_out), (
            "create_cora_status_col not acting as expected."
        )
        # Test that the function can create form_status and given a different
        # main col
        no_fs = input_data.copy().drop("form_status", axis=1)
        no_fs = no_fs.rename(columns={"statusencoded": "stat_enc"})
        no_fs_out = create_cora_status_col(no_fs, main_col="stat_enc")
        assert ("form_status" in no_fs_out), (
            "form_status column not created"
        )
        assert no_fs_out.equals(exp_out_no_fs), (
            "create_cora_status_col not working with no form_status col and "
            "specified main_col."
        )
        