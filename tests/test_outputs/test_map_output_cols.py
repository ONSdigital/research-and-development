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