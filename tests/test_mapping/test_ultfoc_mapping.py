import pytest
import pandas as pd
import numpy as np

# Local Imports
from src.mapping.ultfoc_mapping import join_fgn_ownership, validate_ultfoc_mapper


class TestJoinFgnOwnership(object):
    """Tests for join_fgn_ownership."""

    @pytest.fixture(scope="function")
    def main_input(self):
        """main_df input data."""
        columns = ["test_col", "formtype", "reference"]
        data = [
            [1, "0006", 21],
            [2, "0001", 22],
            [3, "0001", 23],
            [4, np.nan, 24],
            [5, "0001", 25],
            [6, "0001", 26],
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
            [23, "uf3"],
            [24, None],
            [25, ""],
            [26, np.nan],
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
            [4, np.nan, 24, "GB"],
            [5, "0001", 25, "GB"],
            [6, "0001", 26, "GB"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_join_fgn_ownership(self, main_input, mapper_input, exp_output):
        """General tests for join_pg_numeric"""
        output = join_fgn_ownership(main_input, mapper_input)
        assert output.equals(
            exp_output
        ), "Output from join_fgn_ownership not as expected."


class TestValidateUltfocMapper(object):
    """Tests for validate_ultfoc_mapper."""
    @pytest.fixture(scope="function")
    def ultfoc_mapper_input_fail(self):
        """ultfoc_mapper input data."""
        columns = ["ruref", "ultfoc"]
        data = [
            [21, "uf1"],
            [22, "uf2"],
            [23, "uf3"],
            [24, None],
            [25, ""],
            [26, np.nan],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    @pytest.fixture(scope="function")
    def ultfoc_mapper_input_pass(self):
        """ultfoc_mapper input data."""
        columns = ["ruref", "ultfoc"]
        data = [
            [21, "uf1"],
            [22, "uf2"],
            [23, "uf3"],
            [24, "GB"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def test_validate_ultfoc_mapper(self, ultfoc_mapper_input_fail):
        """General tests for validate_ultfoc_mapper."""
        with pytest.raises(ValueError):
            validate_ultfoc_mapper(ultfoc_mapper_input_fail)
        

    def test_validate_ultfoc_mapper_pass(self, ultfoc_mapper_input_pass):
        """General tests for validate_ultfoc_mapper."""
        result = validate_ultfoc_mapper(ultfoc_mapper_input_pass)
        assert result is None

    


