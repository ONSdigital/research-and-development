import numpy as np
import pandas as pd
import pytest

from src.mapping.itl_mapping import join_itl_regions


class TestJoinITLRegions(object):
    """Tests for join_itl_regions."""

    @pytest.fixture(scope="function")
    def input_data(self):
        """Input data for join_itl_regions tests."""
        columns = ["col1", "formtype", "postcodes_harmonised"]
        data = [
            [1, "0006", "NP44 2NZ"],
            [2, "0006", "CF10 1XL"],
            [3, "0001", "NP44 2NY"],
            [4, "0001", "NP44 3YA"],
            [5, "4", "BT93 8AD"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def postcode_mapper(self):
        """Dummy postcode mapper for join_itl_regions tests."""
        columns = ["pcd2", "itl"]
        data = [
            ["NP44 2NZ", "itl1"],
            ["CF10 1XL", "itl2"],
            ["NP44 2NY", "itl3"],
            ["NP44 3YA", np.nan],  # nan
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """Expected output for join_itl_regions tests."""
        columns = ["col1", "formtype", "postcodes_harmonised", "itl"]
        data = [
            [1, "0006", "NP44 2NZ", "itl1"],
            [2, "0006", "CF10 1XL", "itl2"],
            [3, "0001", "NP44 2NY", "itl3"],
            [4, "0001", "NP44 3YA", np.nan],
            [5, "4", "BT93 8AD", "N92000002"],  # NI default ITL
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def test_join_itl_regions_defence(self):
        """Defensive tests for join_itl_regions."""
        # empty df to raise an error
        test_df = pd.DataFrame()
        error_msg = "An error occurred while combining df and postcode_mapper.*"
        with pytest.raises(ValueError, match=error_msg):
            join_itl_regions(test_df, test_df.copy())

    def test_join_itl_regions(self, input_data, postcode_mapper, expected_output):
        """General tests for join_itl_regions."""
        output = join_itl_regions(input_data, postcode_mapper)
        assert output.equals(
            expected_output
        ), "join_itl_regions not behaving as expected."
