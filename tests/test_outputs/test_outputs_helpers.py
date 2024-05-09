"""Tests for outputs_helpers.py."""
# Standard Library Imports
import pytest

# Third Party Imports
import pandas as pd
import numpy as np

# Local Imports
from src.outputs.outputs_helpers import (
    create_output_df,
    regions,
    create_period_year
)

class TestCreateOutputDF(object):
    """Tests for create_output_df."""

    def create_output_df_data(self, case: str = "i") -> pd.DataFrame:
        """Input dataframe for create_output_df tests.

        Args:
            case (str, optional):
                Whether or not to return the input or output data. Options 
                include 'i' and 'o'. Defaults to 'i'.
        
        Returns:
            pd.DataFrame: The dataframe containing the test data.

        Raises:
            TypeError: Raised if case is not of type 'str'.
            ValueError: Raised if case is not one of 'o' or 'i'.
        """
        # defences
        if not isinstance(case, str):
            raise TypeError(f"'case' expected type 'str'. Got {type(case)}")
        if case not in ["i", "o"]:
            raise ValueError(f"'case' must be one of ['i', 'o']. Got {case}")
        # create test data
        if case == "i":
            columns = ["old_col_1", "old_col_2", "not_inc"]
        else:
            columns = ["col_1", "col_2", "not_inc"]
        data = [
            [1, "1", np.nan],
            [2, "2", np.nan],
            [3, "3", np.nan],
            [4, "4", np.nan],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        if case == "o":
            df.drop("not_inc", axis=1, inplace=True)
        return df
    
    @pytest.fixture(scope="function")
    def create_output_df_schema(self):
        """Test schema for create_output_df."""
        schema = {
            "col_1": {"old_name": "old_col_1"},
            "col_2": {"old_name": "old_col_2"},
        }
        return schema

    def test_create_output_df(self, create_output_df_schema):
        """General tests for create_output_df."""
        output = create_output_df(
            self.create_output_df_data(case="i"),
            create_output_df_schema
                                  )
        assert (output.equals(self.create_output_df_data(case="o"))), (
            "create_output_df not behaving as expected."
        )


class TestRegions(object):
    """Tests for regions."""
    
    def test_regions(self):
        """General Tests for Regions"""
        output = regions()
        assert isinstance(output, dict), (
            "Output from 'regions' not of type 'dict'."
        )
        exp_keys = ["England", "Wales", "Scotland", "NI", "GB", "UK"]
        assert sorted(exp_keys) == sorted(output.keys()), (
            f"Dictionary returned by regions() expected keys {exp_keys}. Got "
            f"{list(output.keys())}."
        )


class TestCreatePeriodYear(object):
    """Tests for create_period_year."""

    @pytest.fixture(scope="function")
    def period_year_input(self) -> pd.DataFrame:
        """Test data for create_period_year."""
        df_frame = {
            "period": [202101, 202202, 202301, 202102, 202106]
        }
        df = pd.DataFrame(df_frame)
        return df
    
    def test_create_period_year(self, period_year_input):
        """General tests for create_period_year."""
        output = create_period_year(period_year_input)
        # assert correct periods have been determined
        expected_periods = [2021, 2022, 2023, 2021, 2021]
        resultant_periods = output.period_year.values
        assert sorted(list(resultant_periods)) == sorted(expected_periods), (
            "period_year column not as expected."
        )