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
    aggregate_output,
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
            raise ValueError(f"'case' must be one of ['i', 'e']. Got {case}")
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


class TestAggregateOutput(object):
    """Tests for aggregate_output."""

    @pytest.fixture(scope="function")
    def aggregate_output_input(self):
        """Input data for aggregate_output tests."""
        columns = ["key_col_1", "key_col_2", "value_col"]
        data = [
            [1, 1, 1.25],
            [1, 2, 1.3],
            [1, 2, 4.3],
            [2, 1, 1.0],
            [2, 1, 9.1],
            [3, 1, 12.0],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df
    
    @pytest.fixture(scope="function")
    def aggregate_output_output_sum(self):
        """Expected output data from aggregate_output."""
        columns = ["key_col_1", "key_col_2", "value_col"]
        data = [
            [1, 1, 1.25],
            [1, 2, 5.6],
            [2, 1, 10.1],
            [3, 1, 12.0],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df
    
    @pytest.fixture(scope="function")
    def aggregate_output_output_first(self):
        """Expected output data from aggregate_output."""
        columns = ["key_col_1", "key_col_2", "value_col"]
        data = [
            [1, 1, 1.25],
            [1, 2, 1.3],
            [2, 1, 1.0],
            [3, 1, 12.0],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df
    
    @pytest.mark.parametrize(
            "case", [
                ("sum"),
                ("first")
            ]
            )
    def test_aggregate_output_sum(
            self,
            aggregate_output_input,
            aggregate_output_output_sum,
            aggregate_output_output_first,
            case
            ):
        """Tests for aggregate_output using sum as the aggregation method."""
        # get output
        output = aggregate_output(
            df=aggregate_output_input,
            key_cols=["key_col_1", "key_col_2"],
            value_cols=["value_col"],
            agg_method=case
            )
        # determine expected output
        if case == "sum":
            expected = aggregate_output_output_sum
        elif case == "first":
            expected = aggregate_output_output_first
        else:
            raise ValueError("'case' must be one of ['sum', 'first']")
        # assert output is correct
        assert output.equals(expected), (
            "aggregate_output not behaving as expected."
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