"""Tests for outputs_helpers.py."""
# Standard Library Imports
import pytest

# Third Party Imports
import pandas as pd

# Local Imports
from src.outputs.outputs_helpers import (
    create_output_df,
    regions,
    aggregate_output,
    create_period_year
)

class TestCreateOutputDF(object):
    """Tests for create_output_df."""


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

    
