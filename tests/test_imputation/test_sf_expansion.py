"""Test for sf_expansion.py."""

# Local Imports
import os

# Third Party Imports
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

# Local Imports
from src.imputation.sf_expansion import run_sf_expansion


class TestRunTmi(object):
    """Tests for run_sf_expansion."""

    @pytest.fixture(scope="function")
    def dummy_sf_expansion_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing sf expansion."""
        fpath = os.path.join("tests/data/{YOUR FILENAME HERE}.csv")
        df = pd.read_csv(fpath)
        return df

    @pytest.fixture(scope="function")
    def expected_sf_expansion_output(self) -> pd.DataFrame:
        """The expected output from run_sf_expansion."""
        fpath = os.path.join("tests/data/{YOUR FILENAME HERE}.csv")
        df = pd.read_csv(fpath)
        return df

    def test_run_sf_expansion(
            self, 
            dummy_sf_expansion_df, 
            expected_sf_expansion_output,
            imputation_config
        ):
        """General tests for run_sf_expansion."""
        imputed = run_sf_expansion(
            df=dummy_sf_expansion_df, 
            config=imputation_config
        )
        assert_frame_equal(imputed, expected_sf_expansion_output), (
            "run_sf_expansion() not imputing data as expected."
        )        
