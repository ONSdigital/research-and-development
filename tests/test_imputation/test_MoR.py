"""Tests for MoR.py."""

# Local Imports
import os

# Third Party Imports
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

# Local Imports
from src.imputation.MoR import run_mor


class TestRunMoR(object):
    """Tests for run_mor."""

    @pytest.fixture(scope="function")
    def dummy_mor_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing MoR imputation."""
        fpath = os.path.join("tests/data/{YOUR FILENAME HERE}.csv")
        df = pd.read_csv(fpath)
        return df
    
    @pytest.fixture(scope="function")
    def dummy_mor_backdata(self) -> pd.DataFrame:
        """Dummy backdata used for testing MoR imputation."""
        fpath = os.path.join("tests/data/{YOUR FILENAME HERE}.csv")
        df = pd.read_csv(fpath)
        return df

    @pytest.fixture(scope="function")
    def expected_mor_output(self) -> pd.DataFrame:
        """The expected output from run_mor."""
        fpath = os.path.join("tests/data/{YOUR FILENAME HERE}.csv")
        df = pd.read_csv(fpath)
        return df

    def test_run_mor(
            self, 
            dummy_mor_df, 
            dummy_mor_backdata, 
            expected_mor_output
        ):
        """General tests for run_mor."""
        impute_vars = []
        imputed, qa = run_mor(
            df=dummy_mor_df, 
            backdata=dummy_mor_backdata, 
            impute_vars=impute_vars, 
            config=dummy_config
        )
        assert_frame_equal(imputed, expected_mor_output), (
            "run_mor() not imputing data as expected."
        )        

    def test_run_mor_raises(self):
        """Tests for raises within run_mor."""
        # NOTE: I am not sure if you are going to individually test each function
        #       from imputation.MoR. This test function would pick up the exception
        #       raised in get_threshold_value if you were NOT going to.
        pass