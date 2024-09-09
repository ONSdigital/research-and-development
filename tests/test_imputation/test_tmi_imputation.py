"""Tests for tmi_imputation.py."""

# Local Imports
import os

# Third Party Imports
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

# Local Imports
from src.imputation.tmi_imputation import run_tmi

# This indicates that the tests are a work in progress and should not be run
pytestmark = pytest.mark.runwip

class TestRunTmi(object):
    """Tests for run_tmi."""

    @pytest.fixture(scope="function")
    def dummy_tmi_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing tmi imputation."""
        fpath = os.path.join("tests/data/{YOUR FILENAME HERE}.csv")
        df = pd.read_csv(fpath)
        return df

    @pytest.fixture(scope="function")
    def expected_tmi_output(self) -> pd.DataFrame:
        """The expected output from run_tmi."""
        fpath = os.path.join("tests/data/{YOUR FILENAME HERE}.csv")
        df = pd.read_csv(fpath)
        return df

    def test_run_tmi(
            self,
            dummy_tmi_df,
            expected_tmi_output,
            imputation_config
        ):
        """General tests for run_tmi."""
        imputed, qa = run_tmi(
            df=dummy_tmi_df,
            config=imputation_config
        )
        assert_frame_equal(imputed, expected_tmi_output), (
            "run_tmi() not imputing data as expected."
        )
