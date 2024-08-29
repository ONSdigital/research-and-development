"""Tests for MoR.py."""

# Local Imports
import os

# Third Party Imports
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

# Local Imports
from src.imputation.MoR import run_mor
from src.imputation.imputation_helpers import get_imputation_cols

# pytestmark = pytest.mark.runwip

class TestRunMoR(object):
    """Tests for run_mor."""

    @pytest.fixture(scope="function")
    def input_lf_mor_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing MoR imputation."""
        fpath = os.path.join("tests/data/imputation/lf_mor_input_anon.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        return df

    @pytest.fixture(scope="function")
    def dummy_mor_backdata(self) -> pd.DataFrame:
        """Dummy backdata used for testing MoR imputation."""
        fpath = os.path.join("tests/data/imputation/lf_mor_backdata_anon.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        return df

    @pytest.fixture(scope="function")
    def expected_mor_output(self) -> pd.DataFrame:
        """The expected output from run_mor."""
        fpath = os.path.join("tests/data/imputation/lf_mor_expected.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        # order by reference and then instance
        df = df.sort_values(["reference", "instance"]).reset_index(drop=True)
        return df

    def test_run_mor(
            self,
           input_lf_mor_df,
            dummy_mor_backdata,
            expected_mor_output,
            imputation_config
        ):
        """General tests for run_mor."""
        impute_vars = get_imputation_cols(imputation_config)
        result_df, qa = run_mor(
            df=input_lf_mor_df,
            backdata=dummy_mor_backdata,
            impute_vars=impute_vars,
            config=imputation_config
        )
        # select only the required columns for the result and the expected output
        wanted_cols = ["reference", "instance", "imp_marker", "imp_class", "211_link", "211_imputed", "emp_researcher_imputed", "emp_technician_imputed", "212_imputed", "214_imputed", "216_imputed"]

        result_filter = (result_df.instance != 0) & (result_df.formtype == "0001") & (result_df["200"].notnull())
        result_df = result_df.loc[result_filter][wanted_cols]
        result_df = result_df.sort_values(["reference", "instance"]).reset_index(drop=True)

        expected_mor_output = expected_mor_output[wanted_cols]

        merged_df = pd.merge(result_df, expected_mor_output, on=["reference", "instance"], suffixes=("_result", "_expected"))
        merged_df["211_diffs"] = (merged_df["211_imputed_result"] - merged_df["211_imputed_expected"]).round(2)
        merged_df["big_diff"] = merged_df["211_diffs"].abs() > 0.01

        assert_frame_equal(result_df, expected_mor_output), (
            "run_mor() not imputing data as expected."
        )

    def test_run_mor_raises(self):
        """Tests for raises within run_mor."""
        # NOTE: I am not sure if you are going to individually test each function
        #       from imputation.MoR. This test function would pick up the exception
        #       raised in get_threshold_value if you were NOT going to.
        pass
