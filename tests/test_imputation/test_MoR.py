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

class TestRunMoRLongForm(object):
    """Tests for run_mor."""

    @pytest.fixture(scope="function")
    def input_lf_mor_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing MoR imputation."""
        fpath = os.path.join("tests/data/imputation/lf_mor_input_anon.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        return df

    @pytest.fixture(scope="function")
    def dummy_lf_mor_backdata(self) -> pd.DataFrame:
        """Dummy backdata used for testing MoR imputation."""
        fpath = os.path.join("tests/data/imputation/lf_mor_backdata_anon.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        return df

    @pytest.fixture(scope="function")
    def expected_lf_mor_output(self) -> pd.DataFrame:
        """The expected output from run_mor."""
        fpath = os.path.join("tests/data/imputation/lf_mor_expected.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        # order by reference and then instance
        df = df.sort_values(["reference", "instance"]).reset_index(drop=True)
        return df

    def test_run_mor_long_form(
        self,
        input_lf_mor_df,
        dummy_lf_mor_backdata,
        expected_lf_mor_output,
        imputation_config
    ):
        """General tests for run_mor."""
        impute_vars = get_imputation_cols(imputation_config)
        result_df, qa = run_mor(
            df=input_lf_mor_df,
            backdata=dummy_lf_mor_backdata,
            impute_vars=impute_vars,
            config=imputation_config
        )
        # select only the required columns for the result and the expected output
        wanted_cols = ["reference", "instance", "imp_class", "211_link", "211_imputed", "emp_researcher_imputed", "emp_technician_imputed", "212_imputed", "214_imputed", "216_imputed"]

        result_filter = (result_df.instance != 0) & (result_df.formtype == "0001") & (result_df["200"].notnull()) & (result_df.imp_marker.isin(["CF","MoR"]))
        result_df = result_df.loc[result_filter][wanted_cols].round(4)
        result_df["211_link"] = result_df["211_link"].fillna(1)
        result_df = result_df.sort_values(["reference", "instance"]).reset_index(drop=True)

        # round the expected output to 4 decimal places
        # Apply rounding only to the floating-point columns in the expected output
        float_cols = expected_lf_mor_output.select_dtypes(include='float').columns
        expected_lf_mor_output[float_cols] = expected_lf_mor_output[float_cols].round(4)
        expected_lf_mor_output = expected_lf_mor_output[wanted_cols]

        assert_frame_equal(result_df, expected_lf_mor_output, check_dtype=False, check_exact=False), (
            "run_mor() not imputing data as expected."
        )


class TestRunMoRShortForm(object):
    """Tests for run_mor in the short form case."""

    @pytest.fixture(scope="function")
    def input_sf_mor_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing MoR imputation."""
        fpath = os.path.join("tests/data/imputation/sf_mor_input_anon.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        return df

    @pytest.fixture(scope="function")
    def sf_mor_backdata(self) -> pd.DataFrame:
        """Dummy backdata used for testing MoR imputation."""
        fpath = os.path.join("tests/data/imputation/sf_mor_backdata_anon.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        return df

    @pytest.fixture(scope="function")
    def expected_sf_mor_output(self) -> pd.DataFrame:
        """The expected output from run_mor."""
        fpath = os.path.join("tests/data/imputation/sf_mor_expected.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        # order by reference and then instance
        df = df.sort_values(["reference", "instance"]).reset_index(drop=True)
        return df

    def test_run_mor_short_form(
        self,
        input_sf_mor_df,
        sf_mor_backdata,
        expected_sf_mor_output,
        imputation_config
        ):
        """General tests for run_mor."""
        impute_vars = get_imputation_cols(imputation_config)
        result_df, qa = run_mor(
            df=input_sf_mor_df,
            backdata=sf_mor_backdata,
            impute_vars=impute_vars,
            config=imputation_config
        )
        # select only the required columns for the result and the expected output
        wanted_cols = ["reference", "instance", "imp_class", "211_link", "211_imputed","212_imputed", "214_imputed", "216_imputed"]

        result_filter = (result_df.instance != 0) & (result_df.formtype == "0006") & (result_df["200"].notnull()) & (result_df.imp_marker.isin(["CF","MoR"]))
        result_df = result_df.loc[result_filter][wanted_cols].round(4)
        result_df = result_df.sort_values(["reference", "instance"]).reset_index(drop=True)

        # round the expected output to 4 decimal places
        # Apply rounding only to the floating-point columns in the expected output
        float_cols = expected_sf_mor_output.select_dtypes(include='float').columns
        expected_sf_mor_output[float_cols] = expected_sf_mor_output[float_cols].round(4)
        expected_sf_mor_output = expected_sf_mor_output[wanted_cols]

        assert_frame_equal(result_df, expected_sf_mor_output, check_dtype=False, check_exact=False), (
            "run_mor() not imputing data as expected."
        )
