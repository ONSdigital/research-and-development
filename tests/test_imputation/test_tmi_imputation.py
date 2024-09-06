"""Tests for tmi_imputation.py."""
"""Tests for TMI.py."""

# Local Imports
import os

# Third Party Imports
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

# Local Imports
from src.imputation.tmi_imputation import run_tmi
from src.imputation.imputation_helpers import get_imputation_cols


class TestRunTMILongForm(object):
    """Tests for run_mor."""

    @pytest.fixture(scope="function")
    def input_lf_tmi_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing TMI imputation."""
        fpath = os.path.join("tests/data/imputation/lf_tmi_input_anon.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        return df

    @pytest.fixture(scope="function")
    def expected_lf_tmi_output(self) -> pd.DataFrame:
        """The expected output from run_mor."""
        fpath = os.path.join("tests/data/imputation/lf_mor_expected.csv")
        df = pd.read_csv(fpath)
        df = df.astype({"reference": "Int64", "instance": "Int64"})
        # order by reference and then instance
        df = df.sort_values(["reference", "instance"]).reset_index(drop=True)
        return df

    def test_run_tmi_long_form(
        self,
        input_lf_tmi_df,
        expected_lf_tmi_output,
        imputation_config
    ):
        """General tests for run_tmi."""
        result_df, qa, trim_counts = run_tmi(
            input_lf_tmi_df,
            imputation_config
        )
        # select only the required columns for the result and the expected output
        wanted_cols = ["reference", "instance", "imp_class", "211", "211_imputed", "emp_researcher", "emp_researcher_imputed", "emp_technician_imputed", "212_imputed", "214_imputed", "216_imputed"]

        result_filter = (result_df.instance != 0) & (result_df.formtype == "0001") & (result_df["200"].notnull()) & (result_df.imp_marker.isin(["CF","TMI"]))
        result_df = result_df.loc[result_filter][wanted_cols].round(4)
        result_df = result_df.sort_values(["reference", "instance"]).reset_index(drop=True)

        # round the expected output to 4 decimal places
        # Apply rounding only to the floating-point columns in the expected output
        float_cols = expected_lf_tmi_output.select_dtypes(include='float').columns
        expected_lf_tmi_output[float_cols] = expected_lf_tmi_output[float_cols].round(4)
        expected_lf_tmi_output = expected_lf_tmi_output[wanted_cols]

        assert_frame_equal(result_df, expected_lf_tmi_output, check_dtype=False, check_exact=False), (
            "run_tmi() not imputing data as expected."
        )


# class TestRunTMIShortForm(object):
#     """Tests for run_mor in the short form case."""

#     @pytest.fixture(scope="function")
#     def input_sf_tmi_df(self) -> pd.DataFrame:
#         """A dummy dataframe used for testing TMI imputation."""
#         fpath = os.path.join("tests/data/imputation/sf_tmi_input_anon.csv")
#         df = pd.read_csv(fpath)
#         df = df.astype({"reference": "Int64", "instance": "Int64"})
#         return df

#     @pytest.fixture(scope="function")
#     def expected_sf_tmi_output(self) -> pd.DataFrame:
#         """The expected output from run_mor."""
#         fpath = os.path.join("tests/data/imputation/sf_mor_expected.csv")
#         df = pd.read_csv(fpath)
#         df = df.astype({"reference": "Int64", "instance": "Int64"})
#         # order by reference and then instance
#         df = df.sort_values(["reference", "instance"]).reset_index(drop=True)
#         return df

#     def test_run_tmi_short_form(
#         self,
#         input_sf_tmi_df,
#         expected_sf_tmi_output,
#         imputation_config
#         ):
#         """General tests for run_mor."""
#         impute_vars = get_imputation_cols(imputation_config)
#         result_df, qa = run_mor(
#             df=input_sf_tmi_df,
#             backdata=sf_tmi_backdata,
#             impute_vars=impute_vars,
#             config=imputation_config
#         )
#         # select only the required columns for the result and the expected output
#         wanted_cols = ["reference", "instance", "imp_class", "211_link", "211_imputed","212_imputed", "214_imputed", "216_imputed"]

#         result_filter = (result_df.instance != 0) & (result_df.formtype == "0006") & (result_df["200"].notnull()) & (result_df.imp_marker.isin(["CF","TMI"]))
#         result_df = result_df.loc[result_filter][wanted_cols].round(4)
#         result_df = result_df.sort_values(["reference", "instance"]).reset_index(drop=True)

#         # round the expected output to 4 decimal places
#         # Apply rounding only to the floating-point columns in the expected output
#         float_cols = expected_sf_tmi_output.select_dtypes(include='float').columns
#         expected_sf_tmi_output[float_cols] = expected_sf_tmi_output[float_cols].round(4)
#         expected_sf_tmi_output = expected_sf_tmi_output[wanted_cols]

#         assert_frame_equal(result_df, expected_sf_tmi_output, check_dtype=False, check_exact=False), (
#             "run_mor() not imputing data as expected."
#         )
