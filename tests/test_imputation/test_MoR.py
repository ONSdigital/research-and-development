"""Tests for MoR.py."""

# Third Party Imports
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal

# Local Imports
from src.imputation.MoR import (
    is_lf_only,
    mor_preprocessing,
    calculate_growth_rates,
    group_calc_link,
    calculate_links
)

# pytestmark = pytest.mark.runwip

class TestIsLfOnly(object):
    """Tests for is_lf_only."""
    def test_pnp_survey(self):
        config = {
            "survey": {
                "survey_type": "PNP",
                "survey_year": 2021
            }
        }
        assert is_lf_only(config)

    def test_berd_2021_backdata(self):
        config = {
            "survey": {
                "survey_type": "BERD",
                "survey_year": 2022
            }
        }
        assert is_lf_only(config)

    def test_neither_condition(self):
        config = {
            "survey": {
                "survey_type": "BERD",
                "survey_year": 2021
            }
        }
        assert not is_lf_only(config)


class TestMoRPreprocessing(object):
    """Tests for MoR preprocessing function."""

    def create_input_df(self) -> pd.DataFrame:
        input_columns = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
            "601",
            "status",
            "imp_marker",
            "imp_class"
        ]
        data = [
            [1001, 1, "C", "6.0", "CF14 9XY", "Clear", "R", "C_AA"],
            [1001, 2, "P", "6", "CF14 9XY", "Clear", "R", "P_AA"],
            [1002, 1, "C", "0006", np.nan, "Form sent out", "CF", "C_nan"],
            [1003, 1, "C", "0001", "NP10 2RT", "Clear", "MoR", "C_AB"],
            [1004, 1, "C", "6", np.nan, "Form sent out", "MoR", "C_nan"],
            [1004, 2, "P", "6.0", np.nan, "Form sent out", "MoR", "P_AB"],
            [1005, 0, "C", "0001", "SW5 2DW", "Check needed", "MoR", "C_CD"],
            [1005, 1, "P", "01.0", "SW5 2DW", "Check needed", "MoR", "P_CD"],
            [1006, 0, np.nan, "6", "CF48 9DU", "Clear - overidden", np.nan, np.nan],
            [1006, 1, "C", "6.0", "CF48 9DU", "Clear", "R", "C_AE"],
            [1006, 2, "P", "6.0", "CF48 9DU", "Clear", "R", "P_AE"],
        ]
        input_df = pd.DataFrame(data=data, columns=input_columns)
        return input_df

    def config_dict(self) -> dict:
        """A dummy config for testing."""
        config = {
            "survey": {
                "survey_type": "BERD",
                "survey_year": 2023
            }
        }
        return config

    def create_backdata(self) -> pd.DataFrame:
        """A dummy backdata for testing."""
        backdata_columns = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
            "601",
            "status",
            "imp_marker",
            "imp_class",
        ]

        data = [
            [1007, 1, "C", "0006", "SW52DW", "Clear", "R", "C_AB"],
            [1007, 2, "P", "6", "SW52DW", "Clear", "R", np.nan],
            [1008, 1, "C", "0006", np.nan, "Form sent out", np.nan, "C_nan"],
            [1008, 2, "P", "1.0", "NP10 2RT", "Clear", np.nan, "P_nan"],
            [1009, 1, "C", "6.0", np.nan, "Form sent out", "no_imputation", "C_nan"],
            [1010, 0, np.nan, "0006", "NP10 6RT", "Form sent out", "no_imputation", np.nan],
            [1010, 1, "C", "1", np.nan, "Check needed", np.nan, "C_nan"],
            [1010, 2, "P", "0001", np.nan, "Check needed", np.nan, "P_nan"],
            [1011, 1, "C", "6", np.nan, "Clear - overidden", "R", "C_nan"],
            [1012, 0, np.nan, "0006", "CF489DU", "Clear", np.nan, np.nan],
            [1012, 1, "C", "0006", "CF489DU", "Clear", np.nan, "C_nan"],
        ]

        backdata_df = pd.DataFrame(data=data, columns=backdata_columns)
        return backdata_df

    def expected_to_impute_df(self) -> pd.DataFrame:
        """The expected to_impute_df output from the preprocessing function."""
        expected_columns = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
            "601",
            "status",
            "imp_marker",
            "imp_class"
        ]

        data = [
            [1002, 1, "C", "0006", np.nan, "Form sent out", "CF", "C_nan"],
            [1004, 1, "C", "0006", np.nan, "Form sent out", "MoR", "C_nan"],
            [1005, 0, "C", "0001", "SW5 2DW", "Check needed", "MoR", "C_CD"]
        ]

        expected_to_impute_df = pd.DataFrame(data=data, columns=expected_columns)
        return expected_to_impute_df

    def expected_remainder_df(self) -> pd.DataFrame:
        """The expected remainder_df output from the preprocessing function."""
        expected_columns = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
            "601",
            "status",
            "imp_marker",
            "imp_class"
            ]

        data = [
            [1001, 1, "C", "0006", "CF14 9XY", "Clear", "R", "C_AA"],
            [1001, 2, "P", "0006", "CF14 9XY", "Clear", "R", "P_AA"],
            [1003, 1, "C", "0001", "NP10 2RT", "Clear", "MoR", "C_AB"],
            [1004, 2, "P", "0006", np.nan, "Form sent out", "MoR", "P_AB"],
            [1005, 1, "P", np.nan, "SW5 2DW", "Check needed", "MoR", "P_CD"],
            [1006, 0, np.nan, "0006", "CF48 9DU", "Clear - overidden", np.nan, np.nan],
            [1006, 1, "C", "0006", "CF48 9DU", "Clear", "R", "C_AE"],
            [1006, 2, "P", "0006", "CF48 9DU", "Clear", "R", "P_AE"]
        ]


        expected_remainder_df = pd.DataFrame(data=data, columns=expected_columns)
        return expected_remainder_df

    def expected_backdata_df(self) -> pd.DataFrame:
        """The expected backdata_df output from the preprocessing function."""
        expected_columns = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
            "601",
            "status",
            "imp_marker",
            "imp_class",
        ]

        data = [
            [1007, 1, "C", "0006", "SW5  2DW", "Clear", "R", "C_AB"],
            [1007, 2, "P", "0006", "SW5  2DW", "Clear", "R", "nan"],
            [1011, 1, "C", "0006", np.nan, "Clear - overidden", "R", "C_nan"],
        ]

        expected_backdata_df = pd.DataFrame(data=data, columns=expected_columns)
        return expected_backdata_df

    def test_mor_preprocessing(self):
        """Tests for the MoR preprocessing function."""
        input_df = self.create_input_df()
        config = self.config_dict()
        backdata_df = self.create_backdata()
        to_impute_df, remainder_df, backdata_df = mor_preprocessing(
            input_df, backdata_df, config
        )

        expected_to_impute_df = self.expected_to_impute_df()
        expected_remainder_df = self.expected_remainder_df()
        expected_backdata_df = self.expected_backdata_df()

        # Reset index and replace missing values for comparison
        df_list = [
            to_impute_df, expected_to_impute_df, remainder_df,
            expected_remainder_df, backdata_df, expected_backdata_df
        ]
        for df in df_list:
            df.reset_index(drop=True, inplace=True)

        assert_frame_equal(to_impute_df, expected_to_impute_df, check_dtype=False, check_exact=False, atol=1e-8, rtol=1e-5)
        assert_frame_equal(remainder_df, expected_remainder_df, check_dtype=False, check_exact=False, atol=1e-8, rtol=1e-5)
        assert_frame_equal(backdata_df, expected_backdata_df, check_dtype=False, check_exact=False, atol=1e-8, rtol=1e-5)

class Test_calculate_growth_rates(object):
    """Tests for calculate_growth_rates."""
    def target_vars_list(self):
        """A simple method that returns a list."""
        return ["211", "emp_researcher", "emp_technician"]


    def create_test_CGR_current_df(self):
        """Create an test_CGR_current dataframe for the test."""
        test_CGR_current_columns = [
        "reference",
        "instance",
        "211",
        "emp_researcher",
        "emp_technician",
        "imp_marker",
        "imp_class",
        "selectiontype",
    ]

        data = [
        [1031, 1, 20, 10, 10.0, "R", "C_AA", "C"],
        [1031, 2, 10, 0, 10.0, "R", "D_AA", "C"],
        [1032, 1, 0, 0, 0.0, "R", "<NA>_L", "C"],
        [1033, 1, 0, 0, 0.0, "R", "C_AB", "C"],
        [1040, 1, 86000, 0, 0.0, "R", "D_AB", "C"],
        [1042, 1, 9000, 0, 0.0, "R", "D_AB", "C"],
        [1045, 1, 80500, 20, np.nan, "R", "C_AH", "C"],
        [1045, 2, 36000, 30, 10.0, "R", "D_AH", "C"],
        [1046, 1, 80500, 0, 0.0, "R", "D_AD", "C"],
        [1046, 2, 36000, 0, 0.0, "R", "C_AD", "C"],
        [1046, 3, 0, 0, 0.0, "R", "<NA>_AD", "C"],
        [1046, 4, 0, 0, 0.0, "R", "<NA>_AD", "C"],
        [1047, 1, 400, 20, np.nan, "R", "C_BC", "C"],
        [1047, 2, 200, 10, 10.0, "R", "D_BC", "C"],
    ]

        test_CGR_current_df = pd.DataFrame(data=data, columns=test_CGR_current_columns)
        return test_CGR_current_df


    def create_test_CGR_backdata_df(self):
        """Create an test_CGR_backdata dataframe for the test."""
        test_CGR_backdata_columns = [
        "reference",
        "instance",
        "211",
        "emp_researcher",
        "emp_technician",
        "imp_marker",
        "imp_class",
        "selectiontype",
    ]

        data = [
        [1031, 1, 0.0, 10.0, np.nan, "R", "C_AA", "C"],
        [1031, 2, 10.0, 10.0, 20.0, "R", "D_AA", "C"],
        [1032, 1, np.nan, 0.0, 0.0, "R", "C_L", "C"],
        [1040, 1, 87200.0, np.nan, np.nan, "R", "D_G", "C"],
        [1042, 1, 8000.0, np.nan, np.nan, "R", "D_P", "C"],
        [1045, 1, 10000.0, 0.0, 10.0, "R", "C_AH", "C"],
        [1045, 2, 10000.0, 10.0, 10.0, "R", "D_AH", "C"],
        [1047, 1, 400.0, 20.0, 0.0, "R", "C_BC", "C"],
        [1047, 2, 200.0, 10.0, 10.0, "R", "D_BC", "C"],
    ]

        test_CGR_backdata_df = pd.DataFrame(data=data, columns=test_CGR_backdata_columns)
        return test_CGR_backdata_df


    def create_test_CGR_expected_df(self):
        """Create an test_CGR_expected dataframe for the test."""
        test_CGR_expected_columns = [
        "reference",
        "imp_class",
        "211",
        "emp_researcher",
        "emp_technician",
        "211_prev",
        "emp_researcher_prev",
        "emp_technician_prev",
        "211_gr",
        "emp_researcher_gr",
        "emp_technician_gr",
    ]

        data = [
        [1031, "C_AA", 20, 10, 10.0, 0.0, 10.0, 0.0, np.nan, 1.0, np.nan],
        [1031, "D_AA", 10, 0, 10.0, 10.0, 10.0, 20.0, 1.0, np.nan, 0.5],
        [1045, "C_AH", 80500, 20, 0.0, 10000.0, 0.0, 10.0, 8.05, np.nan, np.nan],
        [1045, "D_AH", 36000, 30, 10.0, 10000.0, 10.0, 10.0, 3.6, 3.0, 1.0],
        [1047, "C_BC", 400, 20, 0.0, 400.0, 20.0, 0.0, 1.0, 1.0, np.nan],
        [1047, "D_BC", 200, 10, 10.0, 200.0, 10.0, 10.0, 1.0, 1.0, 1.0],
    ]

        test_CGR_expected_df = pd.DataFrame(data=data, columns=test_CGR_expected_columns)
        return test_CGR_expected_df


    def test_calculate_growth_rates(self):
        """Test the calculate_growth_rates function."""
        current_df = self.create_test_CGR_current_df()
        backdata_df = self.create_test_CGR_backdata_df()
        expected_df = self.create_test_CGR_expected_df()
        target_vars = self.target_vars_list()

        result_df = calculate_growth_rates(current_df, backdata_df, target_vars)

        assert_frame_equal(result_df, expected_df, check_dtype=False, check_exact=False), (
            "calculate_growth_rates() did not return the expected dataframe."
        )

class TestGroupCalcLink(object):
    """Tests for the group_calc_links function."""
    def create_input_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing group_calc_links function."""
        columns = [
            "reference",
            "imp_class",
            "211",
            "emp_researcher",
            "211_gr",
            "emp_researcher_gr",
        ]
        data = [
            [1031, "C_AA", 20, 10, np.nan, 1.0],
            [1031, "C_AA", 10, 0, 1.0, np.nan],
            [1045, "C_AA", 80500, 20, 8.05, np.nan],
            [1045, "C_AA", 36000, 30, 3.6, 3.0],
            [1047, "C_AA", 400, 20, 1.0, 1.0],
            [1047, "C_AA", 200, 10, 1.0, 1.0]]

        input_df = pd.DataFrame(data=data, columns=columns)
        return input_df

    def dummy_config(self) -> dict:
        """A dummy config for testing."""
        config = {"imputation": {
            "mor_threshold": 3,
            "trim_threshold": 10,
            "lower_trim_perc": 15,
            "upper_trim_perc": 15,
            "target_vars": ["211","emp_researcher"]},
        }
        return config

    def expected_output_df(self) -> pd.DataFrame:
        """Expected dataframe after running group_calc_links function.
            'group_size' is calculated by the sum of valid values in the column.
            'link' is calculated by the mean growth rate of the column.
            'trim' is specified conditions in the config."""
        columns = [
            "reference",
            "imp_class",
            "211",
            "emp_researcher",
            "211_gr",
            "emp_researcher_gr",
            "211_gr_trim",
            "211_group_size",
            "211_link",
            "emp_researcher_gr_trim",
            "emp_researcher_group_size",
            "emp_researcher_link",
        ]
        # Data has been sorted by growth rate (emp_researcher_gr) in descending order
        data = [
            [1047, "C_AA", 400, 20, 1.0, 1.0, False, 5, 2.93, False, 4, 1.5],
            [1047, "C_AA", 200, 10, 1.0, 1.0, False, 5, 2.93, False, 4, 1.5],
            [1031, "C_AA", 20, 10, np.nan, 1.0, False, 5, 2.93, False, 4, 1.5],
            [1045, "C_AA", 36000, 30, 3.6, 3.0, False, 5, 2.93, False, 4, 1.5],
            [1031, "C_AA", 10, 0, 1.0, np.nan, False, 5, 2.93, False, 4, 1.5],
            [1045, "C_AA", 80500, 20, 8.05, np.nan, False, 5, 2.93, False, 4, 1.5],
            ]

        expected_output_df = pd.DataFrame(data=data, columns=columns)
        return expected_output_df


    def test_group_calc_link(self):
        # Create the input and expected output dataframes
        input_df = self.create_input_df()
        config = self.dummy_config()
        expected_output_df = self.expected_output_df()
        target_vars = config["imputation"]["target_vars"]

        # Run the function
        result_df = group_calc_link(input_df, target_vars, config)

        # Reset index for comparison
        df_list = [expected_output_df, result_df]

        for df in df_list:
            df.reset_index(drop=True, inplace=True)

        # Compare the results
        assert_frame_equal(result_df, expected_output_df, check_dtype=False, check_exact=False), (
            "group_calc_links() not calculating links as expected."
        )
class TestCalculateLinks(object):
    """Tests to check the function is ordering the data correctly"""

    def config_dict(self):
        """Dummy config for testing."""
        config = {"imputation": {
            "mor_threshold": 3,
            "trim_threshold": 10,
            "lower_trim_perc": 15,
            "upper_trim_perc": 15,
            "target_vars": ["211", "emp_researcher", "emp_technician"]},
        }
        return config

    def create_input_df(self) -> pd.DataFrame:
        """A dummy dataframe used for testing calculate_links function."""
        columns = [
            "reference",
            "imp_class",
            "211",
            "emp_researcher",
            "emp_technician",
            "211_prev",
            "emp_researcher_prev",
            "emp_technician_prev",
            "211_gr",
            "emp_researcher_gr",
            "emp_technician_gr",
        ]

        data = [
            [1031, "C_AA", 20, 10, 10.0, 0.0, 10.0, 0.0, np.nan, 1.0, np.nan],
            [1031, "D_AA", 10, 0, 10.0, 10.0, 10.0, 20.0, 1.0, np.nan, 0.5],
            [1045, "C_AH", 80500, 20, 0.0, 10000.0, 0.0, 10.0, 8.05, np.nan, np.nan],
            [1045, "D_AH", 36000, 30, 10.0, 10000.0, 10.0, 10.0, 3.6, 3.0, 1.0],
            [1047, "C_BC", 400, 20, 0.0, 400.0, 20.0, 0.0, 1.0, 1.0, np.nan],
            [1047, "D_BC", 200, 10, 10.0, 200.0, 10.0, 10.0, 1.0, 1.0, 1.0],
        ]
        input_df = pd.DataFrame(data, columns=columns)
        return input_df

    def expected_output(self) -> pd.DataFrame:
        """Expected dataframe if 'is_current' is set to false.
       Returns filtered data of both previous and current period data"""
        columns = [
            "imp_class",
            "reference",
            "211",
            "211_prev",
            "211_group_size",
            "211_gr",
            "211_gr_trim",
            "211_link",
            "emp_researcher",
            "emp_researcher_prev",
            "emp_researcher_group_size",
            "emp_researcher_gr",
            "emp_researcher_gr_trim",
            "emp_researcher_link",
            "emp_technician",
            "emp_technician_prev",
            "emp_technician_group_size",
            "emp_technician_gr",
            "emp_technician_gr_trim",
            "emp_technician_link",
        ]

        data = [
            ["C_AA", 1031, 20, 0.0, 0, np.nan, False, 1.0, 10, 10.0, 1, 1.0, False, 1.0, 10.0, 0.0, 0, np.nan, False, 1.0],
            ["D_AA", 1031, 10, 10.0, 1, 1.0, False, 1.0, 0, 10.0, 0, np.nan, False, 1.0, 10.0, 20.0, 1, 0.5, False, 1.0],
            ["C_AH", 1045, 80500, 10000.0, 1, 8.05, False, 1.0, 20, 0.0, 0, np.nan, False, 1.0, 0.0, 10.0, 0, np.nan, False, 1.0],
            ["D_AH", 1045, 36000, 10000.0, 1, 3.6, False, 1.0, 30, 10.0, 1, 3.0, False, 1.0, 10.0, 10.0, 1, 1.0, False, 1.0],
            ["C_BC", 1047, 400, 400.0, 1, 1.0, False, 1.0, 20, 20.0, 1, 1.0, False, 1.0, 0.0, 0.0, 0, np.nan, False, 1.0],
            ["D_BC", 1047, 200, 200.0, 1, 1.0, False, 1.0, 10, 10.0, 1, 1.0, False, 1.0, 10.0, 10.0, 1, 1.0, False, 1.0],
        ]

        exp_df = pd.DataFrame(data, columns=columns)
        return exp_df

    def test_calculate_links(self):
        # Create the input and expected output dataframes
        input_df = self.create_input_df()
        exp_df = self.expected_output()
        config = self.config_dict()
        target_vars = config["imputation"]["target_vars"]

        # Run the function
        result = calculate_links(input_df, target_vars, config)

        # Sort both DataFrames by the same columns before comparison
        sort_cols = ["imp_class", "reference"]
        result = result.sort_values(by=sort_cols).reset_index(drop=True)
        exp_df = exp_df.sort_values(by=sort_cols).reset_index(drop=True)

        # Compare the results
        assert_frame_equal(result, exp_df, check_dtype=False, check_like=True)
