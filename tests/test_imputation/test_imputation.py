import numpy as np
import pandas as pd
from pandas._testing import assert_frame_equal
from pandas import DataFrame as pandasDF

from src.imputation.imputation import (
    run_imputation,
    backwards_imputation,
    forward_imputation,
    loop_unique,
    get_mean_growth_ratio,
    trim_bounds,
    trim_check,
    calc_growth_ratio,
    sort_df,
    filter_by_column_content,
    create_imp_class_col,
    filter_same_class,
    filter_pairs,
    flag_nulls_and_zeros,
)


class TestCleanData:  # usetag
    """Unit test for filter_by_column_content"""

    def input_data_filter_by_column_content(self):
        """Create input data for the filter_by_column_content function"""

        # columns for the dataframe
        input_cols = ["clean_check"]

        # data in the column order above
        input_data = [["clean"], ["not_clean"]]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_filter_by_column_content(self):
        """Create output data for the filter_by_column_content function"""

        # columns for the dataframe
        output_cols = ["clean_check"]

        # data in the column order above
        output_data = [["clean"]]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_filter_by_column_content(self):
        """Test the expected functionality"""

        df_input = self.input_data_filter_by_column_content()
        df_expout = self.output_data_filter_by_column_content()
        column = "clean_check"
        column_content = "clean"
        df_result = filter_by_column_content(
            df_input, column, column_content
        )  # add period filter functionality
        assert_frame_equal(df_result, df_expout)


class TestCreateClassCol:
    """Unit test for create_imp_class_col"""

    def input_data_create_imp_class_col(self):
        """Create input data for the create_imp_class_col function"""

        input_cols = ["200", "201"]

        input_data = [["C", "AG"]]

        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_create_imp_class_col(self):
        """Create output data for the create_imp_class_col function"""

        output_cols = ["200", "201", "class"]

        output_data = [["C", "AG", "C_AG"]]

        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_create_imp_class_col(self):
        """Test the expected functionality"""

        df_input = self.input_data_create_imp_class_col()
        df_expout = self.output_data_create_imp_class_col()

        col_first_half = "200"
        col_second_half = "201"
        class_name = "class"

        df_result = create_imp_class_col(
            df_input, col_first_half, col_second_half, class_name
        )  # add period filter functionality
        assert_frame_equal(df_result, df_expout)


class TestFilterSameClass:
    """Unit test for filter_same_class"""

    def input_data_filter_same_class(self):
        """Create input data for the filter_same_class function"""

        # columns for the dataframe
        input_cols = ["company_ref", "190012_class", "190009_class"]

        # data in the column order above
        input_data = [
            [1, "class1", "class1"],
            [10, "class1", "class2"],
            [20, "class2", "class1"],
        ]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_filter_same_class(self):
        """Create output data for the filter_same_class function"""

        # columns for the dataframe
        output_cols = ["company_ref", "190012_class", "190009_class"]

        # data in the column order above
        output_data = [[1, "class1", "class1"]]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_filter_same_class(self):
        """Test the expected functionality"""

        df_input = self.input_data_filter_same_class()
        df_expout = self.output_data_filter_same_class()

        current_period = "190012"
        previous_period = "190009"

        df_result = filter_same_class(df_input, current_period, previous_period)
        assert_frame_equal(df_result, df_expout)


class TestFilterPairs:
    """Unit test for filter_pairs"""

    def input_data_filter_pairs(self):
        """Create input data for the filter_pairs function"""

        # columns for the dataframe
        input_cols = ["company_ref", "190012_target_status", "190009_target_status"]

        # data in the column order above
        input_data = [
            [1, "Present", "Present"],
            [10, "Missing", "Present"],
            [20, "Present", "Missing"],
        ]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_filter_pairs(self):
        """Create output data for the filter_pairs function"""

        # columns for the dataframe
        output_cols = ["company_ref", "190012_target_status", "190009_target_status"]

        # data in the column order above
        output_data = [[1, "Present", "Present"]]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_filter_pairs(self):
        """Test the expected functionality"""

        df_input = self.input_data_filter_pairs()
        df_expout = self.output_data_filter_pairs()

        target_variable = "target"
        current_period = "190012"
        previous_period = "190009"

        df_result = filter_pairs(
            df_input, target_variable, current_period, previous_period
        )  # add period filter functionality
        assert_frame_equal(df_result, df_expout)


class TestFlagNullsZeros:
    """Unit tests for flag_nulls_zeros."""

    def input_data(self):
        """Create dataframe for input data."""
        input_schema = {
            "ref": "Int64",
            "curr_var1": "Int64",
            "prev_var1": "Int64",
            "curr_var2": "Int64",
            "prev_var2": "Int64",
        }

        input_data = [
            [1, 100, np.nan, 0, 201],
            [2, 100, 101, 200, 201],
            [3, np.nan, 101, 200, 201],
            [4, 100, 101, 200, 201],
            [5, 100, np.nan, 200, 201],
            [6, 100, 101, 200, 201],
            [7, 100, 101, 0, 201],
            [8, 100, 101, 200, 0],
        ]

        input_df = pandasDF(data=input_data, columns=input_schema.keys()).astype(
            input_schema
        )

        return input_df

    def output_data(self):
        """Create dataframe for output data."""
        out_schema = {
            "ref": "Int64",
            "curr_var1": "Int64",
            "prev_var1": "Int64",
            "curr_var2": "Int64",
            "prev_var2": "Int64",
            "var1_valid": "Bool",
            "var2_valid": "Bool",
        }

        output_data = [
            [1, 100, np.nan, 0, 201, False, False],
            [2, 100, 101, 200, 201, True, True],
            [3, np.nan, 101, 200, 201, False, True],
            [4, 100, 101, 200, 201, True, True],
            [5, 100, np.nan, 200, 201, False, True],
            [6, 100, 101, 200, 201, True, True],
            [7, 100, 101, 0, 201, True, False],
            [8, 100, 101, 200, 0, True, False],
        ]

        output_df = pandasDF(data=output_data, columns=out_schema.keys()).astype(
            out_schema
        )

        return output_df

    def test_flag_nulls_and_zeros(self):
        """Unit test for flag_nulls_and_zeros."""
        df_expout = self.output_data()
        input_df = self.input_data()
        df_result = flag_nulls_and_zeros(["var1", "var2"], input_df, "curr", "prev")
        assert_frame_equal(df_result, df_expout)


class TestCalcGrowthRatio:
    """Unit test for calc_growth_ratio"""

    def input_data_calc_growth_ratio(self):
        """Create input data for the calc_growth_ratio function"""

        input_cols = {
            "status": "str",
            "current_var1": "Int64",
            "previous_var1": "Int64",
            "current_var2": "Int64",
            "previous_var2": "Int64",
        }

        input_data = [
            ["Clear", 2, 8, 2, 4],
            ["Clear", 3, 6, 2, np.nan],
            ["Clear", np.nan, 8, np.nan, 4],
            ["Clear", 2, 1, 2, 4],
            ["Form sent out", 5, 3, 2, 4],
        ]

        input_df = pandasDF(data=input_data, columns=input_cols.keys()).astype(
            input_cols
        )

        return input_df

    def output_data_calc_growth_ratio(
        self,
    ):  # 'Imputed(Fwd)','Imputed(Bwd)', 'ACTUAL', 'Const(Prog)'
        """Create output data for the calc_growth_ratio function"""

        output_cols = {
            "status": "str",
            "current_var1": "Int64",
            "previous_var1": "Int64",
            "current_var2": "Int64",
            "previous_var2": "Int64",
            "var1_growth_ratio": "float",
        }

        output_data = [
            ["Clear", 2, 8, 2, 4, 0.25],
            ["Clear", 3, 6, 2, np.nan, 0.5],
            ["Clear", np.nan, 8, np.nan, 4, np.nan],
            ["Clear", 2, 1, 2, 4, 2.0],
            ["Form sent out", 5, 3, 2, 4, np.nan],
        ]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols.keys()).astype(
            output_cols
        )

        return df_expout

    def test_calc_growth_ratio(self):
        """Test the expected functionality"""

        target_variable = "var1"
        input_df = self.input_data_calc_growth_ratio()
        df_expout = self.output_data_calc_growth_ratio()
        current_period = "current"
        previous_period = "previous"

        print(input_df, "\n", df_expout)
        df_result = calc_growth_ratio(
            target_variable, input_df, current_period, previous_period
        )
        assert_frame_equal(df_result, df_expout)


class TestSortDf:
    """Unit test for sort_df"""

    def input_data_sort_df(self):
        """Create input data for the sort_df function"""

        # columns for the dataframe
        input_cols = [
            "200",
            "201",
            "var1_growth_ratio",
            "employees",
            "reference",
        ]

        # data in the column order above
        input_data = [
            [3, 1, 1, 1, 1],
            [2, 1, 1, 1, 1],
            [1, 1, 1, 1, 1],
            [3, 1, 1, 2, 1],
            [2, 1, 1, 2, 1],
            [1, 1, 1, 2, 1],
        ]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_sort_df(self):
        """Create output data for the sort_df function"""

        # columns for the dataframe
        output_cols = [
            "200",
            "201",
            "var1_growth_ratio",
            "employees",
            "reference",
        ]

        # data in the column order above
        output_data = [
            [1, 1, 1, 2, 1],
            [1, 1, 1, 1, 1],
            [2, 1, 1, 2, 1],
            [2, 1, 1, 1, 1],
            [3, 1, 1, 2, 1],
            [3, 1, 1, 1, 1],
        ]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_sort_df(self):
        """Test the expected functionality"""

        df_input = self.input_data_sort_df()
        df_expout = self.output_data_sort_df()
        target_variable = "var1"

        df_result = sort_df(target_variable, df_input)
        assert_frame_equal(df_result, df_expout)


class TestTrimCheck:
    """Unit test for trim_check"""

    def input_data_trim_check_less_than_10(self):
        """Create input data for the trim_check function"""

        # columns for the dataframe
        input_cols = ["col1", "col2"]

        # data in the column order above
        input_data = [
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
        ]  # 9 rows (less than 10)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def input_data_trim_check_equal_10(self):
        """Create input data for the trim_check function"""

        # columns for the dataframe
        input_cols = ["col1", "col2"]

        # data in the column order above
        input_data = [
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
        ]  # 10 rows (==10)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def input_data_trim_check_more_than_10(self):
        """Create input data for the trim_check function"""

        # columns for the dataframe
        input_cols = ["col1", "col2"]

        # data in the column order above
        input_data = [
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
            [1, 1],
        ]  # 11 rows (more than 10)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_trim_check_less_than_10(self):
        """Create output data for the trim_check function"""

        # columns for the dataframe
        output_cols = ["col1", "col2", "trim_check"]

        # data in the column order above
        output_data = [
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
        ]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def output_data_trim_check_equal_10(self):
        """Create output data for the trim_check function"""

        # columns for the dataframe
        output_cols = ["col1", "col2", "trim_check"]

        # data in the column order above
        output_data = [
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
            [1, 1, "below_trim_threshold"],
        ]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def output_data_trim_check_more_than_10(self):
        """Create output data for the trim_check function"""

        # columns for the dataframe
        output_cols = ["col1", "col2", "trim_check"]

        # data in the column order above
        output_data = [
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
            [1, 1, "above_trim_threshold"],
        ]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_trim_check(self):
        """Test the expected functionality"""

        df_input_less_than_10 = self.input_data_trim_check_less_than_10()
        df_input_equal_10 = self.input_data_trim_check_equal_10()
        df_input_more_than_10 = self.input_data_trim_check_more_than_10()

        df_expout_less_than_10 = self.output_data_trim_check_less_than_10()
        df_expout_equal_10 = self.output_data_trim_check_equal_10()
        df_expout_more_than_10 = self.output_data_trim_check_more_than_10()

        df_result_less_than_10 = trim_check(df_input_less_than_10)
        df_result_equal_10 = trim_check(df_input_equal_10)
        df_result_more_than_10 = trim_check(df_input_more_than_10)

        assert_frame_equal(df_expout_less_than_10, df_result_less_than_10)
        assert_frame_equal(df_expout_equal_10, df_result_equal_10)
        assert_frame_equal(df_expout_more_than_10, df_result_more_than_10)


class TestTrimBounds:
    """Unit test for trim_bounds"""

    def input_data_trim_bounds(self):
        """Create input data for the trim_bounds function"""

        # columns for the dataframe
        input_cols = ["col1", "col2", "trim_check"]

        # data in the column order above
        input_data = [
            [1, 1, "above_trim_threshold"],
            [2, 1, "above_trim_threshold"],
            [3, 1, "above_trim_threshold"],
            [4, 1, "above_trim_threshold"],
            [5, 1, "above_trim_threshold"],
        ]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_trim_bounds(self):
        """Create output data for the trim_bounds function"""

        # columns for the dataframe
        output_cols = ["col1", "col2", "trim_check", "trim"]

        # data in the column order above
        output_data = [
            [1, 1, "above_trim_threshold", "do trim"],
            [2, 1, "above_trim_threshold", "dont trim"],
            [3, 1, "above_trim_threshold", "dont trim"],
            [4, 1, "above_trim_threshold", "dont trim"],
            [5, 1, "above_trim_threshold", "do trim"],
        ]  # ! would I want to remove 4th and 5 or just 4th

        # Create a pandas dataframe
        output_df = pandasDF(data=output_data, columns=output_cols)

        return output_df

    def test_trim_bounds(self):
        """Test the expected functionality"""

        input_df = self.input_data_trim_bounds()
        expout_df = self.output_data_trim_bounds()

        df_result = trim_bounds(input_df)  # add period filter functionality
        assert_frame_equal(df_result, expout_df)


class TestGetMeanGrowthRatio:
    """Unit test for get_mean_growth_ratio"""

    def input_data_get_mean_growth_ratio(self):
        """Create input data for the get_mean_growth_ratio function"""

        # columns for the dataframe
        input_cols = ["var1_growth_ratio", "trim"]

        # data in the column order above
        input_data = [
            [1, "dont trim"],
            [2, "dont trim"],
            [3, "dont trim"],
            [4, "dont trim"],
            [5, "dont trim"],
        ]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_get_mean_growth_ratio(self):
        """Create output data for the get_mean_growth_ratio function"""

        # output dict
        output_dict = {"class1_var1_mean_growth_ratio and count": [3.0, 5]}

        return output_dict

    def test_get_mean_growth_ratio(self):
        """Test the expected functionality"""

        input_df = self.input_data_get_mean_growth_ratio()
        expout_dict = self.output_data_get_mean_growth_ratio()
        # expout_df = self.output_data_get_mean_growth_ratio_df()

        result_dict = get_mean_growth_ratio(
            input_df, {}, "class1", "var1"
        )  # add period filter functionality
        assert result_dict == expout_dict
        # assert_frame_equal(results_df, expout_df)


class TestLoopUnique:  # testing for loops run as expected
    """Unit test for loop_unique"""

    def input_data_loop_unique(self):
        """Create input data for the loop_unique function"""

        # columns for the dataframe
        input_cols = [
            "status",
            "current_period_class",
            "200",
            "201",
            "current_period_var1",
            "current_period_var2",
            "previous_period_var1",
            "previous_period_var2",
            "employees",
            "reference",
            "trim",
        ]

        # data in the column order above
        input_data = [
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_loop_unique(self):
        """Create output data for the loop_unique function"""

        # output dict
        output_dict = {
            "class1_var1_mean_growth_ratio and count": [1.0, 7],
            "class1_var2_mean_growth_ratio and count": [2.0, 7],
            "class2_var1_mean_growth_ratio and count": [3.0, 7],
            "class2_var2_mean_growth_ratio and count": [4.0, 7],
        }

        return output_dict

    def test_loop_unique(self):
        """Test the expected functionality"""

        input_df = self.input_data_loop_unique()
        expout_dict = self.output_data_loop_unique()
        # expout_df = self.output_data_loop_unique_df()

        column = "current_period_class"
        target_variables_list = ["var1", "var2"]
        current_period = "current_period"
        previous_period = "previous_period"

        result_dict = loop_unique(
            input_df,  # removed , result_df
            column,
            target_variables_list,
            current_period,
            previous_period,
        )
        assert result_dict == expout_dict
        # assert_frame_equal(result_df, expout_df)


class TestForwardImputation:
    """Unit test for forward_imputation"""

    def input_data_forward_imputation(self):
        """Create input data for the forward_imputation function"""

        input_cols = {
            "status": "str",
            "current_period_class": "str",
            "200": "str",
            "201": "str",
            "current_period_var1": "Int64",
            "previous_period_var1": "Int64",
            "employees": "Int64",
            "reference": "Int64",
            "trim": "str",
        }

        input_data = [
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", np.nan, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", np.nan, 1, 1, 1, "dont trim"],
        ]  # (more than 10 rows per class)

        input_df = pandasDF(data=input_data, columns=input_cols.keys())
        input_df = input_df.astype(input_cols)

        return input_df

    def output_data_forward_imputation(self):
        """Create output data for the forward_imputation function"""

        output_cols = {
            "status": "str",
            "current_period_class": "str",
            "200": "str",
            "201": "str",
            "current_period_var1": "Int64",
            "previous_period_var1": "Int64",
            "employees": "Int64",
            "reference": "Int64",
            "trim": "str",
            "forwards_imputed_var1": "Int64",
        }

        output_data = [
            ["Clear", "class1", "C", "G", np.nan, 1, 1, 1, "dont trim", 4],
            ["Clear", "class2", "D", "G", np.nan, 1, 1, 1, "dont trim", 6],
        ]  # (more than 10 rows per class)

        output_df = pandasDF(
            data=output_data, columns=output_cols.keys(), index=[11, 23]
        )
        output_df = output_df.astype(output_cols)

        return output_df

    def test_forward_imputation(self):
        """Test the expected functionality"""

        input_df = self.input_data_forward_imputation()
        expout_dict = self.output_data_forward_imputation()

        column = "current_period_class"
        target_variables_list = ["var1"]
        current_period = "current_period"
        previous_period = "previous_period"

        df_result = forward_imputation(
            input_df, column, target_variables_list, current_period, previous_period
        )

        assert_frame_equal(df_result, expout_dict)


class TestBackwardsImputation:
    """Unit test for backwards_imputation"""

    def input_data_backwards_imputation(self):
        """Create input data for the backwards_imputation function"""

        # columns for the dataframe
        input_cols = {
            "status": "str",
            "current_period_class": "str",
            "200": "str",
            "201": "str",
            "current_period_var1": "Int64",
            "previous_period_var1": "Int64",
            "employees": "Int64",
            "reference": "Int64",
            "trim": "str",
        }

        # data in the column order above
        input_data = [
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["Clear", "class1", "C", "G", 4, np.nan, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["Clear", "class2", "D", "G", 6, np.nan, 1, 1, "dont trim"],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols.keys())
        input_df = input_df.astype(input_cols)

        return input_df

    def output_data_backwards_imputation(self):
        """Create output data for the backwards_imputation function"""

        # columns for the dataframe
        output_cols = {
            "status": "str",
            "current_period_class": "str",
            "200": "str",
            "201": "str",
            "current_period_var1": "Int64",
            "previous_period_var1": "Int64",
            "employees": "Int64",
            "reference": "Int64",
            "trim": "str",
            "backwards_imputed_var1": "Int64",
        }

        # data in the column order above
        output_data = [
            ["Clear", "class1", "C", "G", 4, np.nan, 1, 1, "dont trim", 1],
            ["Clear", "class2", "D", "G", 6, np.nan, 1, 1, "dont trim", 1],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        output_df = pandasDF(
            data=output_data, columns=output_cols.keys(), index=[11, 23]
        )
        output_df = output_df.astype(output_cols)

        return output_df

    def test_backwards_imputation(self):
        """Test the expected functionality"""

        input_df = self.input_data_backwards_imputation()
        expout_df = self.output_data_backwards_imputation()

        column = "current_period_class"
        target_variables_list = ["var1"]
        current_period = "current_period"
        previous_period = "previous_period"

        df_result = backwards_imputation(
            input_df, column, target_variables_list, current_period, previous_period
        )

        assert_frame_equal(df_result, expout_df)


class TestRunImputation:
    """Unit test for run_imputation"""

    def input_data_run_imputation(self):
        """Create input data for the run_imputation function"""
        # Currently input_df isn't being used as
        # fake data is hard coded into
        # function until ingest is firmed down

        # columns for the dataframe
        input_cols = {
            "status": "str",
            "reference": "Int64",
            "civ_or_def": "str",
            "Product_group": "str",
            "employees": "Int64",
            "202012_var1": "Int64",
            "202012_var2": "Int64",
            "202009_var1": "Int64",
            "202009_var2": "Int64",
        }

        # data in the column order above
        input_data = [
            ["Clear", 1, "2", "A", 100, 1, 1, 1, 3],
            ["Clear", 2, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 3, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 4, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 5, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 6, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 7, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 8, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 9, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 10, "2", "A", 100, 11, 1, 10, 3],
            ["Clear", 11, "2", "A", 100, 110, 1, 100, 3],
            ["Clear", 12, "2", "A", 100, np.nan, 1, 10, 3],
            ["Clear", 13, "2", "B", 100, 1, 1, 1, 3],
            ["Clear", 14, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 15, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 16, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 17, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 18, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 19, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 20, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 21, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 22, "2", "B", 100, 11, 1, 10, 3],
            ["Clear", 23, "2", "B", 100, 110, 1, 100, 3],
            ["Clear", 24, "2", "B", 100, 11, 1, 10, np.nan],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols.keys())
        input_df = input_df.astype(input_cols)

        return input_df

    def output_data_run_imputation(self):
        """Create output data for the run_imputation function"""
        output_cols_f = {
            "status": "str",
            "reference": "Int64",
            "200": "str",
            "201": "str",
            "employees": "Int64",
            "202012_var1": "Int64",
            "202012_var2": "Int64",
            "202009_var1": "Int64",
            "202009_var2": "Int64",
            "202012_class": "str",
            "forwards_imputed_var1": "Int64",
            "forwards_imputed_var2": "Int64",
        }

        output_data_for = [
            ["Clear", 12, "2", "A", 100, np.nan, 1, 10, 3, "2_A", 11, np.nan],
        ]  # (more than 10 rows per class)

        output_df_for = pandasDF(
            data=output_data_for, columns=output_cols_f.keys(), index=[11]
        ).astype(output_cols_f)

        # TODO check data types and update headers
        # when using real data
        # columns for the dataframe
        output_cols_b = {
            "status": "str",
            "reference": "Int64",
            "200": "str",
            "201": "str",
            "employees": "Int64",
            "202012_var1": "Int64",
            "202012_var2": "Int64",
            "202009_var1": "Int64",
            "202009_var2": "Int64",
            "202012_class": "str",
            "backwards_imputed_var1": "Int64",
            "backwards_imputed_var2": "Int64",
        }

        # TODO check data types and update headers
        # when using real data
        # data in the column order above
        output_data_back = [
            ["Clear", 24, "2", "B", 100, 11, 1, 10, np.nan, "2_B", np.nan, 3],
        ]  # (more than 10 rows per class)

        output_df_back = pandasDF(
            data=output_data_back, columns=output_cols_b.keys(), index=[23]
        ).astype(output_cols_b)

        return output_df_for, output_df_back

    def test_run_imputation(self):
        """Test the expected functionality"""

        input_df = self.input_data_run_imputation()
        expout_df_for, expout_df_back = self.output_data_run_imputation()

        target_variables_list = ["var1", "var2"]
        current_period = "202012"
        previous_period = "202009"
        result_for, result_back = run_imputation(
            input_df, target_variables_list, current_period, previous_period
        )
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 2000)
        print(result_for)
        assert_frame_equal(result_for, expout_df_for)
        assert_frame_equal(result_back, expout_df_back)
