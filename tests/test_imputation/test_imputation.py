import numpy as np
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


class TestCalcGrowthRatio:
    """Unit test for calc_growth_ratio"""

    def input_data_calc_growth_ratio(self):
        """Create input data for the calc_growth_ratio function"""

        # columns for the dataframe
        input_cols = ["current_var1", "previous_var1", "current_var2", "previous_var2"]

        # data in the column order above
        input_data = [[2, 8, 2, 4]]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_calc_growth_ratio(
        self,
    ):  # 'Imputed(Fwd)','Imputed(Bwd)', 'ACTUAL', 'Const(Prog)'
        """Create output data for the calc_growth_ratio function"""

        # columns for the dataframe
        output_cols = [
            "current_var1",
            "previous_var1",
            "current_var2",
            "previous_var2",
            "var1_growth_ratio",
        ]

        # data in the column order above
        output_data = [[2, 8, 2, 4, 0.25]]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_calc_growth_ratio(self):
        """Test the expected functionality"""

        target_variable = "var1"
        input_df = self.input_data_calc_growth_ratio()
        df_expout = self.output_data_calc_growth_ratio()
        current_period = "current"
        previous_period = "previous"

        df_result = calc_growth_ratio(
            target_variable, input_df, current_period, previous_period
        )  # add period filter functionality
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


'''
    def output_data_get_mean_growth_ratio_df(self):
        """Create output data for the get_mean_growth_ratio function"""

        # columns for the dataframe
        output_cols = ['var1_growth_ratio', 'col2', 'var1_mean_growth_ratio']

        # data in the column order above
        output_data = [[1, 1, 3.0],
                      [2, 1, 3.0],
                      [3, 1, 3.0],
                      [4, 1, 3.0],
                      [5, 1, 3.0]]

        # Create a pandas dataframe
        output_df = pandasDF(data=output_data, columns=output_cols)

    return output_df
'''


class TestLoopUnique:  # testing for loops run as expected
    """Unit test for loop_unique"""

    def input_data_loop_unique(self):
        """Create input data for the loop_unique function"""

        # columns for the dataframe
        input_cols = [
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
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", "C", "G", 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 8, 2, 2, 1, 1, "dont trim"],
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


'''
    def output_data_loop_unique_df(self):
        """Create output data for the loop_unique function"""

        # columns for the dataframe
        output_cols = ["current_period_class","product_group",
    "civ_or_def",
    "current_period_var1",
    "current_period_var2",
    "previous_period_var1",
    "previous_period_var2",
    "employee_count",
    "ru_ref",
    "current_period_var1_mean_growth_ratio",
    "current_period_var2_mean_growth_ratio", "trim"]

        # data in the column order above
        output_data = [["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0], 'dont trim',
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class1", 1, 1, 1, 2, 0.5, 0.5, 1, 1, 2.0, 4.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim'],
                      ["class2", 1, 1, 3, 4, 0.5, 0.5, 1, 1, 6.0, 8.0, 'dont trim']]

        # Create a pandas dataframe
        output_df = pandasDF(data=output_data, columns=output_cols)

        return output_df
'''


class TestForwardImputation:
    """Unit test for forward_imputation"""

    def input_data_forward_imputation(self):
        """Create input data for the forward_imputation function"""

        input_cols = {
            "current_period_class" : "str",
            "200" : "str",
            "201" : "str",
            "current_period_var1" :  "Int64",
            "previous_period_var1" : "Int64",
            "employees" : "Int64",
            "reference" : "Int64",
            "trim" : "str"
        }

        input_data = [
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", np.nan, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", np.nan, 1, 1, 1, "dont trim"],
        ]  # (more than 10 rows per class)

        input_df = pandasDF(data=input_data, columns=input_cols.keys())
        input_df = input_df.astype(input_cols) 
        
        return input_df

    def output_data_forward_imputation(self):
        """Create output data for the forward_imputation function"""

        output_cols = {
            "current_period_class" : "str",
            "200" : "str",
            "201" : "str",
            "current_period_var1" :  "Int64",
            "previous_period_var1" : "Int64",
            "employees" : "Int64",
            "reference" : "Int64",
            "trim" : "str",
            "forwards_imputed_var1" : "Int64"
        }

        output_data = [
            ["class1", "C", "G", np.nan, 1, 1, 1, "dont trim", 4],
            ["class2", "D", "G", np.nan, 1, 1, 1, "dont trim", 6],
        ]  # (more than 10 rows per class)

        output_df = pandasDF(data=output_data, columns=output_cols.keys(), index=[11, 23])
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
            "current_period_class" : "str",
            "200" : "str",
            "201" : "str",
            "current_period_var1" :  "Int64",
            "previous_period_var1" : "Int64",
            "employees" : "Int64",
            "reference" : "Int64",
            "trim" : "str"
        }

        # data in the column order above
        input_data = [
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, 1, 1, 1, "dont trim"],
            ["class1", "C", "G", 4, np.nan, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, 1, 1, 1, "dont trim"],
            ["class2", "D", "G", 6, np.nan, 1, 1, "dont trim"],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols.keys())
        input_df = input_df.astype(input_cols)
        
        return input_df

    def output_data_backwards_imputation(self):
        """Create output data for the backwards_imputation function"""

        # columns for the dataframe
        output_cols = {
            "current_period_class" : "str",
            "200" : "str",
            "201" : "str",
            "current_period_var1" :  "Int64",
            "previous_period_var1" : "Int64",
            "employees" : "Int64",
            "reference" : "Int64",
            "trim" : "str",
            "backwards_imputed_var1" : "Int64"
        }

        # data in the column order above
        output_data = [
            ["class1", "C", "G",4, np.nan, 1, 1, "dont trim", 1],
            ["class2", "D", "G", 6, np.nan, 1, 1, "dont trim", 1],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        output_df = pandasDF(data=output_data, columns=output_cols.keys(), index=[11, 23])
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
            "reference" : "Int64",
            "civ_or_def" : "str",
            "Product_group" : "str",
            "employees" : "Int64",
            "202012_var1" : "Int64",
            "202012_var2" : "Int64",
            "202009_var1" : "Int64",
            "202009_var2" : "Int64",
        }

        # data in the column order above
        input_data = [
            [1, "2", "A", 100, 1, 1, 1, 3],
            [2, "2", "A", 100, 11, 1, 10, 3],
            [3, "2", "A", 100, 11, 1, 10, 3],
            [4, "2", "A", 100, 11, 1, 10, 3],
            [5, "2", "A", 100, 11, 1, 10, 3],
            [6, "2", "A", 100, 11, 1, 10, 3],
            [7, "2", "A", 100, 11, 1, 10, 3],
            [8, "2", "A", 100, 11, 1, 10, 3],
            [9, "2", "A", 100, 11, 1, 10, 3],
            [10, "2", "A", 100, 11, 1, 10, 3],
            [11, "2", "A", 100, 110, 1, 100, 3],
            [12, "2", "A", 100, np.nan, 1, 10, 3],
            [13, "2", "B", 100, 1, 1, 1, 3],
            [14, "2", "B", 100, 11, 1, 10, 3],
            [15, "2", "B", 100, 11, 1, 10, 3],
            [16, "2", "B", 100, 11, 1, 10, 3],
            [17, "2", "B", 100, 11, 1, 10, 3],
            [18, "2", "B", 100, 11, 1, 10, 3],
            [19, "2", "B", 100, 11, 1, 10, 3],
            [20, "2", "B", 100, 11, 1, 10, 3],
            [21, "2", "B", 100, 11, 1, 10, 3],
            [22, "2", "B", 100, 11, 1, 10, 3],
            [23, "2", "B", 100, 110, 1, 100, 3],
            [24, "2", "B", 100, 11, 1, 10, np.nan],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols.keys())
        input_df = input_df.astype(input_cols)

        return input_df

    def output_data_run_imputation(self):
        """Create output data for the run_imputation function"""
        output_cols_f = {
            "reference" : "Int64",
            "200" : "str",
            "201" : "str",
            "employees" : "Int64",
            "202012_var1" : "Int64",
            "202012_var2" : "Int64",
            "202009_var1" : "Int64",
            "202009_var2" : "Int64",
            "202012_class" : "str",
            "forwards_imputed_var1" : "Int64",
            "forwards_imputed_var2" : "Int64",
        }

        output_data_for = [
            [12, "2", "A", 100, np.nan, 1, 10, 3, "2_A", 11, np.nan],
        ]  # (more than 10 rows per class)

        output_df_for = pandasDF(
            data=output_data_for, columns=output_cols_f.keys(), index=[11]
        ).astype(output_cols_f)


        # TODO check data types and update headers
        # when using real data
        # columns for the dataframe
        output_cols_b = {
            "reference" : "Int64",
            "200" : "str",
            "201" : "str",
            "employees" : "Int64",
            "202012_var1" : "Int64",
            "202012_var2" : "Int64",
            "202009_var1" : "Int64",
            "202009_var2" : "Int64",
            "202012_class" : "str",
            "backwards_imputed_var1" : "Int64",
            "backwards_imputed_var2" : "Int64",
        }

        # TODO check data types and update headers
        # when using real data
        # data in the column order above
        output_data_back = [
            [24, "2", "B", 100, 11, 1, 10, np.nan, "2_B", np.nan, 3],
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

        assert_frame_equal(result_for, expout_df_for)
        assert_frame_equal(result_back, expout_df_back)
