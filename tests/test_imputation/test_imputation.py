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
    sort,
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
        )  # add quarter filter functionality
        assert_frame_equal(df_result, df_expout)


class TestCreateClassCol:
    """Unit test for create_imp_class_col"""

    def input_data_create_imp_class_col(self):
        """Create input data for the create_imp_class_col function"""

        # columns for the dataframe
        input_cols = ["col1", "col2"]

        # data in the column order above
        input_data = [["contents_1", "contents_2"]]

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_create_imp_class_col(self):
        """Create output data for the create_imp_class_col function"""

        # columns for the dataframe
        output_cols = ["col1", "col2", "class"]

        # data in the column order above
        output_data = [["contents_1", "contents_2", "contents_1contents_2"]]

        # Create a pandas dataframe
        df_expout = pandasDF(data=output_data, columns=output_cols)

        return df_expout

    def test_create_imp_class_col(self):
        """Test the expected functionality"""

        df_input = self.input_data_create_imp_class_col()
        df_expout = self.output_data_create_imp_class_col()

        col_first_half = "col1"
        col_second_half = "col2"
        class_name = "class"

        df_result = create_imp_class_col(
            df_input, col_first_half, col_second_half, class_name
        )  # add quarter filter functionality
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

        current_quarter = "190012"
        previous_quarter = "190009"

        df_result = filter_same_class(df_input, current_quarter, previous_quarter)
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
        current_quarter = "190012"
        previous_quarter = "190009"

        df_result = filter_pairs(
            df_input, target_variable, current_quarter, previous_quarter
        )  # add quarter filter functionality
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
        current_quarter = "current"
        previous_quarter = "previous"

        df_result = calc_growth_ratio(
            target_variable, input_df, current_quarter, previous_quarter
        )  # add quarter filter functionality
        assert_frame_equal(df_result, df_expout)


class TestSort:
    """Unit test for sort"""

    def input_data_sort(self):
        """Create input data for the sort function"""

        # columns for the dataframe
        input_cols = [
            "survey",
            "checkletter",
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

    def output_data_sort(self):
        """Create output data for the sort function"""

        # columns for the dataframe
        output_cols = [
            "survey",
            "checkletter",
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

    def test_sort(self):
        """Test the expected functionality"""

        df_input = self.input_data_sort()
        df_expout = self.output_data_sort()
        target_variable = "var1"

        df_result = sort(target_variable, df_input)
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

        df_result = trim_bounds(input_df)  # add quarter filter functionality
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
        )  # add quarter filter functionality
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
            "current_quarter_class",
            "survey",
            "checkletter",
            "current_quarter_var1",
            "current_quarter_var2",
            "previous_quarter_var1",
            "previous_quarter_var2",
            "employees",
            "reference",
            "trim",
        ]

        # data in the column order above
        input_data = [
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class1", 1, 1, 2, 4, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 8, 2, 2, 1, 1, "dont trim"],
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

        column = "current_quarter_class"
        target_variables_list = ["var1", "var2"]
        current_quarter = "current_quarter"
        previous_quarter = "previous_quarter"

        result_dict = loop_unique(
            input_df,  # removed , result_df
            column,
            target_variables_list,
            current_quarter,
            previous_quarter,
        )
        assert result_dict == expout_dict
        # assert_frame_equal(result_df, expout_df)


'''
    def output_data_loop_unique_df(self):
        """Create output data for the loop_unique function"""

        # columns for the dataframe
        output_cols = ["current_quarter_class","product_group",
    "civ_or_def",
    "current_quarter_var1",
    "current_quarter_var2",
    "previous_quarter_var1",
    "previous_quarter_var2",
    "employee_count",
    "ru_ref",
    "current_quarter_var1_mean_growth_ratio",
    "current_quarter_var2_mean_growth_ratio", "trim"]

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

        # columns for the dataframe
        input_cols = [
            "current_quarter_class",
            "survey",
            "checkletter",
            "current_quarter_var1",
            "previous_quarter_var1",
            "employees",
            "reference",
            "trim",
        ]

        # data in the column order above
        input_data = [
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, np.nan, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, np.nan, 1, 1, 1, "dont trim"],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_forward_imputation(self):
        """Create output data for the forward_imputation function"""

        # columns for the dataframe
        output_cols = [
            "current_quarter_class",
            "survey",
            "checkletter",
            "current_quarter_var1",
            "previous_quarter_var1",
            "employees",
            "reference",
            "trim",
            "forwards_imputed_var1",
        ]

        # data in the column order above
        output_data = [
            ["class1", 1, 1, np.nan, 1, 1, 1, "dont trim", 4],
            ["class2", 1, 1, np.nan, 1, 1, 1, "dont trim", 6],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        output_df = pandasDF(data=output_data, columns=output_cols, index=[11, 23])

        return output_df

    def test_forward_imputation(self):
        """Test the expected functionality"""

        input_df = self.input_data_forward_imputation()
        expout_dict = self.output_data_forward_imputation()

        column = "current_quarter_class"
        target_variables_list = ["var1"]
        current_quarter = "current_quarter"
        previous_quarter = "previous_quarter"

        df_result = forward_imputation(
            input_df, column, target_variables_list, current_quarter, previous_quarter
        )

        assert_frame_equal(df_result, expout_dict)


class TestBackwardsImputation:
    """Unit test for backwards_imputation"""

    def input_data_backwards_imputation(self):
        """Create input data for the backwards_imputation function"""

        # columns for the dataframe
        input_cols = [
            "current_quarter_class",
            "survey",
            "checkletter",
            "current_quarter_var1",
            "previous_quarter_var1",
            "employees",
            "reference",
            "trim",
        ]

        # data in the column order above
        input_data = [
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, 1, 1, 1, "dont trim"],
            ["class1", 1, 1, 4, np.nan, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, 1, 1, 1, "dont trim"],
            ["class2", 1, 1, 6, np.nan, 1, 1, "dont trim"],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_backwards_imputation(self):
        """Create output data for the backwards_imputation function"""

        # columns for the dataframe
        output_cols = [
            "current_quarter_class",
            "survey",
            "checkletter",
            "current_quarter_var1",
            "previous_quarter_var1",
            "employees",
            "reference",
            "trim",
            "backwards_imputed_var1",
        ]

        # data in the column order above
        output_data = [
            ["class1", 1, 1, 4, np.nan, 1, 1, "dont trim", 1.0],
            ["class2", 1, 1, 6, np.nan, 1, 1, "dont trim", 1.0],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        output_df = pandasDF(data=output_data, columns=output_cols, index=[11, 23])

        return output_df

    def test_backwards_imputation(self):
        """Test the expected functionality"""

        input_df = self.input_data_backwards_imputation()
        expout_df = self.output_data_backwards_imputation()

        column = "current_quarter_class"
        target_variables_list = ["var1"]
        current_quarter = "current_quarter"
        previous_quarter = "previous_quarter"

        df_result = backwards_imputation(
            input_df, column, target_variables_list, current_quarter, previous_quarter
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
        input_cols = [
            "reference",
            "survey",
            "checkletter",
            "employees",
            "202012_201",
            "202012_202",
            "202009_201",
            "202009_202",
        ]

        # data in the column order above
        input_data = [
            [1, "2", "A", 100, 1, 1, 1, 1],
            [2, "2", "A", 100, 11, 1, 10, 1],
            [3, "2", "A", 100, 11, 1, 10, 1],
            [4, "2", "A", 100, 11, 1, 10, 1],
            [5, "2", "A", 100, 11, 1, 10, 1],
            [6, "2", "A", 100, 11, 1, 10, 1],
            [7, "2", "A", 100, 11, 1, 10, 1],
            [8, "2", "A", 100, 11, 1, 10, 1],
            [9, "2", "A", 100, 11, 1, 10, 1],
            [10, "2", "A", 100, 11, 1, 10, 1],
            [11, "2", "A", 100, 110, 1, 100, 1],
            [12, "2", "A", 100, np.nan, 1, 10, 1],
            [13, "2", "B", 100, 1, 1, 1, 1],
            [14, "2", "B", 100, 11, 1, 10, 1],
            [15, "2", "B", 100, 11, 1, 10, 1],
            [16, "2", "B", 100, 11, 1, 10, 1],
            [17, "2", "B", 100, 11, 1, 10, 1],
            [18, "2", "B", 100, 11, 1, 10, 1],
            [19, "2", "B", 100, 11, 1, 10, 1],
            [20, "2", "B", 100, 11, 1, 10, 1],
            [21, "2", "B", 100, 11, 1, 10, 1],
            [22, "2", "B", 100, 11, 1, 10, 1],
            [23, "2", "B", 100, 110, 1, 100, 1],
            [24, "2", "B", 100, 11, 1, 10, np.nan],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        input_df = pandasDF(data=input_data, columns=input_cols)

        return input_df

    def output_data_run_imputation(self):
        """Create output data for the run_imputation function"""

        # TODO check data types and update headers
        # when using real data
        # columns for the dataframe
        output_cols_f = [
            "reference",
            "survey",  # object
            "checkletter",
            "employees",
            "202012_201",
            "202012_202",
            "202009_201",
            "202009_202",  # object
            "202012_class",
            "forwards_imputed_201",
            "forwards_imputed_202",  # object
        ]

        # TODO check data types and update headers
        # when using real data
        # data in the column order above
        output_data_for = [
            [12, "2", "A", 100, np.nan, 1, 10, 1.0, "2A", 10.0, np.nan],
        ]  # (more than 10 rows per class)

        # TODO check data types and update headers
        # when using real data
        # columns for the dataframe
        output_cols_b = [
            "reference",
            "survey",  # object
            "checkletter",
            "employees",
            "202012_201",
            "202012_202",
            "202009_201",
            "202009_202",  # object
            "202012_class",
            "backwards_imputed_201",
            "backwards_imputed_202",  # object
        ]

        # TODO check data types and update headers
        # when using real data
        # data in the column order above
        output_data_back = [
            [24, "2", "B", 100, 11.0, 1, 10, np.nan, "2B", np.nan, 1.0],
        ]  # (more than 10 rows per class)

        # Create a pandas dataframe
        output_df_for = pandasDF(
            data=output_data_for, columns=output_cols_f, index=[11]
        )

        output_df_back = pandasDF(
            data=output_data_back, columns=output_cols_b, index=[23]
        )

        return output_df_for, output_df_back

    def test_run_imputation(self):
        """Test the expected functionality"""

        input_df = self.input_data_run_imputation()
        expout_df_for, expout_df_back = self.output_data_run_imputation()

        target_variables_list = ["201", "202"]
        current_quarter = "current_quarter"
        previous_quarter = "previous_quarter"
        result_for, result_back = run_imputation(
            input_df, target_variables_list, current_quarter, previous_quarter
        )

        assert_frame_equal(result_for, expout_df_for)
        assert_frame_equal(result_back, expout_df_back)
