import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal

from src.imputation.tmi_imputation import run_tmi
from src.imputation.impute_civ_def import (
    impute_civil_defence,
    create_civdef_dict,
    calc_cd_proportions
)
# from src.imputation.impute_civil_defence import (
#     calc_cd_proportions
# )
# from src.imputation.impute_civil_defence import calc_cd_proportions

class TestCalcCDPorportions:
    """Unit tests for calc_cd_proportions function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "202",
            "pg_sic_class"]

        data1 = [
            [1001, 1, "C", 100, "AC_1234"],
            [1001, 2, "C", 200, "AC_1234"],
            [1001, 3, "D", 50, "AC_1234"],
            [3003, 1, "C", 230, "AC_1234"],
            [3003, 2, "C", 59, "AC_1234"],
            [3003, 3, "D", 805, "AC_1234"],
            [3003, 4, "C", 33044, "AC_1234"],
            [3003, 5, "D", 4677, "AC_1234"],
            [3003, 6, np.nan, 0, "AC_1234"],
        ]

        data2 = [
            [1001, 1, "C", 100, "AC_1234"],
            [1001, 2, "C", 200, "AC_1234"],
            [3003, 6, np.nan, 0, "AC_1234"],
        ]

        input_df1 = pandasDF(data=data1, columns=input_cols)
        input_df2 = pandasDF(data=data2, columns=input_cols)
        return input_df1, input_df2

    def test_calc_cd_proportions(self):
        "Test for calc_cd_proportions function."
        input_df1, input_df2 = self.create_input_df()

        out_c1, out_d1 = calc_cd_proportions(input_df1)

        assert out_c1 == 0.625  
        assert out_d1 == 0.375

        out_c2, out_d2 = calc_cd_proportions(input_df2)

        assert out_c2 == 1.0
        assert out_d2 == 0.0
        

class TestCreateCivdefDict:
    """Unit tests for calc_cd_proportions function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "201",
            "202",
            "cellnumber",
            "rusic"]

        data = [
            [1001, 0, np.nan, np.nan, 0, "800", "1234"],
            [1001, 1, "C", "AC", 100, "800", "1234"],
            [1001, 2, "C", "AC", 200, "800", "1234"],
            [1001, 3, "D", "AC", 50, "800", "1234"],
            [1002, 0, np.nan, np.nan, 0, "800", "1234"],
            [1002, 1, "C", "AC", 100, "800", "1234"],
            [2002, 0, np.nan, np.nan, np.nan, "800", "1234"],
            [2002, 1, np.nan, "DD", 200, "800", "1234"],
            [3003, 0, np.nan, np.nan, 0, "800", "1234"],
            [3003, 1, "C",  "ZZ", 230, "800", "12"],
            [3003, 2, "C",  "ZZ", 59, "800", "12"],
            [3003, 3, "D",  "ZZ", 805, "800", "12"],
            [3003, 4, "C",  "ZZ", 33044, "800", "12"],
            [3003, 5, "D",  "ZZ", 4677, "800", "12"],
            [3003, 6, np.nan, np.nan, 0, "800", "12"],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_exp_output_df(self):
        """Create an input dataframe for the test."""
        output_cols = [
            "reference",
            "instance",
            "200",
            "201",
            "202",
            "cellnumber",
            "rusic",
            "pg_sic_class",
            "empty_group"]

        data = [
            [1001, 0, np.nan, np.nan, 0, "800", "1234", "nan_1234", False],
            [1001, 1, "C", "AC", 100, "800", "1234", "AC_1234", False],
            [1001, 2, "C", "AC", 200, "800", "1234", "AC_1234", False],
            [1001, 3, "D", "AC", 50, "800", "1234", "AC_1234", False],
            [1002, 0, np.nan, np.nan, 0, "800", "1234", "nan_1234", False],
            [1002, 1, "C", "AC", 100, "800", "1234", "AC_1234", False],
            [2002, 0, np.nan, np.nan, np.nan, "800", "1234", "nan_1234", False],
            [2002, 1, np.nan, "DD", 200, "800", "1234", "DD_1234", False],
            [3003, 0, np.nan, np.nan, 0, "800", "1234", "nan_1234", False],
            [3003, 1, "C",  "ZZ", 230, "800", "12", "ZZ_12", False],
            [3003, 2, "C",  "ZZ", 59, "800", "12", "ZZ_12", False],
            [3003, 3, "D",  "ZZ", 805, "800", "12", "ZZ_12", False],
            [3003, 4, "C",  "ZZ", 33044, "800", "12", "ZZ_12", False],
            [3003, 5, "D",  "ZZ", 4677, "800", "12", "ZZ_12", False],
            [3003, 6, np.nan, np.nan, 0, "800", "12", "nan_12", False],
        ]

        output_df = pandasDF(data=data, columns=output_cols)
        return output_df

    def test_create_civdef_dict(self):
        "Test for calc_cd_proportions function."
        input_df = self.create_input_df()
        expected_df = self.create_exp_output_df()

        result_dict, result_df = create_civdef_dict(input_df)

        assert_frame_equal(result_df, expected_df)

