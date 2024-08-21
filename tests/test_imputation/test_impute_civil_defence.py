import pytest
import numpy as np
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal

from src.imputation.tmi_imputation import run_tmi
from src.imputation.impute_civ_def import (
    prep_cd_imp_classes,
    create_civdef_dict,
    calc_cd_proportions,
    _get_random_civdef,
)


class TestCalcCDPorportions:
    """Unit tests for calc_cd_proportions function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "instance", "200", "202", "pg_sic_class"]

        data1 = [
            [1001, 1, "C", 100, "AC_1234"],  # noqa
            [1001, 2, "C", 200, "AC_1234"],  # noqa
            [1001, 3, "D", 50, "AC_1234"],  # noqa
            [3003, 1, "C", 230, "AC_1234"],  # noqa
            [3003, 2, "C", 59, "AC_1234"],  # noqa
            [3003, 3, "D", 805, "AC_1234"],  # noqa
            [3003, 4, "C", 33044, "AC_1234"],  # noqa
            [3003, 5, "D", 4677, "AC_1234"],  # noqa
            [3003, 6, np.nan, 0, "AC_1234"],  # noqa
        ]

        data2 = [
            [1001, 1, "C", 100, "AC_1234"],  # noqa
            [1001, 2, "C", 200, "AC_1234"],  # noqa
            [3003, 6, np.nan, 0, "AC_1234"],  # noqa
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
    """Unit tests for create_civdef_dict function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "201",
            "202",
            "statusencoded",
            "cellnumber",
            "rusic",
            "pg_sic_class",
            "empty_pgsic_group",
            "pg_class",
            "empty_pg_group",
        ]

        data = [
            [
                1001,
                0,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "1234",
                "nan_1234",
                False,
                "nan",
                False,
            ],  # noqa
            [
                1001,
                1,
                "C",
                "AC",
                100,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                1001,
                2,
                "C",
                "AC",
                200,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                1001,
                3,
                "D",
                "AC",
                50,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                1002,
                0,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "1234",
                "nan_1234",
                False,
                "nan",
                False,
            ],  # noqa
            [
                1002,
                1,
                "C",
                "AC",
                100,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                2002,
                0,
                np.nan,
                np.nan,
                np.nan,
                "Clear",
                "800",
                "444",
                "nan_444",
                False,
                "nan",
                False,
            ],  # noqa
            [
                2002,
                1,
                np.nan,
                "AC",
                200,
                "Clear",
                "800",
                "444",
                "AC_444",
                True,
                "AC",
                False,
            ],  # noqa
            [
                2002,
                2,
                "D",
                "AC",
                200,
                "999",
                "800",
                "444",
                "AC_444",
                True,
                "AC",
                False,
            ],  # noqa
            [
                3003,
                0,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "1234",
                "nan_1234",
                False,
                "nan",
                False,
            ],  # noqa
            [
                3003,
                1,
                "C",
                "ZZ",
                230,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                2,
                "C",
                "ZZ",
                59,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                3,
                "D",
                "ZZ",
                805,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                4,
                "C",
                "ZZ",
                33044,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                5,
                "D",
                "ZZ",
                4677,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                6,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "12",
                "nan_12",
                False,
                "nan",
                False,
            ],  # noqa
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def test_create_civdef_dict(self):
        "Test the pg_sic dict calcuclation in create_civdef_dict function."
        input_df = self.create_input_df()

        pgsic_dict, pg_dict = create_civdef_dict(input_df)

        exp_pgsic_dict = {"AC_1234": (0.75, 0.25), "ZZ_12": (0.6, 0.4)}

        assert pgsic_dict == exp_pgsic_dict

    def test_create_civdef_dict2(self):
        "Test the pg only dict calcuclation in create_civdef_dict function."
        input_df = self.create_input_df()

        pgsic_dict, pg_dict = create_civdef_dict(input_df)

        exp_pg_dict = {"AC": (0.6, 0.4)}

        assert pg_dict == exp_pg_dict


class TestPrepCDImpClasses:
    """Unit tests for prep_cd_imp_classes function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "200",
            "201",
            "202",
            "status",
            "cellnumber",
            "rusic",
        ]

        data = [
            [1001, 0, np.nan, np.nan, 0, "Clear", "800", "1234"],  # noqa
            [1001, 1, "C", "AC", 100, "Clear", "800", "1234"],  # noqa
            [1001, 2, "C", "AC", 200, "Clear", "800", "1234"],  # noqa
            [1001, 3, "D", "AC", 50, "Clear", "800", "1234"],  # noqa
            [1002, 0, np.nan, np.nan, 0, "Clear", "800", "1234"],  # noqa
            [1002, 1, "C", "AC", 100, "Clear", "800", "1234"],  # noqa
            [2002, 0, np.nan, np.nan, np.nan, "Clear", "800", "444"],  # noqa
            [2002, 1, np.nan, "AC", 200, "Clear", "800", "444"],  # noqa
            [2002, 2, "D", "AC", 200, "999", "800", "444"],  # noqa
            [3003, 0, np.nan, np.nan, 0, "Clear", "800", "1234"],  # noqa
            [3003, 1, "C", "ZZ", 230, "Clear", "800", "12"],  # noqa
            [3003, 2, "C", "ZZ", 59, "Clear", "800", "12"],  # noqa
            [3003, 3, "D", "ZZ", 805, "Clear", "800", "12"],  # noqa
            [3003, 4, "C", "ZZ", 33044, "Clear", "800", "12"],  # noqa
            [3003, 5, "D", "ZZ", 4677, "Clear", "800", "12"],  # noqa
            [3003, 6, np.nan, np.nan, 0, "Clear", "800", "12"],  # noqa
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_exp_output_df(self):
        """Create an expected output dataframe for the test."""
        output_cols = [
            "reference",
            "instance",
            "200",
            "201",
            "202",
            "status",
            "cellnumber",
            "rusic",
            "pg_sic_class",
            "empty_pgsic_group",
            "pg_class",
            "empty_pg_group",
        ]

        data = [
            [
                1001,
                0,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "1234",
                "nan_1234",
                False,
                "nan",
                False,
            ],  # noqa
            [
                1001,
                1,
                "C",
                "AC",
                100,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                1001,
                2,
                "C",
                "AC",
                200,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                1001,
                3,
                "D",
                "AC",
                50,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                1002,
                0,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "1234",
                "nan_1234",
                False,
                "nan",
                False,
            ],  # noqa
            [
                1002,
                1,
                "C",
                "AC",
                100,
                "Clear",
                "800",
                "1234",
                "AC_1234",
                False,
                "AC",
                False,
            ],  # noqa
            [
                2002,
                0,
                np.nan,
                np.nan,
                np.nan,
                "Clear",
                "800",
                "444",
                "nan_444",
                False,
                "nan",
                False,
            ],  # noqa
            [
                2002,
                1,
                np.nan,
                "AC",
                200,
                "Clear",
                "800",
                "444",
                "AC_444",
                True,
                "AC",
                False,
            ],  # noqa
            [
                2002,
                2,
                "D",
                "AC",
                200,
                "999",
                "800",
                "444",
                "AC_444",
                True,
                "AC",
                False,
            ],  # noqa
            [
                3003,
                0,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "1234",
                "nan_1234",
                False,
                "nan",
                False,
            ],  # noqa
            [
                3003,
                1,
                "C",
                "ZZ",
                230,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                2,
                "C",
                "ZZ",
                59,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                3,
                "D",
                "ZZ",
                805,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                4,
                "C",
                "ZZ",
                33044,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                5,
                "D",
                "ZZ",
                4677,
                "Clear",
                "800",
                "12",
                "ZZ_12",
                False,
                "ZZ",
                False,
            ],  # noqa
            [
                3003,
                6,
                np.nan,
                np.nan,
                0,
                "Clear",
                "800",
                "12",
                "nan_12",
                False,
                "nan",
                False,
            ],  # noqa
        ]

        output_df = pandasDF(data=data, columns=output_cols)
        return output_df

    def test_prep_cd_imp_classes(self):
        """Test for prep_imp_classes function"""
        input_df = self.create_input_df()
        expected_df = self.create_exp_output_df()

        result_df = prep_cd_imp_classes(input_df)

        assert_frame_equal(result_df, expected_df)


class TestPrepCDImpClasses2:
    """Unit tests for calc_cd_proportions function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "201",
            "rusic",
            "cellnumber",
            "status",
            "200",
        ]

        data = [
            [
                1,
                "A",
                "12",
                "800",
                "Clear",
                "C",
            ],  # noqa # test valid (statusencoded=211) row with no nans in class name
            [
                2,
                "A",
                "12",
                "800",
                "Clear - overridden",
                "D",
            ],  # noqa #  test valid (statusencoded=210, 200 not null) row with no nans in class name
            [
                3,
                "A",
                "12",
                "800",
                "other",
                "C",
            ],  # noqa #  test other but still valid rows within class
            [
                4,
                "A",
                "12",
                "800",
                "Clear",
                np.nan,
            ],  # noqa #  test np.nan in 200 but still valid rows within class
            [
                5,
                "B",
                "12",
                "800",
                "Clear",
                np.nan,
            ],  # noqa #  test np.nan in 200 and no valid rows within class - FLAG
            [
                6,
                "B",
                "12",
                "800",
                "other",
                "C",
            ],  # noqa #  test statusencoded not 211 or 210 and no valid rows within class - FLAG
            [
                6,
                "B",
                "12",
                "800",
                "other",
                np.nan,
            ],  # noqa #  test statusencoded not 211 or 210 and np.nan in 200 and no valid rows within class - FLAG
            [
                6,
                np.nan,
                "122",
                "800",
                "other",
                np.nan,
            ],  # noqa #  test statusencoded not 211 or 210 and np.nan in 200 and no valid rows within class but nan in class name
            [1, np.nan, "12", "800", "Clear", "C"],  # noqa #  nan in class name
            [1, "A", np.nan, "800", "Clear", "C"],  #  nan in class name
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_exp_output_df(self):
        """Create an expected output dataframe for the test."""
        output_cols = [
            "reference",
            "201",
            "rusic",
            "cellnumber",
            "status",
            "200",
            "pg_sic_class",
            "empty_pgsic_group",
            "pg_class",
            "empty_pg_group",
        ]

        data = [
            [
                1,
                "A",
                "12",
                "800",
                "Clear",
                "C",
                "A_12",
                False,
                "A",
                False,
            ],  # noqa # test valid (statusencoded=211) row with no nans in class name
            [
                2,
                "A",
                "12",
                "800",
                "Clear - overridden",
                "D",
                "A_12",
                False,
                "A",
                False,
            ],  # noqa #  test valid (statusencoded=210, 200 not null) row with no nans in class name
            [
                3,
                "A",
                "12",
                "800",
                "other",
                "C",
                "A_12",
                False,
                "A",
                False,
            ],  # noqa #  test other but still valid rows within class
            [
                4,
                "A",
                "12",
                "800",
                "Clear",
                np.nan,
                "A_12",
                False,
                "A",
                False,
            ],  # noqa #  test np.nan in 200 but still valid rows within class
            [
                5,
                "B",
                "12",
                "800",
                "Clear",
                np.nan,
                "B_12",
                True,
                "B",
                True,
            ],  # noqa #  test np.nan in 200 and no valid rows within class - FLAG
            [
                6,
                "B",
                "12",
                "800",
                "other",
                "C",
                "B_12",
                True,
                "B",
                True,
            ],  # noqa #  test statusencoded not 211 or 210 and no valid rows within class - FLAG
            [
                6,
                "B",
                "12",
                "800",
                "other",
                np.nan,
                "B_12",
                True,
                "B",
                True,
            ],  # noqa #  test statusencoded not 211 or 210 and np.nan in 200 and no valid rows within class - FLAG
            [
                6,
                np.nan,
                "122",
                "800",
                "other",
                np.nan,
                "nan_122",
                False,
                "nan",
                False,
            ],  # noqa #  test statusencoded not 211 or 210 and np.nan in 200 and no valid rows within class but nan in class name
            [
                1,
                np.nan,
                "12",
                "800",
                "Clear",
                "C",
                "nan_12",
                False,
                "nan",
                False,
            ],  # noqa #  nan in class name
            [
                1,
                "A",
                np.nan,
                "800",
                "Clear",
                "C",
                "A_nan",
                False,
                "A",
                False,
            ],  #  nan in class name
        ]

        output_df = pandasDF(data=data, columns=output_cols)
        return output_df

    def test_prep_cd_imp_classes2(self):
        """Test for prep_imp_classes function"""
        input_df = self.create_input_df()
        expected_df = self.create_exp_output_df()

        result_df = prep_cd_imp_classes(input_df)

        assert_frame_equal(result_df, expected_df)


class TestGetRandomCivdef(object):
    """Tests for _get_random_civdef."""

    @pytest.mark.parametrize(
            "seed, proportions", 
            [
                (12345, [0.4, 0.6]),
                (1, [0.52, 0.48]),
                (54321, [0.1, 0.9]),
                (9999999, [0.95, 0.05]),
            ]
    )
    def test__get_random_civdef(self, seed, proportions):
        """General tests for _get_random_civdef."""
        # ensure the seed continues to give the same value with a given proportion
        values = []
        for i in range(10):
            rand = _get_random_civdef(seed=seed, proportions=proportions)
            values.append(rand)
        unique = set(values)
        assert len(unique) == 1, "Multiple random values found from one seed."
