"""Tests for functions in calculate_weights"""

import pandas as pd
import numpy as np
import logging
import pytest
import src.estimation.calculate_weights as calw
from pandas._testing import assert_frame_equal, assert_series_equal


# Three tests for calc_lower_n:
# testing calc_lower_n where duplicate refs
# testing calc_lower_n where missing col
# testing calc_lower_n where nan in reference


class TestCalcLowerNDuplicateRefs:
    """Test for calc_lower_n with duplicate refs."""

    def create_input_df(self):
        """Creates input df for test"""
        input_cols = [
            "reference",
            "709",
        ]
        data = [
            [1, "A"],
            [2, "B"],
            [2, "C"],
            [4, "D"],
            [1, "E"],
        ]
        input_df = pd.DataFrame(data=data, columns=input_cols)
        return input_df

    def test_calc_lower_n(self):
        """Test for calc_lower_n with duplicate refs."""

        input_df = self.create_input_df()
        # Call calc_lower_n function
        actual_result = calw.calc_lower_n(input_df)
        # Defined expected result
        expected_result = 3
        assert actual_result == expected_result, "calc_lower_n not behaving as expected"


class TestCalcLowerNMissingCol:
    """Test for calc_lower_n with missing col"""

    def create_input_df(self, cols, msg, exp_col):
        """Creates input df for test"""
        data = [
            [1, "A"],
            [2, "B"],
            [2, "C"],
            [4, "D"],
            [1, "E"],
        ]
        input_df = pd.DataFrame(data=data, columns=cols)
        return input_df, cols, msg, exp_col

    @pytest.mark.parametrize(
        "cols, msg, exp_col",
        [
            (["ref", "709"], f"'reference' or 709 missing.", "709"),
            (["reference", "706"], f"'reference' or 707 missing.", "707"),
        ],
    )
    def test_calc_lower_n_missing_col(self, cols, msg, exp_col):
        """Test for calc_lower_n with missing col"""
        input_df, cols, msg, exp_col = self.create_input_df(cols, msg, exp_col)

        with pytest.raises(ValueError) as error_msg:
            calw.calc_lower_n(input_df, exp_col)
        assert str(error_msg.value) == msg, "calc_lower_n not behaving as expected"


class TestCalcLowerNRefNan:
    """Test for calc_lower_n with nan in reference."""

    def create_input_df(self):
        """Creates input df for test"""
        input_cols = [
            "reference",
            "709",
        ]

        data = [
            [1, "A"],
            [2, "B"],
            [np.nan, "C"],
            [4, "D"],
            [1, "E"],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)
        return input_df

    def test_calc_lower_n_nan_ref(self):
        """Test for calc_lower_n with nan in reference."""

        input_df = self.create_input_df()

        # Call calc_lower_n function
        actual_result = calw.calc_lower_n(input_df)

        # Defined expected result
        expected_result = 3

        assert actual_result == expected_result, "calc_lower_n not behaving as expected"


# Five tests for calculate_weighting_factor:
# testing calculate_weighting_factor where missing outlier col
# testing calculate_weighting_factor filter
# testing calculate_weighting_factor 709 to numeric with no missing vals
# testing calculate_weighting_factor 709 to numeric with missing vals
# testing calculate_weighting_factor with missing vals


class TestCalcWeightMissingCol:
    """Test for calculate_weighting_factor with missing outlier col"""

    def create_input_df(self):
        """Creates input df for test"""
        input_cols = [
            "reference",
            "709",
        ]

        data = [
            [1, "A"],
            [2, "B"],
            [2, "C"],
            [4, "D"],
            [1, "E"],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)
        return input_df

    def test_calculate_weighting_factor_missing_col(self):
        """Test for calculate_weighting_factor with missing outlier col"""
        input_df = self.create_input_df()

        with pytest.raises(
            ValueError, match=r"The column essential 'outlier' is missing"
        ):
            calw.calculate_weighting_factor(input_df, "709")


class TestCalcWeightFilter:
    """Test for calculate_weighting_factor for filter
    and np.nan taken out of calculation"""

    def create_input_df(self):
        """Creates input df for test"""
        input_cols = [
            "reference",
            "instance",
            "709",
            "selectiontype",
            "statusencoded",
            "formtype",
            "cellnumber",
            "uni_count",
            "outlier",
        ]

        data = [
            [1, 0, "12", "P", "210", "0006", 1, 20, True],
            [2, 0, 14, "P", "211", "0006", 2, 4, False],
            [2, 1, 16, "P", "210", "0006", 2, 4, False],
            [4, 0, 18, "P", "210", "0006", 4, 3, False],
            [1, 0, "20", "X", "210", "0006", 5, 10, False],
            [3, 0, 1, "P", "999", "0006", 1, 20, False],
            [5, 0, 14, "P", "211", "0001", 2, 4, False],
            [6, 0, 10, "P", "210", "0006", 1, 20, False],
            [7, 1, 10, "P", "210", "0006", 5, 10, False],
            [8, 1, np.nan, "P", "210", "0006", 2, 4, False],
            [9, 0, 5, "P", "210", "0006", 1, 20, False],
            [10, 0, 10, "P", "210", "0006", 1, 20, False],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)
        return input_df

    def create_expected_output(self):
        """Creates expected df for test"""
        expected_cols = [
            "reference",
            "instance",
            "709",
            "selectiontype",
            "statusencoded",
            "formtype",
            "cellnumber",
            "uni_count",
            "outlier",
            "a_weight",
        ]

        data = [
            [1, 0, 12.0, "P", "210", "0006", 1, 20, True, 6.3],
            [2, 0, 14.0, "P", "211", "0006", 2, 4, False, 4.0],
            [2, 1, 16.0, "P", "210", "0006", 2, 4, False, 4.0],
            [4, 0, 18.0, "P", "210", "0006", 4, 3, False, 3.0],
            [1, 0, 20.0, "X", "210", "0006", 5, 10, False, 1.0],
            [3, 0, 1.0, "P", "999", "0006", 1, 20, False, 1.0],
            [5, 0, 14.0, "P", "211", "0001", 2, 4, False, 1.0],
            [6, 0, 10.0, "P", "210", "0006", 1, 20, False, 6.3],
            [7, 1, 10.0, "P", "210", "0006", 5, 10, False, 1.0],
            [8, 1, np.nan, "P", "210", "0006", 2, 4, False, 4.0],
            [9, 0, 5.0, "P", "210", "0006", 1, 20, False, 6.3],
            [10, 0, 10.0, "P", "210", "0006", 1, 20, False, 6.3],
        ]

        expected_df = pd.DataFrame(data=data, columns=expected_cols)
        return expected_df

    def create_expected_qa(self):
        """Creates expected qa df for test"""
        expected_qa_cols = [
            "cellnumber",
            "N",
            "n",
            "o",
            "a_weight",
        ]

        data = [
            [1, 20, 4, 1, 6.3],
            [2, 4, 1, 0, 4.0],
            [4, 3, 1, 0, 3.0],
            [5, 10, 0, 0, 1.0],
        ]

        expected_qa_df = pd.DataFrame(data=data, columns=expected_qa_cols)
        return expected_qa_df

    def test_calculate_weighting_factor_filter(self):
        """Test for calculate_weighting_factor for filter
        and np.nan taken out of calculation"""

        input_df = self.create_input_df()
        expected_df = self.create_expected_output()
        expected_qa_df = self.create_expected_qa()

        result_df, result_qa_df = calw.calculate_weighting_factor(input_df, "709")

        result_qa_df["a_weight"] = result_qa_df["a_weight"].round(1)

        assert_frame_equal(result_df, expected_df, check_exact=False, rtol=0.01)
        assert_frame_equal(result_qa_df, expected_qa_df, check_exact=False, rtol=0.01)


class TestCalcWeightWithMissingVals:
    """Test for calculate_weighting_factor for filter
    and np.nan taken out of calculation"""

    def create_input_df(self):
        """Creates input df for test"""
        input_cols = [
            "reference",
            "709",
            "selectiontype",
            "statusencoded",
            "formtype",
            "instance",
            "cellnumber",
            "uni_count",
            "outlier",
        ]

        data = [
            [1, 1, "P", "210", "0006", 0, 1, 10, False],
            [2, np.nan, "P", "210", "0006", 0, 1, 10, False],
            [3, 1, np.nan, "210", "0006", 0, 1, 10, False],
            [4, 1, "P", np.nan, "0006", 0, 1, 10, False],
            [5, 1, "P", "210", np.nan, 0, 1, 10, False],
            [6, 1, "P", "210", "0006", np.nan, 2, 5, False],
            [7, 1, "P", "210", "0006", 0, np.nan, 5, False],
            [8, 1, "P", "210", "0006", 0, 2, 5, np.nan],
            [9, 1, "P", "210", "0006", 0, 2, 5, False],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)
        return input_df

    def create_expected_output(self):
        """Creates expected df for test"""
        expected_cols = [
            "reference",
            "709",
            "selectiontype",
            "statusencoded",
            "formtype",
            "instance",
            "cellnumber",
            "uni_count",
            "outlier",
            "a_weight",
        ]

        data = [[1, 1, "P", "210", "0006", 0, 1, 10, False, 10.0],
            [2, np.nan, "P", "210", "0006", 0, 1, 10, False, 10.0],  # filtered from calc but weight applied
            [3, 1, np.nan, "210", "0006", 0, 1, 10, False, 1.0],  # filtered out (selectiontype)
            [4, 1, "P", np.nan, "0006", 0, 1, 10, False, 1.0],  # filtered out (statusencoded)
            [5, 1, "P", "210", np.nan, 0, 1, 10, False, 1.0],  # filtered out (formtype)
            [6, 1, "P", "210", "0006", np.nan, 2, 5, False, 2.5],  # filtered out (instance) but weight applied
            [7, 1, "P", "210", "0006", 0, np.nan, 5, False, 1.0],  # No cellno
            [8, 1, "P", "210", "0006", 0, 2, 5, np.nan, 2.5],  # No outlier
            [9, 1, "P", "210", "0006", 0, 2, 5, False, 2.5]]

        expected_df = pd.DataFrame(data=data, columns=expected_cols)
        return expected_df

    def create_expected_qa(self):
        """Creates expected qa df for test"""
        expected_qa_cols = [
            "cellnumber",
            "N",
            "n",
            "outliers",
            "a_weight",
        ]

        data = [
            [1.0, 10.0, 1.0, 0.0, 10.0],
            [2.0, 5.0, 2.0, 0.0, 2.5],
        ]

        expected_qa_df = pd.DataFrame(data=data, columns=expected_qa_cols)
        return expected_qa_df

    def test_calculate_weighting_factor_with_missing_vals(self):
        """Test for calculate_weighting_factor for filter
        and np.nan taken out of calculation"""

        input_df = self.create_input_df()
        expected_df = self.create_expected_output()
        expected_qa_df = self.create_expected_qa()
        cellno_dict = {1: 10, 2: 5}

        result_df, result_qa_df = calw.calculate_weighting_factor(input_df)

        assert_frame_equal(
            result_df, expected_df, check_exact=False, rtol=0.01, check_dtype=False
        )
        assert_frame_equal(
            result_qa_df,
            expected_qa_df,
            check_exact=False,
            rtol=0.01,
            check_dtype=False,
        )


# One tests for outlier_weights:
# test that all appropriate rows are given an a_weight = 1.0


class TestOutlierWeight:
    """Test for outlier_weights."""

    def create_input_df(self):
        """Creates input df for test"""
        input_cols = [
            "reference",
            "outlier",
        ]

        data = [
            [1, True],
            [2, False],
            [2, True],
            [4, True],
            [1, False],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)
        return input_df

    def create_expected_output(self):
        """Creates expected df for test"""
        expected_cols = [
            "reference",
            "outlier",
            "a_weight",
        ]

        data = [
            [1, True, 1.0],
            [2, False, None],
            [2, True, 1.0],
            [4, True, 1.0],
            [1, False, None],
        ]

        expected_df = pd.DataFrame(data=data, columns=expected_cols)
        return expected_df

    def test_outlier_weights(self):
        """Test for outlier_weights."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_output()

        result_df = calw.outlier_weights(input_df)
        assert_frame_equal(result_df, expected_df)
