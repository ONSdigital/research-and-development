"""Test for functions in calculate_weights"""

import pandas as pd
import numpy as np
import logging
import pytest
import src.estimation.calculate_weights as calw
from pandas._testing import assert_frame_equal


# Four tests for calc_lower_n:
# testing calc_lower_n where duplicate refs
# testing calc_lower_n where missing col
# testing calc_lower_n where nan in reference
# testing calc_lower_n where nan in exp_col


def test_calc_lower_n():
    """Test for calc_lower_n with duplicate refs."""

    input_cols = [
        "reference",
        "709",
    ]

    data = [
        [1, 'A'],
        [2, 'B'],
        [2, 'C'],
        [4, 'D'],
        [1, 'E'],
    ]

    input_df = pd.DataFrame(data=data, columns=input_cols)

    # Call calc_lower_n function
    actual_result = calw.calc_lower_n(input_df)

    # Defined expected result
    expected_result = 3

    assert actual_result == expected_result


def test_calc_lower_n_missing_col():
    """Test for calc_lower_n with missing col"""

    input_cols = [
        "ref",
        "709",
    ]

    data = [
        [1, 'A'],
        [2, 'B'],
        [2, 'C'],
        [4, 'D'],
        [1, 'E'],
    ]

    input_df = pd.DataFrame(data=data, columns=input_cols)


    with pytest.raises(ValueError):
        calw.calc_lower_n(input_df)


def test_calc_lower_n_nan_ref():
    """Test for calc_lower_n with nan in reference."""

    input_cols = [
        "reference",
        "709",
    ]

    data = [
        [1, 'A'],
        [2, 'B'],
        [np.nan, 'C'],
        [4, 'D'],
        [1, 'E'],
    ]

    input_df = pd.DataFrame(data=data, columns=input_cols)

    # Call calc_lower_n function
    actual_result = calw.calc_lower_n(input_df)

    # Defined expected result
    expected_result = 3

    assert actual_result == expected_result


def test_calc_lower_n_nan_col():
    """Test for calc_lower_n with nan in reference."""

    input_cols = [
        "reference",
        "709",
    ]

    data = [
        [1, 'A'],
        [2, 'B'],
        [2, np.nan],
        [4, 'D'],
        [1, 'E'],
    ]

    input_df = pd.DataFrame(data=data, columns=input_cols)

    # Call calc_lower_n function
    actual_result = calw.calc_lower_n(input_df)

    # Defined expected result
    expected_result = 3

    assert actual_result == expected_result


# [no.] tests for calculate_weighting_factor:
# testing calculate_weighting_factor where missing outlier col
# testing calculate_weighting_factor filter
# testing calculate_weighting_factor for 709 numeric


def test_calculate_weighting_factor_missing_col():
    """Test for calculate_weighting_factor with missing outlier col"""

    input_cols = [
        "reference",
        "709",
    ]

    data = [
        [1, 'A'],
        [2, 'B'],
        [2, 'C'],
        [4, 'D'],
        [1, 'E'],
    ]

    input_df = pd.DataFrame(data=data, columns=input_cols)

    with pytest.raises(ValueError):
        calw.calculate_weighting_factor(input_df, None, "709")


# def test_calculate_weighting_factor_filter(caplog):
#     """Test for calculate_weighting_factor for 709 numeric"""

#     input_cols = [
#         "reference",
#         "709",
#         "selectiontype",
#         "statusencoded",
#         "formtype",
#         "instance",
#         "cellnumber",
#         "outlier",
#     ]

#     data = [
#         [1, '12', "P", "210", "0006", 0, 1, True],
#         [2, '14', "P", "211", "0006", 0, 2, False],
#         [2, '16', "P", "210", "0006", 0, 3, False],
#         [4, '18', "P", "210", "0006", 0, 4, False],
#         [1, '20', "X", "210", "0006", 0, 5, False],
#         [3, '1', "P", "999", "0006", 0, 1, False],
#         [5, '14', "P", "211", "0001", 0, 2, False],
#         [6, '10', "P", "210", "0006", 1, 1, False],
#     ]

#     input_df = pd.DataFrame(data=data, columns=input_cols)

#     cellno_dict = {1: 2, 2: 4, 3: 6, 4: 8, 5: 10}

#     expected_cols = [
#         "reference",
#         "709",
#         "selectiontype",
#         "statusencoded",
#         "formtype",
#         "instance",
#         "cellnumber",
#         "outlier",
#         "a_weight",
#     ]

#     data = [
#         [1, 12, "P", "210", "0006", 0, 1, True],
#         [2, 14, "P", "211", "0006", 0, 2, False],
#         [2, 16, "P", "210", "0006", 0, 3, False],
#         [4, 18, "P", "210", "0006", 0, 4, False],
#         [1, 20, "X", "210", "0006", 0, 5, False],
#         [3, 1, "P", "999", "0006", 0, 1, False],
#         [5, 14, "P", "211", "0001", 0, 2, False],
#         [6, 10, "P", "210", "0006", 1, 1, False],

#     ]

#     expected_df = pd.DataFrame(data=data, columns=expected_cols)

#     result_df, qa_df = calw.calculate_weighting_factor(input_df, cellno_dict)

#     print(result_df)
#     print(qa_df)
#     print(expected_df)

#     assert_frame_equal(result_df, expected_df)


# One tests for outlier_weights:
# test that all appropriate rows are given an a_weight = 1.0

def test_outlier_weights():
    """Test for outlier_weights."""

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

    result_df = calw.outlier_weights(input_df)

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

    assert_frame_equal(result_df, expected_df)
