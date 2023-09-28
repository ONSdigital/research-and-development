import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from src.imputation.tmi_imputation import (
    create_mean_dict,
    apply_tmi,
)


def test_create_mean_dict():

    # Create a sample DataFrame for testing
    data = {
        'statusencoded': ["210", "210", "210", "210", "210", "210", "210",
                          "210", "210", "210", "211", "210", "211", "210",
                          "211", "not_210_or_211"],
        'imp_class': ['A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
                      'B', 'B', 'C', 'C', 'C'],
        'Value_trim': ['dont trim', 'dont trim', 'dont trim', 'dont trim',
                       'dont trim', 'dont trim', 'dont trim', 'dont trim',
                       'dont trim', 'dont trim', 'dont trim', 'dont trim',
                       'dont trim', 'dont trim', 'dont trim', 'dont trim'],
        'Value1': [100, 100, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], #TODO if both same which should be trimmed?
        'Value2': [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                   100, 100, 100, 100, 100],
        'reference': ['P', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y',
                      'Z', 'W', 'V', 'U', 'S'],
        'employees': [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                      100, 100, 100, 100, 100],
        'formtype': ['0001', '0001', '0001', '0001', '0001', '0001', '0001',
                     '0001', '0001', '0001', '0001', '0001', '0001', '0001',
                     '0001', '0001']
    }
    df = pd.DataFrame(data)

    # Define the target_variable_list for testing
    target_variable_list = ['Value1', 'Value2']

    # Call the function
    mean_dict, result_df = create_mean_dict(df, target_variable_list)

    # Define the expected mean_dict and result_df
    expected_mean_dict = {
        'Value1': {'Value1_A_mean': 12.0, 'Value1_A_count': 9,
                   'Value1_B_mean': 1.0, 'Value1_B_count': 2,
                   'Value1_C_mean': 1.0, 'Value1_C_count': 2},
        'Value2': {'Value2_A_mean': 100.0, 'Value2_A_count': 9,
                   'Value2_B_mean': 100.0, 'Value2_B_count': 2,
                   'Value2_C_mean': 100.0, 'Value2_C_count': 2}
    }

    expected_result_data = {
        'statusencoded': ["210", "210", "210", "210", "210", "210", "210",
                          "210", "210", "210", "211", "210", "211", "210",
                          "211"],
        'imp_class': ['A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A',
                      'B', 'B', 'C', 'C'],
        'Value_trim': ['dont trim', 'dont trim', 'dont trim', 'dont trim',
                       'dont trim', 'dont trim', 'dont trim', 'dont trim',
                       'dont trim', 'dont trim', 'dont trim', 'dont trim',
                       'dont trim', 'dont trim', 'dont trim'], #TODO determine function of this column
        'Value1': [100, 100, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], 
        'Value2': [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                   100, 100, 100, 100],
        'reference': ['P', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                      'Y', 'Z', 'W', 'V', 'U'],
        'employees': [100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                      100, 100, 100, 100, 100],
        'formtype': ['0001', '0001', '0001', '0001', '0001', '0001', '0001',
                     '0001', '0001', '0001', '0001', '0001', '0001', '0001',
                     '0001'],
        'trim_check': ['above_trim_threshold', 'above_trim_threshold',
                       'above_trim_threshold', 'above_trim_threshold',
                       'above_trim_threshold', 'above_trim_threshold',
                       'above_trim_threshold', 'above_trim_threshold',
                       'above_trim_threshold', 'above_trim_threshold',
                       'above_trim_threshold', 'below_trim_threshold',
                       'below_trim_threshold', 'below_trim_threshold',
                       'below_trim_threshold'],
        'Value1_trim': [False, True, True, False,
                        False, False, False, False,
                        False, False, False, False,
                        False, False, False],
        'Value2_trim': [True, False, False, False,
                        False, False, False, False,
                        False, False, True, False,
                        False, False, False],
        'qa_index': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    }
    expected_result_df = pd.DataFrame(expected_result_data)

    # Assert that the result matches the expected result
    assert mean_dict == expected_mean_dict
    assert result_df.equals(expected_result_df)


def test_apply_tmi():
    # Create a sample DataFrame for testing
    data = {
        'status': ['Form sent out', 'Form sent out', 'Check needed',
                   'Check needed', 'other'],
        'imp_class': ['A', 'A', 'B', 'B', 'B'],
        'Value': [5, None, 15, None, None]
    }
    df = pd.DataFrame(data)

    # Define target_variables and mean_dict for testing
    target_variables = ['Value']
    mean_dict = {
        'Value': {'Value_A_mean': 4.0, 'Value_B_mean': 12.0}
    }

    # Call the function
    result_df = apply_tmi(df, target_variables, mean_dict)

    # Define the expected result DataFrame
    expected_data = {
        'status': ['Form sent out', 'Form sent out', 'Check needed',
                   'Check needed', 'other'],
        'imp_class': ['A', 'A', 'B', 'B', 'B'],
        'Value': [5, None, 15, None, None],
        'imp_marker': ['TMI', 'TMI', 'TMI', 'TMI', 'N/A', ],
        'Value_imputed': [4.0, 4.0, 12.0, 12.0, None]
    }
    expected_result_df = pd.DataFrame(expected_data)

    # Assert that the result matches the expected result
    assert_frame_equal(result_df, expected_result_df)

