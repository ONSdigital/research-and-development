import pandas as pd
from pandas.testing import assert_frame_equal

from src.imputation.tmi_imputation import (
    create_mean_dict,
    apply_tmi,
)


def test_create_mean_dict():

    cols = [
        "statusencoded",
        "imp_class",
        "Value_trim",
        "Value1",
        "Value2",
        "reference",
        "employees",
        "formtype",
    ]

    # Create a sample DataFrame for testing
    data = [
        ["210", "A", "dont trim", 100, 100, "O", 100, "0001"],
        ["210", "A", "dont trim", 100, 100, "P", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "Q", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "R", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "S", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "T", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "U", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "V", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "W", 100, "0001"],
        ["210", "A", "dont trim", 0, 100, "X", 100, "0001"],
        ["210", "A", "dont trim", 1, 100, "N", 100, "0001"],
        ["211", "A", "dont trim", 1, 100, "Y", 100, "0001"],
        ["210", "B", "dont trim", 1, 100, "Z", 100, "0001"],
        ["211", "B", "dont trim", 1, 100, "W", 100, "0001"],
        ["210", "C", "dont trim", 1, 100, "V", 100, "0001"],
        ["211", "C", "dont trim", 1, 100, "U", 100, "0001"],
        ["not_210_or_211", "C", "dont trim", 1, 100, "S", 100, "0001"],
    ]

    df = pd.DataFrame(data=data, columns=cols)

    # Define the target_variable_list for testing
    target_variable_list = ["Value1", "Value2"]

    # Call the function
    mean_dict, result_df = create_mean_dict(df, target_variable_list)

    # Define the expected mean_dict and result_df
    expected_mean_dict = {
        "Value1": {
            "Value1_A_mean": 10.9,
            "Value1_A_count": 10,
            "Value1_B_mean": 1.0,
            "Value1_B_count": 2,
            "Value1_C_mean": 1.0,
            "Value1_C_count": 2,
        },
        "Value2": {
            "Value2_A_mean": 100.0,
            "Value2_A_count": 10,
            "Value2_B_mean": 100.0,
            "Value2_B_count": 2,
            "Value2_C_mean": 100.0,
            "Value2_C_count": 2,
        },
    }

    expected_cols = [
        "statusencoded",
        "imp_class",
        "Value_trim",
        "Value1",
        "Value2",
        "reference",
        "employees",
        "formtype",
        "trim_check",
        "Value1_trim",
        "Value2_trim",
        "qa_index",
    ]

    # Create a sample DataFrame for testing
    expected_result_data = [
        ["210", "A", "dont trim", 100, 100, "O", 100, "0001"]
        + ["above_trim_threshold", False, False, 0],
        ["210", "A", "dont trim", 100, 100, "P", 100, "0001"]
        + ["above_trim_threshold", True, False, 1],
        ["210", "A", "dont trim", 1, 100, "Q", 100, "0001"]
        + ["above_trim_threshold", False, False, 2],
        ["210", "A", "dont trim", 1, 100, "R", 100, "0001"]
        + ["above_trim_threshold", False, False, 3],
        ["210", "A", "dont trim", 1, 100, "S", 100, "0001"]
        + ["above_trim_threshold", False, False, 4],
        ["210", "A", "dont trim", 1, 100, "T", 100, "0001"]
        + ["above_trim_threshold", False, False, 5],
        ["210", "A", "dont trim", 1, 100, "U", 100, "0001"]
        + ["above_trim_threshold", False, False, 6],
        ["210", "A", "dont trim", 1, 100, "V", 100, "0001"]
        + ["above_trim_threshold", False, False, 7],
        ["210", "A", "dont trim", 1, 100, "W", 100, "0001"]
        + ["above_trim_threshold", False, False, 8],
        ["210", "A", "dont trim", 0, 100, "X", 100, "0001"]
        + ["above_trim_threshold", True, False, 9],
        ["210", "A", "dont trim", 1, 100, "N", 100, "0001"]
        + ["above_trim_threshold", False, True, 10],
        ["211", "A", "dont trim", 1, 100, "Y", 100, "0001"]
        + ["above_trim_threshold", False, True, 11],
        ["210", "B", "dont trim", 1, 100, "Z", 100, "0001"]
        + ["below_trim_threshold", False, False, 12],
        ["211", "B", "dont trim", 1, 100, "W", 100, "0001"]
        + ["below_trim_threshold", False, False, 13],
        ["210", "C", "dont trim", 1, 100, "V", 100, "0001"]
        + ["below_trim_threshold", False, False, 14],
        ["211", "C", "dont trim", 1, 100, "U", 100, "0001"]
        + ["below_trim_threshold", False, False, 15],
    ]

    expected_result_df = pd.DataFrame(data=expected_result_data, columns=expected_cols)

    # Assert that the result matches the expected result
    assert mean_dict == expected_mean_dict
    assert result_df.equals(expected_result_df)


def test_apply_tmi():
    # Create a sample DataFrame for testing
    data = {
        "status": [
            "Form sent out",
            "Form sent out",
            "Check needed",
            "Check needed",
            "other",
        ],
        "imp_class": ["A", "A", "B", "B", "B"],
        "Value": [5, None, 15, None, None],
    }
    df = pd.DataFrame(data)

    # Define target_variables and mean_dict for testing
    target_variables = ["Value"]
    mean_dict = {"Value": {"Value_A_mean": 4.0, "Value_B_mean": 12.0}}

    # Call the function
    result_df = apply_tmi(df, target_variables, mean_dict)

    # Define the expected result DataFrame
    expected_data = {
        "status": [
            "Form sent out",
            "Form sent out",
            "Check needed",
            "Check needed",
            "other",
        ],
        "imp_class": ["A", "A", "B", "B", "B"],
        "Value": [5, None, 15, None, None],
        "imp_marker": [
            "TMI",
            "TMI",
            "TMI",
            "TMI",
            "N/A",
        ],
        "Value_imputed": [4.0, 4.0, 12.0, 12.0, None],
    }
    expected_result_df = pd.DataFrame(expected_data)

    # Assert that the result matches the expected result
    assert_frame_equal(result_df, expected_result_df)
