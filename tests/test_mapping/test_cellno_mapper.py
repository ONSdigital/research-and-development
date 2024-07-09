"""Test for cellno mapper function"""
import pytest
import pandas as pd

from src.mapping.cellno_mapper import clean_thousands_comma, join_cellno_mapper


@pytest.fixture(scope="module")
def cellno_mapper_df():
    columns = ["cell_no", "UNI_Count", "uni_employment", "uni_turnover"]
    data = [
        [41, "87,577", "27", "22,867"],
        [117, "13,147", "11,88", "1,838"],
        [674, "23", "5,757", "15,284"],
        [805, "14", "28,154", "8,848"],
        [888, "9", "18,5", "8,558"],
    ]
    cellno_mapper = pd.DataFrame(data=data, columns=columns)
    return cellno_mapper


@pytest.fixture(scope="module")
def full_input_df(self):
    """Main input data for test."""
    columns = ["reference", "instance", "formtype", "cellnumber", "selectiontype"]
    data = [
        [49900001031, 0, "0006", 674, "C"],
        [49900001530, 0, "0006", 805, "P"],
        [49900001601, 0, "0001", 117, "C"],
        [49900001601, 1, "0001", 117, "C"],
        [49900003099, 0, "0006", 41, "L"],
    ]
    df = pd.DataFrame(columns=columns, data=data)
    return df


def test_clean_thousands_comma(cellno_mapper_df):
    """Test for clean_thousands_comma function."""
    columns = ["cell_no", "UNI_Count", "uni_employment", "uni_turnover"]
    expected_data = [
        [41, 87577, 27, 22867],
        [117, 13147, 1188, 1838],
        [674, 23, 5757, 15284],
        [805, 14, 28154, 8848],
        [888, 9, 185, 8558],
    ]
    expected_result = pd.DataFrame(data=expected_data, columns=columns)

    actual_result = clean_thousands_comma(cellno_mapper_df, columns[1:])
    # Ignore data types when comparing as we have different types of int
    pd.testing.assert_frame_equal(actual_result, expected_result, check_dtype=False)


@pytest.fixture(scope="module")
def expected_output(self):
    """Expected output for joining cellno mapper test."""
    columns = [
        "reference",
        "instance",
        "formtype",
        "cellnumber",
        "selectiontype",
        "UNI_Count",
    ]
    data = [
        [49900001031, 0, "0006", 674, "C", 23],
        [49900001530, 0, "0006", 805, "P", 14],
        [49900001601, 0, "0001", 117, "C", None],
        [49900001601, 1, "0001", 117, "C", None],
        [49900003099, 0, "0006", 41, "L", 87577],
    ]


# testing cellno_unit_dict
def test_cellno_unit_dict():
    """Test for cellno_unit_dict function."""
    # Mock data

    # Call cellno_unit_dict function
    actual_result = cellno_unit_dict(mock_data)

    # Defined expected result
    expected_result = {1: 8757, 2: 1314, 3: 23, 4: 14, 5: 9}

    assert actual_result == expected_result


def test_mapper_df():
    """Sample mapper for testing."""
    columns = ["ruref", "ultfoc"]
    data = [
        ["abc", "AB"],
        ["def", "EF"],
        ["ghi", "IJ"],
        ["jkl", "MN"],
        ["mno", "QR"],
        ["pqr", None],
    ]
    return pd.DataFrame(data=data, columns=columns)
