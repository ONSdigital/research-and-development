"""Test for cellno mapper function"""
import pytest
import pandas as pd

from src.mapping.cellno_mapper import (
    clean_thousands_comma,
    join_cellno_mapper,
    check_expected_number_of_cellnumbers,
    check_cellno_range,
    clean_validate_cellno_mapper,
)


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
def full_input_df():
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

    result = cellno_mapper_df.copy()
    for col in columns[1:]:
        result[col] = result[col].str.replace(",", "").astype(int)
    # Ignore data types when comparing as we have different types of int
    pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)


@pytest.fixture(scope="module")
def expected_output():
    """Expected output for joining cellno mapper test."""
    columns = [
        "reference",
        "instance",
        "formtype",
        "cellnumber",
        "selectiontype",
        "uni_count",
    ]
    data = [
        [49900001031, 0, "0006", 674, "C", 23],
        [49900001530, 0, "0006", 805, "P", 14],
        [49900001601, 0, "0001", 117, "C", 13147],
        [49900001601, 1, "0001", 117, "C", 13147],
        [49900003099, 0, "0006", 41, "L", 87577],
    ]
    df = pd.DataFrame(columns=columns, data=data)
    return df


def test_join_cellno_mapper(full_input_df, cellno_mapper_df, expected_output):
    """Test for join_cellno_mapper function."""
    cellno_mapper_df = clean_validate_cellno_mapper(cellno_mapper_df, 5)
    actual_output = join_cellno_mapper(full_input_df, cellno_mapper_df)
    pd.testing.assert_frame_equal(actual_output, expected_output, check_dtype=False)


def test_check_expected_number_of_cellnumbers_failure(cellno_mapper_df):
    """Test for check_expected_number_of_cellnumbers function in the case of failure."""
    with pytest.raises(ValueError):
        check_expected_number_of_cellnumbers(cellno_mapper_df, 4)


def test_check_expected_number_of_cellnumbers_success(cellno_mapper_df):
    """Test for check_expected_number_of_cellnumbers function in the case of success."""
    check_expected_number_of_cellnumbers(cellno_mapper_df, 5)


def test_check_cellno_range_failure(cellno_mapper_df):
    """Test for check_cellno_range function in the case of failure."""
    with pytest.raises(ValueError):
        check_cellno_range(cellno_mapper_df)  # cell_no 888 is not in the expected range


def test_check_cellno_range_success(cellno_mapper_df):
    """Test for check_cellno_range function in the case of success."""
    success_df = cellno_mapper_df.copy().replace(888, 817)
    check_cellno_range(success_df)


def test_clean_validate_cellno_mapper_failure(cellno_mapper_df):
    """Test for clean_validate_cellno_mapper function in the case of failure."""
    with pytest.raises(ValueError):
        clean_validate_cellno_mapper(
            cellno_mapper_df
        )  # cell_no 888 is not in the expected range


def test_clean_validate_cellno_mapper_success(cellno_mapper_df):
    """Test for clean_validate_cellno_mapper function in the case of success."""
    success_df = cellno_mapper_df.copy().replace(888, 817)

    expected_columns = ["cellnumber", "uni_count"]
    expected_data = [
        [41, 87577],
        [117, 13147],
        [674, 23],
        [805, 14],
        [817, 9],
    ]

    expected_result = pd.DataFrame(data=expected_data, columns=expected_columns)
    result = clean_validate_cellno_mapper(success_df, 5)

    assert result.equals(
        expected_result
    ), "clean_validate_cellno_mapper not behaving as expected"
