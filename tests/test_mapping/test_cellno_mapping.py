"""Test for cellno mapper function"""
import pytest
import pandas as pd

from src.mapping.cellno_mapping import clean_validate_cellno_mapper


@pytest.fixture(scope="module")
def cellno_mapper_df():
    columns = ["cell_no", "UNI_Count", "uni_employment", "uni_turnover"]
    data = [
        [41, 87577, 27, 22867],
        [117, 13147, 1188, 1838],
        [674, 23, 5757, 15284],
        [805, 14, 28154, 8848],
        [888, 9, 185, 8558],
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


def test_clean_validate_cellno_mapper_failure_range(cellno_mapper_df):
    """Test for clean_validate_cellno_mapper function in the case of failure.
    Failure due to the incorrect range in cellno items in the mapper.
    """
    with pytest.raises(ValueError):
        test_expected_num_df = cellno_mapper_df
        clean_validate_cellno_mapper(test_expected_num_df, 5)  # wrong range


def test_clean_validate_cellno_mapper_success(cellno_mapper_df):
    """Test for clean_validate_cellno_mapper function in the case of success."""
    success_df = cellno_mapper_df.copy().replace(888, 817)

    result = clean_validate_cellno_mapper(success_df, 5)

    assert result.shape == (5, 2)
    assert result["cellnumber"].nunique() == 5
    assert result["cellnumber"].between(1, 817).all()


@pytest.fixture(scope="module")
def expected_clean_mapper(cellno_mapper_df):
    """Expected output for clean_validate_cellno_mapper test."""
    columns = ["cellnumber", "uni_count"]
    data = [
        [41, 87577],
        [117, 13147],
        [674, 23],
        [805, 14],
        [817, 9],
    ]
    df = pd.DataFrame(data=data, columns=columns)
    return df


def test_clean_validate_cellno_mapper_success(cellno_mapper_df, expected_clean_mapper):
    """Test for clean_validate_cellno_mapper function in the case of success."""
    success_df = cellno_mapper_df.copy().replace(888, 817)

    result = clean_validate_cellno_mapper(success_df, 5)

    pd.testing.assert_frame_equal(result, expected_clean_mapper, check_dtype=False)
