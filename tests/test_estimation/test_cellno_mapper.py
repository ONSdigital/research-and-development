"""Test for cellno mapper function"""
import pandas as pd
from src.mapping.cellno_mapper import cellno_unit_dict


# testing cellno_unit_dict
def test_cellno_unit_dict():
    # Mock data
    mock_data = pd.DataFrame(
        {
            "cell_no": [1, 2, 3, 4, 5],
            "UNI_Count": ["87,57", "13,14", "2,3", "14", "9"],
            "uni_employment": ["2,7", "11,8", "5,77", "28,14", "18,5"],
            "uni_turnover": ["22,67", "1,83", "15,24", "8,48", "8,55"],
        }
    )
    # Call cellno_unit_dict function
    actual_result = cellno_unit_dict(mock_data)

    # Defined expected result
    expected_result = {1: 8757, 2: 1314, 3: 23, 4: 14, 5: 9}

    assert actual_result == expected_result
