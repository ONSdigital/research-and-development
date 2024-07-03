import pytest
import pandas as pd
import numpy as np

# Local Imports
from src.mapping.mapping_helpers import update_ref_list


class TestUpdateRefList(object):
    """Tests for update_ref_list."""

    @pytest.fixture(scope="function")
    def full_input_df(self):
        """Main input data for update_ref_list tests."""
        columns = ["reference", "instance", "formtype", "cellnumber", "selectiontype"]
        data = [
            [49900001031, 0, "0006", 674, "A"],
            [49900001530, 0, "0006", 805, "B"],
            [49900001601, 0, "0001", 117, "C"],
            [49900001601, 1, "0001", 117, "D"],
            [49900003099, 0, "0006", 41, "E"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def ref_list_input(self):
        """Reference list df input for update_ref_list tests."""
        columns = ["reference", "cellnumber", "selectiontype", "formtype"]
        data = [[49900001601, 117, "C", "1"]]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """Expected output for update_ref_list tests."""
        columns = ["reference", "instance", "formtype", "cellnumber", "selectiontype"]
        data = [
            [49900001031, 0, "0006", 674, "A"],
            [49900001530, 0, "0006", 805, "B"],
            [49900001601, 0, "0001", 817, "L"],
            [49900001601, 1, "0001", 817, "L"],
            [49900003099, 0, "0006", 41, "E"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def test_update_ref_list(self, full_input_df, ref_list_input, expected_output):
        """General tests for update_ref_list."""
        output = update_ref_list(full_input_df, ref_list_input)
        assert output.equals(
            expected_output
        ), "update_ref_list not behaving as expected"

    def test_update_ref_list_raises(self, full_input_df, ref_list_input):
        """Test the raises in update_ref_list."""
        # add a non valid reference
        ref_list_input.loc[1] = [34567123123, 117, "C", "1"]
        error_msg = r"The following references in the reference list mapper are.*"
        with pytest.raises(ValueError, match=error_msg):
            update_ref_list(full_input_df, ref_list_input)
