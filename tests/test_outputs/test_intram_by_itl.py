"""Tests fpr intram_by_itl.py."""
# Standard Library Imports
import pytest
import os
from datetime import datetime

# Third Party Imports
import pandas as pd
import numpy as np

# Local Imports
from src.outputs.intram_by_itl import (
    save_detailed_csv,
    rename_itl,
    output_intram_by_itl
)


class TestSaveDetailedCSV(object):
    """Tests for save_detailed_csv."""

    @pytest.fixture(scope="function")
    def test_frame(self) -> pd.DataFrame:
        """Minimal df for testing."""
        df = pd.DataFrame(
            {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        )
        return df


    def test_save_detailed_csv_defence(
            self, 
            tmp_path, 
            write_csv_func, 
            test_frame
        ):
        """Defensive tests for save_detailed_csv."""
        # save test dataframe
        save_detailed_csv(
                df=test_frame,
                dir=tmp_path,
                title="test_df",
                run_id=1,
                write_csv=write_csv_func,
                overwrite=True

            )
        # ensure error is raised
        msg = "File .*test_df.* already exists.*"
        with pytest.raises(FileExistsError, match=msg):
            save_detailed_csv(
                df=test_frame,
                dir=tmp_path,
                title="test_df",
                run_id=1,
                write_csv=write_csv_func,
                overwrite=False
            )


    def test_save_detailed_csv(
            self, 
            tmp_path,
            test_frame,
            write_csv_func
        ):
        """General tests for save_detailed_csv."""
        save_detailed_csv(
            df=test_frame,
            dir=tmp_path,
            title="test_df",
            run_id=40,
            write_csv=write_csv_func,
            overwrite=False
        )
        # create expected filename
        date = datetime.now().strftime("%Y-%m-%d")
        save_name = f"test_df_{date}_v40.csv"
        fpath = os.path.join(tmp_path, save_name)
        # assert file exists
        assert os.path.exists(fpath), (
            "save_detailed_csv not saving out dataframe."
        )
        # assert data is correct
        assert pd.read_csv(fpath).equals(test_frame), (
            "Saved data not as expected."
        )


class TestRenameItl(object):
    """Tests for renamed_itl."""
    def test_data(self, itl) -> pd.DataFrame:
        """Test data for rename_itl tests."""
        columns = [f"ITL{itl}21CD", f"ITL{itl}21NM"]
        data = [
            [0, 0]
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df
    

    def test_rename_itl(self):
        """General tests for rename_itl."""
        # read in sample df's
        data_1 = self.test_data(itl=1)
        data_2 = self.test_data(itl=2)
        # assert column name changes
        data_1 = rename_itl(data_1, 1)
        data_2 = rename_itl(data_2, 2)
        data_1_missing = rename_itl(data_1, 2) # no changes
        assert (
            np.array_equal(
                data_1.columns,
                ["Area Code (ITL1)", "Region (ITL1)"]
            )
        ), "ITL1 columns not renamed"
        assert (
            np.array_equal(
                data_2.columns,
                ["Area Code (ITL2)", "Region (ITL2)"]
            )
        ), "ITL2 columns not renamed"
        assert (
            np.array_equal(
                data_1_missing.columns, 
                ["Area Code (ITL2)", "Region (ITL2)"]
            )
        ), "ITL1 columns did not remain the same."



