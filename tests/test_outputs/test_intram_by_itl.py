"""Tests for intram_by_itl.py."""
# Standard Library Imports
import pytest
import os
import pathlib
from datetime import datetime

# Third Party Imports
import pandas as pd
import numpy as np

# Local Imports
from src.outputs.intram_by_itl import rename_itl, aggregate_itl

class TestRenameItl(object):
    """Tests for renamed_itl."""

    def get_test_data(self, itl: str) -> pd.DataFrame:
        """Test data for rename_itl tests."""
        columns = [f"ITL{itl}21CD", f"ITL{itl}21NM"]
        data = [[0, 0]]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def test_rename_itl(self):
        """General tests for rename_itl."""
        # read in sample df's
        data_1 = self.get_test_data(itl=1)
        data_2 = self.get_test_data(itl=2)
        # assert column name changes
        data_1 = rename_itl(data_1, 1, 2022)
        data_2 = rename_itl(data_2, 2, 2022)
        data_1_missing = rename_itl(data_1.copy(), 2, 2022)  # no changes

        assert np.array_equal(
            data_1.columns, ["Area Code (ITL1)", "Region (ITL1)"]
        ), "ITL1 columns not renamed"
        assert np.array_equal(
            data_2.columns, ["Area Code (ITL2)", "Region (ITL2)"]
        ), "ITL2 columns not renamed"
        assert np.array_equal(
            data_1_missing.columns, ["Area Code (ITL1)", "Region (ITL1)"]
        ), "ITL1 columns did not remain the same."

class TestAggregateItl(object):
    """Tests for aggregate_itl."""

    @pytest.fixture(scope="function")
    def ni_input_data(self) -> pd.DataFrame:
        """UK input data for output_intram_by_itl tests."""
        columns = ["formtype", "211"]
        data = [["0003", 213.0], ["0003", 25.0], ["0003", 75.0], ["0003", 167.0]]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    @pytest.fixture(scope="function")
    def gb_input_data(self):
        """A dataframe that joins the ITL columns to the input data."""
        columns = ["postcodes_harmonised", "formtype", "211", "pcd2", "itl", "ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"]
        data = [
            ["YO25 6TH", "0006", 337266.6667, "E06000011", "TLE", "TLE1", "East Yorkshire and Northern Lincolnshire", "TLE", "Yorkshire and The Humber"],
            ["CF33 6NU", "0006", 14061.6883, "W06000013", "TLL", "TLL1", "West Wales and The Valleys", "TLL", "Wales"],
            ["NP44 3HQ", "0006", 345523.98, "W06000020", "TLL", "TLL1", "West Wales and The Valleys", "TLL", "Wales"],
            ["W4   5YA", "0001", 0.0, "E09000018", "TLI", "TLI7", "Outer London - West and North West", "TLI", "London"],
            ["GU30 7RP", "0001", 200.0, "E07000085", "TLJ", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"],
            ["TA20 2GB", "0001", 1400.0, "E07000189", "TLK", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["TA21 8NL", "0001", 134443.0, "E07000246", "TLK", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["TA21 8NL", "0001", 15463.5, "E07000246", "TLK", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["SP10 3SD", "0006", 0.0, "E07000093", "TLJ", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"],
            ["SP10 3SD", "0001", 12345678.0, "E07000093", "TLJ", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    @pytest.fixture(scope="function")
    def gb_itl1_output(self) -> pd.DataFrame:
        """Expected output for GB - ITL1."""
        columns = ["Area Code (ITL1)", "Region (ITL1)", "Year 2022 Total q211"]
        data = [
            ["TLE", "Yorkshire and The Humber", 337266.6667],
            ["TLI", "London", 0.0],
            ["TLJ", "South East (England)", 12345878.0],
            ["TLK", "South West (England)", 151306.5],
            ["TLL", "Wales", 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def gb_itl2_output(self) -> pd.DataFrame:
        """Expected output for GB - ITL2."""
        columns = ["Area Code (ITL2)", "Region (ITL2)", "Year 2022 Total q211"]
        data = [
            ["TLE1", "East Yorkshire and Northern Lincolnshire", 337266.6667],
            ["TLI7", "Outer London - West and North West", 0.0],
            ["TLJ3", "Hampshire and Isle of Wight", 12345878.0],
            ["TLK2", "Dorset and Somerset", 151306.5],
            ["TLL1", "West Wales and The Valleys", 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def uk_itl1_output(self) -> pd.DataFrame:
        """Expected output for UK - ITL1."""
        columns = ["Area Code (ITL1)", "Region (ITL1)", "Year 2022 Total q211"]
        data = [
            ["TLE", "Yorkshire and The Humber", 337266.6667],
            ["TLI", "London", 0.0],
            ["TLJ", "South East (England)", 12345878.0],
            ["TLK", "South West (England)", 151306.5],
            ["TLL", "Wales", 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def uk_itl2_output(self) -> pd.DataFrame:
        """Expected output for UK - ITL2."""
        columns = ["Area Code (ITL2)", "Region (ITL2)", "Year 2022 Total q211"]
        data = [
            ["TLE1", "East Yorkshire and Northern Lincolnshire", 337266.6667],
            ["TLI7", "Outer London - West and North West", 0.0],
            ["TLJ3", "Hampshire and Isle of Wight", 12345878.0],
            ["TLK2", "Dorset and Somerset", 151306.5],
            ["TLL1", "West Wales and The Valleys", 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def test_aggregate_itl_gb(
        self,
        gb_input_data,
        ni_input_data,
        gb_itl1_output,
        gb_itl2_output,
    ):
        """Test for aggregate_itl with GB data."""
        print(gb_input_data)
        print(ni_input_data)
        print(gb_itl1_output)
        print(gb_itl2_output)


        itl1, itl2 = aggregate_itl(gb_input_data, ni_input_data, 2022)
        itl1 = itl1.round(4)
        itl2 = itl2.round(4)
        assert itl1.equals(gb_itl1_output), "GB ITL1 Output Not as Expected."
        assert itl2.equals(gb_itl2_output), "GB ITL2 Output Not as Expected."


    def test_aggregate_itl_uk(
        self,
        gb_input_data,
        ni_input_data,
        uk_itl1_output,
        uk_itl2_output,
    ):
        """Test fpr aggregate_itl with NI data."""
        itl1, itl2 = aggregate_itl(gb_input_data, ni_input_data, 2022, uk_output=True)

        itl1 = itl1.round(4)
        itl2 = itl2.round(4)
        assert itl1.equals(uk_itl1_output), "UK ITL1 Output Not as Expected."
        assert itl2.equals(uk_itl2_output), "UK ITL2 Output Not as Expected."
