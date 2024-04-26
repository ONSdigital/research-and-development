"""Tests fpr intram_by_itl.py."""
# Standard Library Imports
import pytest
import os
import pathlib
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

    def get_test_data(self, itl: str) -> pd.DataFrame:
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
        data_1 = self.get_test_data(itl=1)
        data_2 = self.get_test_data(itl=2)
        # assert column name changes
        data_1 = rename_itl(data_1, 1)
        data_2 = rename_itl(data_2, 2)
        data_1_missing = rename_itl(data_1.copy(), 2) # no changes
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
                ["Area Code (ITL1)", "Region (ITL1)"]
            )
        ), "ITL1 columns did not remain the same."


class TestOutputIntramByItl(object):
    """Tests for output_intram_by_itl."""

    @pytest.fixture(scope="function")
    def gb_input_data(self):
        """GB input data for output_intram_by_itl tests."""
        columns = [
            "postcodes_harmonised", "formtype", "211"
        ]
        data = [
            ['YO25 6TH', "0006", 337266.6667],
            ['CF33 6NU', "0006", 14061.6883],
            ['NP44 3HQ', "0006", 345523.98],
            ['W4   5YA', "0001", 0.0],
            ['GU30 7RP', "0001", 200.0],
            ['TA20 2GB', "0001", 1400.0],
            ['TA21 8NL', "0001", 134443.0],
            ['TA21 8NL', "0001", 15463.5],
            ['SP10 3SD', "0006", 0.0],
            ['SP10 3SD', "0001", 12345678.0]
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df


    @pytest.fixture(scope="function")
    def ni_input_data(self) -> pd.DataFrame:
        """UK input data for output_intram_by_itl tests."""
        columns = ["formtype", "211"]
        data = [
            ["0003", 213.0],
            ["0003",  25.0],
            ["0003",  75.0],
            ["0003", 167.0]
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df
    

    @pytest.fixture(scope="function")
    def postcode_mapper(self) -> pd.DataFrame:
        """Postcode mapper for output_intram_by_itl tests."""
        columns = ['pcd2', 'itl']
        data = [
            ['CF33 6NU', 'W06000013'],
            ['GU30 7RP', 'E07000085'],
            ['NP44 3HQ', 'W06000020'],
            ['SP10 3SD', 'E07000093'],
            ['TA20 2GB', 'E07000189'],
            ['TA21 8NL', 'E07000246'],
            ['W4   5YA', 'E09000018'],
            ['YO25 6TH', 'E06000011']
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df
    

    @pytest.fixture(scope="function")
    def itl_mapper(self) -> pd.DataFrame:
        """ITL mapper for output_intram_by_itl tests."""
        columns = ["LAU121CD", "ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"]
        data = [
            ['E06000011', 'TLE1', 'East Yorkshire and Northern Lincolnshire',
                'TLE', 'Yorkshire and The Humber'],
            ['E09000018', 'TLI7', 'Outer London - West and North West', 'TLI',
                'London'],
            ['E07000189', 'TLK2', 'Dorset and Somerset', 'TLK',
                'South West (England)'],
            ['E07000246', 'TLK2', 'Dorset and Somerset', 'TLK',
                'South West (England)'],
            ['W06000020', 'TLL1', 'West Wales and The Valleys', 'TLL',
                'Wales'],
            ['W06000013', 'TLL1', 'West Wales and The Valleys', 'TLL',
                'Wales'],
            ['E07000085', 'TLJ3', 'Hampshire and Isle of Wight', 'TLJ',
                'South East (England)'],
            ['E07000093', 'TLJ3', 'Hampshire and Isle of Wight', 'TLJ',
                'South East (England)']
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df
    

    @pytest.fixture(scope="function")
    def gb_itl1_output(self) -> pd.DataFrame:
        """Expected output for GB - ITL1."""
        columns = ["Area Code (ITL1)", "Region (ITL1)", "2022 Total q211"]
        data = [
            ['TLE', 'Yorkshire and The Humber', 337266.6667],
            ['TLI', 'London', 0.0],
            ['TLJ', 'South East (England)', 12345878.0],
            ['TLK', 'South West (England)', 151306.5],
            ['TLL', 'Wales', 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df
    

    @pytest.fixture(scope="function")
    def gb_itl2_output(self) -> pd.DataFrame:
        """Expected output for GB - ITL2."""
        columns = ["Area Code (ITL2)", "Region (ITL2)", "2022 Total q211"]
        data = [
            ['TLE1', 'East Yorkshire and Northern Lincolnshire', 337266.6667],
            ['TLI7', 'Outer London - West and North West', 0.0],
            ['TLJ3', 'Hampshire and Isle of Wight', 12345878.0],
            ['TLK2', 'Dorset and Somerset', 151306.5],
            ['TLL1', 'West Wales and The Valleys', 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df


    @pytest.fixture(scope="function")
    def uk_itl1_output(self) -> pd.DataFrame:
        """Expected output for UK - ITL1."""
        columns = ["Area Code (ITL1)", "Region (ITL1)", "2022 Total q211"]
        data = [
            ['TLE', 'Yorkshire and The Humber', 337266.6667],
            ['TLI', 'London', 0.0],
            ['TLJ', 'South East (England)', 12345878.0],
            ['TLK', 'South West (England)', 151306.5],
            ['TLL', 'Wales', 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df
    

    @pytest.fixture(scope="function")
    def uk_itl2_output(self) -> pd.DataFrame:
        """Expected output for UK - ITL2."""
        columns = ["Area Code (ITL2)", "Region (ITL2)", "2022 Total q211"]
        data = [
            ['TLE1', 'East Yorkshire and Northern Lincolnshire', 337266.6667],
            ['TLI7', 'Outer London - West and North West', 0.0],
            ['TLJ3', 'Hampshire and Isle of Wight', 12345878.0],
            ['TLK2', 'Dorset and Somerset', 151306.5],
            ['TLL1', 'West Wales and The Valleys', 359585.6683],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df


    def create_dummy_config(self, path: pathlib.Path):
        """Create dummy config to provide output paths"""
        config = {
            "global": {"network_or_hdfs": "network"}, # no impact for test
            "network_paths": {"output_path": path}
        }
        return config


    def setup_output_dir(
            self,
            path: pathlib.Path,
            region: str,
        ) -> list:
        """Create output directories for outputs to be saved to."""
        paths = []
        for i in [1, 2]:
            new_dir = os.path.join(path, f"output_intram_{region}_itl{i}")
            os.mkdir(new_dir)
            paths.append(new_dir)
        return paths
    

    def get_dfs_from_paths(self, paths: list) -> list:
        """Get the df's from a list of directories."""
        dfs = {}
        for path in paths:
            files = [
                os.path.join(path, fpath) for fpath in os.listdir(path) if 
                fpath.endswith(".csv")
                ]
            for file in files:
                dfs[os.path.basename(file)] = pd.read_csv(file)
        return dfs


    def test_output_intram_by_itl_gb(
            self, 
            tmp_path,
            gb_input_data,
            postcode_mapper,
            itl_mapper,
            write_csv_func,
            gb_itl1_output,
            gb_itl2_output
        ):
        """General tests for output_intram_by_itl with no NI data."""
        config = self.create_dummy_config(tmp_path)
        output_paths = self.setup_output_dir(tmp_path, "gb")
        # save outputs
        output_intram_by_itl(
            df_gb=gb_input_data,
            config=config,
            write_csv=write_csv_func,
            run_id=1,
            postcode_mapper=postcode_mapper,
            itl_mapper=itl_mapper
        )
        output_dfs = self.get_dfs_from_paths(output_paths)
        keys = list(output_dfs.keys())
        pd.testing.assert_frame_equal(output_dfs[keys[0]], gb_itl1_output)
        assert output_dfs[keys[0]].equals(gb_itl1_output), (
            "GB ITL1 Output Not as Expected."
        )
        assert output_dfs[keys[1]].equals(gb_itl2_output), (
            "GB ITL2 Output Not as Expected."
        )


    def test_output_intram_by_itl_uk(
        self, 
        tmp_path,
        gb_input_data,
        postcode_mapper,
        itl_mapper,
        write_csv_func,
        ni_input_data,
        uk_itl1_output,
        uk_itl2_output
    ):
        """General tests for output_intram_by_itl with NI data."""
        config = self.create_dummy_config(tmp_path)
        output_paths = self.setup_output_dir(tmp_path, "uk")
        # save outputs
        output_intram_by_itl(
            df_gb=gb_input_data,
            config=config,
            write_csv=write_csv_func,
            run_id=1,
            postcode_mapper=postcode_mapper,
            itl_mapper=itl_mapper,
            df_ni=ni_input_data
        )
        output_dfs = self.get_dfs_from_paths(output_paths)
        keys = list(output_dfs.keys())
        assert output_dfs[keys[0]].equals(uk_itl1_output), (
            "UK ITL1 Output Not as Expected."
        )
        assert output_dfs[keys[1]].equals(uk_itl2_output), (
            "UK ITL2 Output Not as Expected."    
        )

        
