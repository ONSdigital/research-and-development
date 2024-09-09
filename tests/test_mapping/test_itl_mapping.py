import numpy as np
import pandas as pd
import pytest

from src.mapping.itl_mapping import join_itl_regions

@pytest.fixture(scope="module")
def config() -> dict:
    """A dummy config for running join_itl_regions tests."""
    config = {
        "mappers": {
            "geo_cols": ["ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"],
            "gb_itl": "LAU121CD",
            "ni_itl": "N92000002",
        }
    }
    return config

class TestJoinITLRegions(object):
    """Tests for join_itl_regions."""

    @pytest.fixture(scope="function")
    def gb_input_data(self):
        """GB input data for output_intram_by_itl tests."""
        columns = ["postcodes_harmonised", "formtype", "211"]
        data = [
            ["YO25 6TH", "0006", 337266.6667],
            ["CF33 6NU", "0006", 14061.6883],
            ["NP44 3HQ", "0006", 345523.98],
            ["W4   5YA", "0001", 0.0],
            ["GU30 7RP", "0001", 200.0],
            ["TA20 2GB", "0001", 1400.0],
            ["TA21 8NL", "0001", 134443.0],
            ["TA21 8NL", "0001", 15463.5],
            ["SP10 3SD", "0006", 0.0],
            ["SP10 3SD", "0001", 12345678.0],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    @pytest.fixture(scope="function")
    def ni_input_data(self) -> pd.DataFrame:
        """UK input data for output_intram_by_itl tests."""
        columns = ["formtype", "211"]
        data = [["0003", 213.0], ["0003", 25.0], ["0003", 75.0], ["0003", 167.0]]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    @pytest.fixture(scope="function")
    def postcode_mapper(self) -> pd.DataFrame:
        """Postcode mapper for output_intram_by_itl tests."""
        columns = ["pcd2", "itl"]
        data = [
            ["CF33 6NU", "W06000013"],
            ["GU30 7RP", "E07000085"],
            ["NP44 3HQ", "W06000020"],
            ["SP10 3SD", "E07000093"],
            ["TA20 2GB", "E07000189"],
            ["TA21 8NL", "E07000246"],
            ["W4   5YA", "E09000018"],
            ["YO25 6TH", "E06000011"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def itl_mapper(self) -> pd.DataFrame:
        """ITL mapper for output_intram_by_itl tests."""
        columns = ["LAU121CD", "ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"]
        data = [
            ["E06000011", "TLE1", "East Yorkshire and Northern Lincolnshire", "TLE", "Yorkshire and The Humber"],
            ["E09000018", "TLI7", "Outer London - West and North West", "TLI", "London"],
            ["E07000189", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["E07000246", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["W06000020", "TLL1", "West Wales and The Valleys", "TLL", "Wales"],
            ["W06000013", "TLL1", "West Wales and The Valleys", "TLL", "Wales"],
            ["E07000085", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"],
            ["E07000093", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"]
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def expected_gb_output(self):
        """Expected output for join_itl_regions tests."""
        columns = [
            "postcodes_harmonised",
            "formtype",
            "211",
            "itl",
            "ITL221CD",
            "ITL221NM",
            "ITL121CD",
            "ITL121NM",
        ]
        data = [
            ["YO25 6TH", "0006", 337266.6667, "E06000011", "TLE1", "East Yorkshire and Northern Lincolnshire", "TLE", "Yorkshire and The Humber"],
            ["CF33 6NU", "0006", 14061.6883, "W06000013", "TLL1", "West Wales and The Valleys", "TLL", "Wales"],
            ["NP44 3HQ", "0006", 345523.98, "W06000020", "TLL1", "West Wales and The Valleys", "TLL", "Wales"],
            ["W4   5YA", "0001", 0.0, "E09000018", "TLI7", "Outer London - West and North West", "TLI", "London"],
            ["GU30 7RP", "0001", 200.0, "E07000085", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"],
            ["TA20 2GB", "0001", 1400.0, "E07000189", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["TA21 8NL", "0001", 134443.0, "E07000246", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["TA21 8NL", "0001", 15463.5, "E07000246", "TLK2", "Dorset and Somerset", "TLK", "South West (England)"],
            ["SP10 3SD", "0006", 0.0, "E07000093", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"],
            ["SP10 3SD", "0001", 12345678.0, "E07000093", "TLJ3", "Hampshire and Isle of Wight", "TLJ", "South East (England)"],
        ]

        expected_output = pd.DataFrame(data=data, columns=columns)
        return expected_output

    @pytest.fixture(scope="function")
    def expected_ni_output(self):
        """Expected output for join_itl_regions tests."""
        columns = ["formtype", "211", "itl"]
        data = [
            ["0003", 213.0, "N92000002"],
            ["0003", 25.0, "N92000002"],
            ["0003", 75.0, "N92000002"],
            ["0003", 167.0, "N92000002"],
        ]
        expected_output = pd.DataFrame(data=data, columns=columns)
        return expected_output

    def test_join_itl_regions(
        self,
        gb_input_data,
        ni_input_data,
        postcode_mapper,
        itl_mapper,
        expected_gb_output,
        expected_ni_output,
        config
    ):
        """General tests for join_itl_regions."""
        input_data = (gb_input_data, ni_input_data)
        gb_output, ni_output = join_itl_regions(input_data, postcode_mapper, itl_mapper, config)

        assert gb_output.equals(
            expected_gb_output
        ), "join_itl_regions not behaving as expected."

        assert ni_output.equals(
            expected_ni_output
        ), "join_itl_regions not behaving as expected."
