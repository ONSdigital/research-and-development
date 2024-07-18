"""Tests for 'form_output_prep.py'."""
# Standard Library Imports
import pytest

# Third Party Imports
import pandas as pd
import numpy as np

# Local Imports
from src.outputs.form_output_prep import form_output_prep


class TestFormOutputPrep(object):
    """Tests for form_output_prep."""

    @pytest.fixture(scope="function")
    def estimated_df(self) -> pd.DataFrame:
        """'estimated_df' input for form_output_prep.

        NOTE: This is a subset of columns for testing purposes
        """
        columns = [
            "reference",
            "instance",
            "period",
            "201",
            "202",
            "211",
            "602",
            "604",
            "a_weight",
            "formtype",
            "pg_numeric",
        ]

        data = [ # noqa
            [39900000404, 0, 202212, "AA", np.nan, np.nan, 100.0, "Yes", 6.0, "0006", 40],  # noqa
            [39900000404, 1, 202212, "AA", 3146383.0, 4628363.0, 100.0, "Yes", 6.0, "0006", 40],  # noqa
            [39900000404, 2, 202212, "AA", 0.0, 0.0, 100.0, "Yes", 6.0, "0006", 40],  # noqa
            [39900000408, 0, 202212, "AA", np.nan, np.nan, 100.0, "No", 1.0, "0006", 40],  # noqa
            [39900000408, 1, 202212, "AA", 0.0, 1222.0, 100.0, "No", 1.0, "0006", 40],  # noqa
            [39900000408, 2, 202212, "AA", 0.0, 0.0, 100.0, "No", 1.0, "0006", 40],  # noqa
            [39900000576, 1, 202212, "AA", 0.0, 0.0, 100.0, np.nan, 1.0, "0001", 40],  # noqa
            [39900000960, 0, 202212, "AA", np.nan, np.nan, np.nan, "No", 1.0, "0001", 40],  # noqa
            [39900000960, 1, 202212, "AA", np.nan, 0.0, np.nan, "No", 1.0, "0001", 40],  # noqa
            [39900001029, 1, 202212, "I", 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14],  # noqa
        ]  # noqa

        estimated_df = pd.DataFrame(data=data, columns=columns)
        return estimated_df

    @pytest.fixture(scope="function")
    def weighted_df(self) -> pd.DataFrame:
        """'weighted_df' input for form_output_prep.

        NOTE: This is a subset of columns for testing purposes
        """
        columns = [
            "reference",
            "instance",
            "period",
            "201",  # pg
            "202",
            "211",
            "602",
            "604",
            "a_weight",
            "formtype",
            "pg_numeric",
        ]
        data = [ # noqa
            [39900000404, 1, 202212, "AA", 467705.0, 688000.0, 100.0, "Yes", 6.0, "0006", 40],  # noqa
            [39900000404, 2, 202212, "AA", 0.0, 0.0, 100.0, "Yes", 6.0, "0006", 40],  # noqa
            [39900000408, 1, 202212, "AA", 0.0, 0.0, 100.0, "No", 1.0, "0006", 40],  # noqa
            [39900000408, 2, 202212, "AA", 0.0, 0.0, 100.0, "No", 1.0, "0006", 40],  # noqa
            [39900000576, 1, 202212, "AA", 0.0, 0.0, 100.0, np.nan, 1.0, "0001", 40],  # noqa
            [39900000960, 1, 202212, "AA", np.nan, 0.0, np.nan, "No", 1.0, "0001", 40],  # noqa
            [39900001029, 1, 202212, "I", 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14],  # noqa
            [39900001031, 1, 202212, "U", 0.0, 0.0, 100.0, "No", 49.0, "0006", 30],  # noqa
            [39900001031, 2, 202212, "U", 0.0, 0.0, 100.0, "No", 49.0, "0006", 30],  # noqa
            [39900001032, 1, 202212, "I", 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14]  # noqa
        ]  # noqa
        weighted_df = pd.DataFrame(data=data, columns=columns)
        return weighted_df

    @pytest.fixture(scope="function")
    def full_output_expected(self) -> pd.DataFrame:
        columns = [
            "reference",
            "instance",
            "period",
            "201",
            "202",
            "211",
            "602",
            "604",
            "a_weight",
            "formtype",
            "pg_numeric",
            "period_year",
        ]
        data = [ # noqa
            [39900000404, 0, 202212, "AA", np.nan, np.nan, 100.0, "Yes", 6.0, "0006", 40, 2022],  # noqa
            [39900000404, 1, 202212, "AA", 3146383.0, 4628363.0, 100.0, "Yes", 6.0, "0006", 40, 2022],  # noqa
            [39900000404, 2, 202212, "AA", 0.0, 0.0, 100.0, "Yes", 6.0, "0006", 40, 2022],  # noqa
            [39900000408, 0, 202212, "AA", np.nan, np.nan, 100.0, "No", 1.0, "0006", 40, 2022],  # noqa
            [39900000408, 2, 202212, "AA", 0.0, 0.0, 100.0, "No", 1.0, "0006", 40, 2022],  # noqa
            [39900000576, 1, 202212, "AA", 0.0, 0.0, 100.0, np.nan, 1.0, "0001", 40, 2022],  # noqa
            [39900000960, 0, 202212, "AA", np.nan, np.nan, np.nan, "No", 1.0, "0001", 40, 2022],  # noqa
            [39900000960, 1, 202212, "AA", np.nan, 0.0, np.nan, "No", 1.0, "0001", 40, 2022],  # noqa
            [39900001029, 1, 202212, "I", 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14, 2022]  # noqa
        ]  # noqa
        full_outputs = pd.DataFrame(data=data, columns=columns)
        return full_outputs

    @pytest.fixture(scope="function")
    def tau_expected(self) -> pd.DataFrame:
        columns = [
            "reference",
            "instance",
            "period",
            "201",
            "202",
            "211",
            "602",
            "604",
            "a_weight",
            "formtype",
            "pg_numeric",
            "period_year",
            "ultfoc",
            "407",
            "rusic",
            "form_status",
        ]
        data = [ # noqa
            [39900001031, 2, 202212.0, "U", 0.0, 0.0, 100.0, "No", 49.0, "0006", 30.0, 2022, np.nan, np.nan, np.nan, np.nan],  # noqa
            [39900001032, 1, 202212.0, "I", 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14, 2022, np.nan, np.nan, np.nan, np.nan],  # noqa
            [39900001577, 1, np.nan, "C", np.nan, np.nan, 100.0, "Yes", 1.0, "0003", 3.0, 2021, "GB ", 0.72, 10130.0, 600.0],  # noqa
            [39900006060, 1, np.nan, "E", np.nan, np.nan, 100.0, "Yes", 1.0, "0003", 9.0, 2021, "GB ", 0.6, 17219.0, 600.0],  # noqa
            [39900008752, 1, np.nan, "AA", np.nan, np.nan, 100.0, "Yes", 1.0, "0003", 40.0, 2021, "GB ", 0.72, 46420.0, 600.0],  # noqa
            [39900008767, 1, np.nan, "D", np.nan, np.nan, 100.0, "Yes", 1.0, "0003", 6.0, 2021, "GB ", 0.72, 14190.0, 600.0]  # noqa
        ]  # noqa

        full_outputs = pd.DataFrame(data=data, columns=columns)
        return full_outputs
