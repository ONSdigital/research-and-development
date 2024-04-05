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
            "pg_numeric"
        ]
        data = [
            [39900000404, 0, 202212, 'AA', np.nan, np.nan, 100.0, 'Yes', 6.0, "0006", 40],
            [39900000404, 1, 202212, 'AA', 3146383.0, 4628363.0, 100.0, 'Yes', 6.0, "0006", 40],
            [39900000404, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'Yes', 6.0, "0006", 40],
            [39900000408, 0, 202212, 'AA', np.nan, np.nan, 100.0, 'No', 1.0, "0006", 40],
            [39900000408, 1, 202212, 'AA', 0.0, 1222.0, 100.0, 'No', 1.0, "0006", 40], # 604 No and 211 > 0
            [39900000408, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, "0006", 40],
            [39900000576, 1, 202212, 'AA', 0.0, 0.0, 100.0, np.nan, 1.0, "0001", 40],
            [39900000960, 0, 202212, 'AA', np.nan, np.nan, np.nan, 'No', 1.0, "0001", 40],
            [39900000960, 1, 202212, 'AA', np.nan, 0.0, np.nan, 'No', 1.0, "0001", 40],
            [39900001029, 1, 202212, 'I', 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14]
        ]
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
            "201", #pg
            "202",
            "211",
            "602", 
            "604", 
            "a_weight", 
            "formtype",
            "pg_numeric"
        ]
        data = [
            [39900000404, 1, 202212, 'AA', 467705.0, 688000.0, 100.0, 'Yes', 6.0, "0006", 40],
            [39900000404, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'Yes', 6.0, "0006", 40],
            [39900000408, 1, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, "0006", 40],
            [39900000408, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, "0006", 40],
            [39900000576, 1, 202212, 'AA', 0.0, 0.0, 100.0, np.nan, 1.0, "0001", 40],
            [39900000960, 1, 202212, 'AA', np.nan, 0.0, np.nan, 'No', 1.0, "0001", 40],
            [39900001029, 1, 202212, 'I', 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14],
            [39900001031, 1, 202212, 'U', 0.0, 0.0, 100.0, 'No', 49.0, "0006", 30],
            [39900001031, 2, 202212, 'U', 0.0, 0.0, 100.0, 'No', 49.0, "0006", 30],
            [39900001032, 1, 202212, 'I', 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14]
        ]
        weighted_df = pd.DataFrame(data=data, columns=columns)
        return weighted_df
    

    @pytest.fixture(scope="function")
    def ni_full_responses(self) -> pd.DataFrame:
        """'ni_full_responses' input for form_output_prep.
        
        NOTE: This is a subset of columns for testing purposes
        """
        columns = [
            "reference", 
            "instance",
            "period_year",
            "foc",
            "407",
            "201", #pg
            "rusic" #sic
        ]
        data = [
            [39900001577, 1, 2021, 'GB ', 0.72, np.nan, 10130],
            [39900006060, 1, 2021, 'GB ', 0.6, 9, 17219],
            [39900008752, 1, 2021, 'GB ', 0.72, np.nan, 46420],
            [39900008767, 1, 2021, 'GB ', 0.72, np.nan, 14190],
            [39900008807, 1, 2021, 'GB ', 0.0, np.nan, 46390],
            [39900008914, 1, 2021, 'US ', 0.0, np.nan, 17219],
            [39900008968, 1, 2021, 'GB ', 1.44, np.nan, 46420],
            [39900009016, 1, 2021, 'GB ', 4.0, np.nan, 71129],
            [39900009078, 1, 2021, 'GB ', 0.72, np.nan, 23610]
        ]
        ni_full_responses = pd.DataFrame(data=data, columns=columns)
        return ni_full_responses
    

    @pytest.fixture(scope="function")
    def sic_pg_num(self) -> pd.DataFrame:
        """'sic_pg_num' input for form_output_prep.
        
        NOTE: This is a subset of columns for testing purposes
        """
        columns = [
            "Unnamed: 0",
            "SIC 2007_CODE",
            "2016 > Form PG"
        ]
        data = [
            [100, 10130, 3],
            [198, 14190, 6],
            [223, 17219, 9],
            [253, 20120, 12],
            [311, 23610, 15],
            [772, 46390, 40],
            [774, 46420, 40],
            [1088, 71129, 50],
       ]
        sic_pg_num = pd.DataFrame(data=data, columns=columns)
        return sic_pg_num
    

    @pytest.fixture(scope="function")
    def pg_num_alpha(self) -> pd.DataFrame:
        """'pg_num_alpha' input for form_output_prep.
        
        NOTE: This is a subset of columns for testing purposes
        """
        columns = [
            "Unnamed: 0",
            "pg_numeric",
            "pg_alpha"
        ]
        data = [
            [2, 3, 'C'],
            [5, 6, 'D'],
            [8, 9, 'E'],
            [11, 12, 'G'],
            [14, 15, 'J'],
            [39, 40, 'AA'],
            [48, 50, 'AD'],
       ]
        pg_num_alpha = pd.DataFrame(data=data, columns=columns)
        return pg_num_alpha
    

    @pytest.fixture(scope="function")
    def ni_expected(self) -> pd.DataFrame:
        columns = [
            'reference',
            'instance',
            "period_year",
            'ultfoc',
            '407',
            '201',
            'rusic',
            'a_weight',
            '604',
            'form_status',
            '602',
            'formtype',
            'pg_numeric'
        ]
        # pg_numeric as float to match outputs 
        data = [
            [39900001577, 1, 2021, 'GB ', 0.72, 'C', 10130, 1, 'Yes', 600, 100.0, "0003", 3.0],
            [39900006060, 1, 2021, 'GB ', 0.6, 'E', 17219, 1, 'Yes', 600, 100.0, "0003", 9.0],
            [39900008752, 1, 2021, 'GB ', 0.72, 'AA', 46420, 1, 'Yes', 600, 100.0, "0003", 40.0],
            [39900008767, 1, 2021, 'GB ', 0.72, 'D', 14190, 1, 'Yes', 600, 100.0, "0003", 6.0],
            [39900008807, 1, 2021, 'GB ', 0.0, 'AA', 46390, 1, 'Yes', 600, 100.0, "0003", 40.0],
            [39900008914, 1, 2021, 'US ', 0.0, 'E', 17219, 1, 'Yes', 600, 100.0, "0003", 9.0],
            [39900008968, 1, 2021, 'GB ', 1.44, 'AA', 46420, 1, 'Yes', 600, 100.0, "0003", 40.0],
            [39900009016, 1, 2021, 'GB ', 4.0, 'AD', 71129, 1, 'Yes', 600, 100.0, "0003", 50.0],
            [39900009078, 1, 2021, 'GB ', 0.72, 'J', 23610, 1, 'Yes', 600, 100.0, "0003", 15.0]
        ]
        ni_expected = pd.DataFrame(data=data, columns=columns)
        ni_expected["201"] = ni_expected["201"].astype("category")
        ni_expected["pg_numeric"] = ni_expected["pg_numeric"].astype("category")
        return ni_expected
    

    @pytest.fixture(scope="function")
    def full_output_expected(self) -> pd.DataFrame:
        columns = [
            'reference',
            'instance',
            'period',
            '201',
            '202',
            '211',
            '602',
            '604',
            'a_weight',
            'formtype', 
            'pg_numeric', 
            'period_year'
        ]
        data = [
            [39900000404, 0, 202212, 'AA', np.nan, np.nan, 100.0, 'Yes', 6.0, "0006", 40, 2022],
            [39900000404, 1, 202212, 'AA', 3146383.0, 4628363.0, 100.0, 'Yes', 6.0, "0006", 40, 2022],
            [39900000404, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'Yes', 6.0, "0006", 40, 2022],
            [39900000408, 0, 202212, 'AA', np.nan, np.nan, 100.0, 'No', 1.0, "0006", 40, 2022],
            [39900000408, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, "0006", 40, 2022],
            [39900000576, 1, 202212, 'AA', 0.0, 0.0, 100.0, np.nan, 1.0, "0001", 40, 2022],
            [39900000960, 0, 202212, 'AA', np.nan, np.nan, np.nan, 'No', 1.0, "0001", 40, 2022],
            [39900000960, 1, 202212, 'AA', np.nan, 0.0, np.nan, 'No', 1.0, "0001", 40, 2022],
            [39900001029, 1, 202212, 'I', 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14, 2022]
        ]
        full_outputs = pd.DataFrame(data=data, columns=columns)
        return full_outputs
    

    @pytest.fixture(scope="function")
    def tau_expected(self) -> pd.DataFrame:
        columns = [
            'reference',
            'instance',
            'period',
            '201',
            '202',
            '211',
            '602',
            '604',
            'a_weight',
            'formtype',
            'pg_numeric',
            'period_year',
            'ultfoc',
            '407',
            'rusic',
            'form_status']
        data = [
            [39900001031, 2, 202212.0, 'U', 0.0, 0.0, 100.0, 'No', 49.0, "0006", 30.0, 2022, np.nan, np.nan, np.nan, np.nan],
            [39900001032, 1, 202212.0, 'I', 119376.5, 244543.0, 100.0, np.nan, 1.0, "0001", 14, 2022, np.nan, np.nan, np.nan, np.nan],
            [39900001577, 1, np.nan, 'C', np.nan, np.nan, 100.0, 'Yes', 1.0, "0003", 3.0, 2021, 'GB ', 0.72, 10130.0, 600.0],
            [39900006060, 1, np.nan, 'E', np.nan, np.nan, 100.0, 'Yes', 1.0, "0003", 9.0, 2021, 'GB ', 0.6, 17219.0, 600.0],
            [39900008752, 1, np.nan, 'AA', np.nan, np.nan, 100.0, 'Yes', 1.0, "0003", 40.0, 2021, 'GB ', 0.72, 46420.0, 600.0],
            [39900008767, 1, np.nan, 'D', np.nan, np.nan, 100.0, 'Yes', 1.0, "0003", 6.0, 2021, 'GB ', 0.72, 14190.0, 600.0]
        ]
        full_outputs = pd.DataFrame(data=data, columns=columns)
        return full_outputs


    def test_form_output_prep_with_ni(
            self,
            estimated_df,
            weighted_df,
            ni_full_responses,
            sic_pg_num,
            pg_num_alpha,
            ni_expected,
            full_output_expected,
            tau_expected
        ):
        """General tests for form_output_prep."""
        output = form_output_prep(estimated_df, weighted_df, ni_full_responses, pg_num_alpha, sic_pg_num)
        # assert the function outputs as expected
        assert isinstance(output, tuple), (
            "Output of form_output_prep is not a tuple."
            )
        assert len(output) == 3, (
            "Output of form_output_prep is unexpected length."
        )
        (ni_outputs, full_outputs, tau_outputs) = output
        # assert ni outputs are correct
        pd.testing.assert_frame_equal(
            left=ni_outputs,
            right=ni_expected,
        )
        # assert full outputs are correct
        pd.testing.assert_frame_equal(
            left=full_outputs.reset_index(drop=True),
            right=full_output_expected,
        )
        # assert combined (tau) outputs are correct
        pd.testing.assert_frame_equal(
            left=tau_outputs.iloc[7:13].reset_index(drop=True),
            right=tau_expected,
        )


    def test_form_output_prep_no_ni(
            self,
            estimated_df,
            weighted_df,
            sic_pg_num,
            pg_num_alpha,
            full_output_expected
        ):
        """General tests for form_output_prep (no NI data)."""
        output = form_output_prep(estimated_df, weighted_df, None, pg_num_alpha, sic_pg_num)
        # assert the function outputs as expected
        assert isinstance(output, tuple), (
            "Output of form_output_prep is not a tuple."
            )
        assert len(output) == 3, (
            "Output of form_output_prep is unexpected length."
        )
        (ni_outputs, full_outputs, tau_outputs) = output
        # assert NI outputs are correct (empty df)
        pd.testing.assert_frame_equal(
            left=ni_outputs,
            right=pd.DataFrame(),
        )
        # assert full outputs are correct
        pd.testing.assert_frame_equal(
            left=full_outputs.reset_index(drop=True),
            right=full_output_expected,
        )

    
    