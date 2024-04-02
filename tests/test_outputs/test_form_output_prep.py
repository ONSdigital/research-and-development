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
            "201"
            "202",
            "211",
            "602", 
            "604", 
            "a_weight", 
            "formtype",
            "pg_numeric"
        ]
        data = [
            [49900000404, 0, 202212, 'AA', np.nan, np.nan, 100.0, 'Yes',
                6.727272727272728, 6, 40],
            [49900000404, 1, 202212, 'AA', 3146383.9595, 4628363.6364, 100.0,
                'Yes', 6.727272727272728, 6, 40],
            [49900000404, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'Yes',
                6.727272727272728, 6, 40],
            [49900000408, 0, 202212, 'AA', np.nan, np.nan, 100.0, 'No', 1.0, 6, 40],
            [49900000408, 1, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, 6, 40],
            [49900000408, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, 6, 40],
            [49900000576, 1, 202212, 'AA', 0.0, 0.0, 100.0, np.nan, 1.0, 1, 40],
            [49900000960, 0, 202212, 'AA', np.nan, np.nan, np.nan, 'No', 1.0, 1, 40],
            [49900000960, 1, 202212, 'AA', np.nan, 0.0, np.nan, 'No', 1.0, 1, 40],
            [49900001029, 1, 202212, 'I', 119376.5, 244543.1667, 100.0, np.nan,
                1.0, 1, 14]
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
            "201" #pg
            "202",
            "211",
            "602", 
            "604", 
            "a_weight", 
            "formtype",
            "pg_numeric"
        ]
        data = [
            [49900000404, 1, 202212, 'AA', 467705.7237114774, 688000.0, 100.0,
                'Yes', 6.727272727272728, 6, 40],
            [49900000404, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'Yes',
                6.727272727272728, 6, 40],
            [49900000408, 1, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, 6, 40],
            [49900000408, 2, 202212, 'AA', 0.0, 0.0, 100.0, 'No', 1.0, 6, 40],
            [49900000576, 1, 202212, 'AA', 0.0, 0.0, 100.0, np.nan, 1.0, 1, 40],
            [49900000960, 1, 202212, 'AA', np.nan, 0.0, np.nan, 'No', 1.0, 1, 40],
            [49900001029, 1, 202212, 'I', 119376.5, 244543.16666666663, 100.0,
                np.nan, 1.0, 1, 14],
            [49900001031, 1, 202212, 'U', 0.0, 0.0, 100.0, 'No', 49.0, 6, 30],
            [49900001031, 2, 202212, 'U', 0.0, 0.0, 100.0, 'No', 49.0, 6, 30],
            [49900001032, 1, 202212, 'I', 119376.5, 244543.16666666663, 100.0,
                np.nan, 1.0, 1, 14]
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
            "foc",
            "407",
            "201", #pg
            "rusic" #sic
        ]
        data = [
            [49900001577, 1, 'GB ', 0.72, np.nan, 10130],
            [49900006060, 1, 'GB ', 0.6, np.nan, 20120],
            [49900008752, 1, 'GB ', 0.72, np.nan, 46420],
            [49900008762, 1, 'GB ', 6.9, np.nan, 22230],
            [49900008767, 1, 'GB ', 0.72, np.nan, 14190],
            [49900008807, 1, 'GB ', 0.0, np.nan, 46390],
            [49900008914, 1, 'US ', 0.0, np.nan, 17219],
            [49900008968, 1, 'GB ', 1.44, np.nan, 46420],
            [49900009016, 1, 'GB ', 4.0, np.nan, 71129],
            [49900009078, 1, 'GB ', 0.72, np.nan, 23610]
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
    
    

    
    