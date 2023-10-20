"""Expansion imputation for the 2xx and 3xx questions.
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Union

import pandas as pd
from pandas.testing import assert_frame_equal

from src.imputation.expansion_imputation import run_expansion



class TestRunExpansion:
    """Unit tests for run_expansion function."""

    def test_calc_202_totals(self):
        """Test for flag_outliers function."""

        cols = [
            "211_trim",
            "305_trim",
            "imp_class",
            "status",
            "211",
            "305",
            "201",
            "202",
            "301",
            "302",
            "imp_marker",
            "211_imputed",
            "305_imputed",
            "instance",
            "reference",
        ]

        # Create a sample DataFrame for testing
        data = [
            # This part will need updated when this TODO is actioned: TODO: Fix datatypes and remove this temp-fix
            [False, False, "A_1", "Clear", np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, "TMI", 40, 1, 1, 1],
            # test nans in 211_trim are treated same as False 
            [np.nan, False, "A_1", "Clear", 50, 1, 5, 1, 1, 1, "TMI", 40, 1, 1, 2],
            # test Trues are filterred out from expansion process i.e. 211_trim will haves nans for imputed 10-21
            [True, False, "A_1", "Clear", 25, 1, 1, 1, 1, 1, "TMI", 40, 1, 1, 3],
            #Test that for different imp_classes (A_1 and A_2) with everything else the same (ref 4 and 8) gives same imputed 10-21
            [False, False, "A_2", "Clear", 1, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 4],
            # test nans in 211_trim are treated same as False 
            [np.nan, False, "A_1", "Clear", 50, 1, 20, 1, 1, 1, "TMI", 40, 1, 1, 5],
            [np.nan, False, "A_1", "Form sent out", 50, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 6],
            [True, False, "A_1", "Form sent out", 25, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 7], 
            [False, False, "A_2", "Clear", 1, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 8],
            [False, False, "A_1", "Form sent out", 1, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 9],
            [np.nan, False, "A_1", "Form sent out", 25, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 10],
        ]

        df = pd.DataFrame(data=data, columns=cols)

        # Define the target_variable_list for testing
        config = {"breakdowns":{"2xx": ["201", "202"], "3xx": ["301", "302"]}}

        # Call the function
        result_df = run_expansion(df, config)

        expected_cols = [
            "211_trim",
            "305_trim",
            "imp_class",
            "status",
            "211",
            "305",
            "201",
            "202",
            "301",
            "302",
            "imp_marker",
            "211_imputed",
            "305_imputed",
            "instance",
            "reference",
            "201_imputed",
            "202_imputed",
            "301_imputed",
            "302_imputed"
        ]

        expected_result_data = [
            [False, False, "A_1", "Clear", np.nan, np.nan, np.nan, np.nan, 0, 0, "TMI", 40, 1, 1, 1, np.nan, np.nan, 0, 0],
            [False, False, "A_1", "Clear", 50, 1, 5, 1, 1, 1, "TMI", 40, 1, 1, 2, 5, 1, 1, 1], 
            # TODO check why "301_imputed" and "302_imputed" aren't nans
            [True, False, "A_1", "Clear", 25, 1, 1, 1, 1, 1, "TMI", 40, 1, 1, 3, np.nan, np.nan, 1, 1],
            [False, False, "A_2", "Clear", 1, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 4, 1, 1, 1, 1],
            [False, False, "A_1", "Clear", 50, 1, 20, 1, 1, 1, "TMI", 40, 1, 1, 5, 20, 1, 1, 1],
            [False, False, "A_1", "Form sent out", 50, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 6, 10.0, 0.8, 1.0, 1.0],
            # TODO check why "301_imputed" and "302_imputed" aren't nans
            [True, False, "A_1", "Form sent out", 25, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 7, np.nan, np.nan, 1, 1],
            [False, False, "A_2", "Clear", 1, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 8, 1, 1, 1, 1],
            [False, False, "A_1", "Form sent out", 1, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 9, 10.0, 0.8, 1, 1],
            [False, False, "A_1", "Form sent out", 25, 1, 1, 1, 1, 1, "TMI", 1, 1, 1, 10, 10.0, 0.8, 1, 1],
        ]

        expected_result_df = pd.DataFrame(data=expected_result_data, columns=expected_cols)
        
        #convert from int32 to int64
        expected_result_df["301"] = expected_result_df["301"].astype(int)
        expected_result_df["302"] = expected_result_df["302"].astype(int)

        assert result_df.equals(expected_result_df)
