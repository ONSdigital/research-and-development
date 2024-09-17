import pandas as pd
import numpy as np

from src.outlier_detection.manual_outliers import apply_manual_outliers


class TestManualOutliers:
    """Unit tests for apply_manual_outliers function."""

    def input_data(self):
        """Input dataframe for the three different scenarios."""
        data = {
            "reference": [1, 2, 3],
            "instance": [0, 0, 0],
            "auto_outlier": [False, False, True],
            "manual_outlier": [True, np.nan, np.nan],
            "auto_override_outlier_status": [np.nan, np.nan, False],
        }

        input_data = pd.DataFrame(data)
        return input_data

    def output_data(self):
        """Output dataframe for the three different scenarios."""
        data = {
            "reference": [1, 2, 3],
            "instance": [0, 0, 0],
            "auto_outlier": [False, False, True],
            "manual_outlier": [True, np.nan, np.nan],
            "auto_override_outlier_status": [np.nan, np.nan, False],
            "outlier": [True, False, False],
        }

        output_data = pd.DataFrame(data)
        return output_data

    def test_apply_manual_outliers(self):
        """Test for flag_outliers function."""
        input_data = self.input_data()
        output_data = self.output_data()

        df_result = apply_manual_outliers(input_data)

        pd.testing.assert_frame_equal(df_result, output_data, check_dtype=False)
