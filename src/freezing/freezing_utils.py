"""Utility functions for the freezing module."""
from datetime import datetime

import pandas as pd

from src.utils.defence import type_defence

def _add_last_frozen_column(
        frozen_df: pd.DataFrame,
        run_id: int
    ) -> pd.DataFrame:
    """Add the last_frozen column to staged data.

    Args:
        frozen_df (pd.DataFrame): The frozen data.
        run_id (int): The current run id.

    Returns:
        pd.DataFrame: A dataframe containing the updated last_frozen column.
    """
    type_defence(frozen_df, "frozen_df", pd.DataFrame)
    type_defence(run_id, "run_id", int)
    todays_date = datetime.today().strftime("%y-%m-%d")
    last_frozen = f"{todays_date}_v{str(run_id)}"
    frozen_df["last_frozen"] = last_frozen
    return frozen_df