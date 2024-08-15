"""Utility functions for the freezing module."""
from datetime import datetime
from typing import Tuple

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


def validate_main_config(config: dict) -> Tuple[bool, bool, bool, bool]:
    """Validate the four main config parameters of the freezing module.

    Args:
        config (dict): The pipeline config.

    Raises:
        ValueError: Raised if multiple pipeline run options are True.

    Returns:
        Tuple[bool, bool, bool, bool]: The main freezing config settings.
    """
    run_with_snapshot_until_freezing = config["global"]["run_with_snapshot_until_freezing"]
    load_updated_snapshot_for_comparison = config["global"]["load_updated_snapshot_for_comparison"]
    run_updates_and_freeze = config["global"]["run_updates_and_freeze"]
    run_frozen_data = config["global"]["run_frozen_data"]
    values = [
        run_with_snapshot_until_freezing,
        load_updated_snapshot_for_comparison,
        run_updates_and_freeze,
        run_frozen_data
    ]
    if len([val for val in values if val==True]) > 1:
        raise ValueError(
            "Only one type of pipeline run is allowed (freezing). Please update"
            " the user config."
        )
    return tuple(values)
