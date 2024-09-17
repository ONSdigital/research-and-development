import logging
from typing import Callable

import pandas as pd
import numpy as np

from src.outputs.outputs_helpers import create_period_year

from src.construction.construction_helpers import replace_values_in_construction

def postcode_data_construction(
    construction_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    construction_logger: logging.Logger
    ) -> pd.DataFrame:
    """Run postcode construction on GB data.
    This process is different from the all data construction that happens
    before mapping.

    Args:
        construction_df (pd.DataFrame): The construction data
        snapshot_df (pd.DataFrame): The snapshot data
        construction_logger (logging.Logger): The logger for the construction.

    Returns:
        pd.DataFrame: The snapshot data with the constructed values
    """
    # Drop columns without constructed values
    construction_df = construction_df.dropna(axis="columns", how="all")

    # Create period_year column (NI already has it)
    snapshot_df = create_period_year(snapshot_df)
    construction_df = create_period_year(construction_df)

    # Make a copy of the snapshot
    updated_snapshot_df = snapshot_df.copy()

    # Add flags to indicate row was constructed
    construction_df["is_constructed"] = True

    updated_snapshot_df, construction_df = replace_values_in_construction(updated_snapshot_df, construction_df)

    updated_snapshot_df = updated_snapshot_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    construction_logger.info(f"Postcode construction edited {construction_df.shape[0]} rows.")

    return updated_snapshot_df
