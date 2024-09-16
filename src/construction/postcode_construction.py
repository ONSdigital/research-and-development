import logging
from typing import Callable

import pandas as pd
import numpy as np

from src.outputs.outputs_helpers import create_period_year

from src.construction.construction_helpers import (
    prepare_forms_gb,
    clean_construction_type,
    add_constructed_nonresponders,
    remove_short_to_long_0,
    finalise_forms_gb,
)
from src.construction.construction_validation import (
    validate_short_to_long,
    validate_construction_references,
)

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

    # Update the values with the constructed ones
    construction_df.set_index(
        [
            "reference",
            "instance",
            "period_year",
        ],
        inplace=True,
    )
    updated_snapshot_df.set_index(
        [
            "reference",
            "instance",
            "period_year",
        ],
        inplace=True,
    )
    updated_snapshot_df.update(construction_df)
    updated_snapshot_df.reset_index(inplace=True)

    updated_snapshot_df = updated_snapshot_df.astype(
        {"reference": "Int64", "instance": "Int64", "period_year": "Int64"}
    )

    updated_snapshot_df = updated_snapshot_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    construction_logger.info(f"Construction edited {construction_df.shape[0]} rows.")

    return updated_snapshot_df
