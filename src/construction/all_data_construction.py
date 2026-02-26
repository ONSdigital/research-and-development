import logging

import pandas as pd
import numpy as np

from src.utils.breakdown_validation import run_breakdown_validation

from src.construction.construction_helpers import (
    prepare_forms_gb,
    clean_construction_type,
    add_constructed_nonresponders,
    finalise_forms_gb,
    replace_values_in_construction,
)
from src.construction.construction_validation import (
    validate_short_to_long,
    validate_construction_references,
)


def all_data_construction(  # noqa: C901
    construction_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    construction_logger: logging.Logger,
    config: dict,
    is_northern_ireland: bool = False,
) -> pd.DataFrame:
    """Run all data construction on the GB or NI data.
    This process is different from the postcode only construction that happens
    after imputation.

    Args:
        construction_df (pd.DataFrame): The construction data
        snapshot_df (pd.DataFrame): The snapshot data
        construction_logger (logging.Logger): The logger for the construction
        is_northern_ireland (bool, optional): Whether the data is for Northern Ireland.
            Defaults to False.

    Returns:
        pd.DataFrame: The snapshot data with the constructed values
    """
    # to ensure compatibility, change short_to_long to construction_type
    # short_to_long used for 2022
    if "short_to_long" in construction_df.columns:
        construction_df.rename(
            columns={"short_to_long": "construction_type"}, inplace=True
        )
        construction_df.loc[
            construction_df["construction_type"].isin([True]), "construction_type"
        ] = "short_to_long"

    # clean construction type column
    if "construction_type" in construction_df.columns:
        # clean values
        construction_df.construction_type = construction_df.construction_type.apply(
            clean_construction_type
        )
        # standardise nulls
        construction_df.construction_type = construction_df.construction_type.replace(
            ["<NA>", "<na>", ""], np.nan
        )
        # Validate
        invalid_mask = (
            ~construction_df["construction_type"].isin(["short_to_long", "new"])
            & construction_df["construction_type"].notna()
        )
        if invalid_mask.any():
            e = (
                f"Invalid value(s) for construction_type: "
                f"{construction_df.loc[invalid_mask, 'construction_type'].unique()}. "
                "Expected only 'short_to_long', 'new', or null."
            )
            raise ValueError(e)

    if not is_northern_ireland:
        validate_short_to_long(construction_df, construction_logger)

        # validate the references passed in construction
        validate_construction_references(
            construction_df=construction_df,
            snapshot_df=snapshot_df,
            logger=construction_logger,
        )

    # Drop columns without constructed values
    construction_df = construction_df.copy().dropna(axis="columns", how="all")
    if "statusencoded" in construction_df.columns:
        construction_df["statusencoded"] = (
            construction_df["statusencoded"].astype(str).str.split(".").str[0]
        )

    # Make a copy of the snapshot
    updated_snapshot_df = snapshot_df.copy()

    # Add flags to indicate whether a row was constructed or should be imputed
    updated_snapshot_df.loc[:, "is_constructed"] = False
    updated_snapshot_df.loc[:, "force_imputation"] = False
    construction_df.loc[:, "is_constructed"] = True
    if "force_imputation" not in construction_df.columns:
        construction_df.loc[:, "force_imputation"] = False
    else:
        construction_df.loc[:, "force_imputation"] = construction_df.loc[
            :, "force_imputation"
        ].fillna(False)

    # Run GB specific actions
    if not is_northern_ireland:
        updated_snapshot_df, construction_df = prepare_forms_gb(
            updated_snapshot_df, construction_df
        )

    # NI data has no instance but needs an instance of 1
    if is_northern_ireland:
        construction_df["instance"] = 1

    # Add constructed non-responders (i.e. new rows) to df
    if "construction_type" in construction_df.columns:
        if "new" in construction_df["construction_type"].dropna().unique():
            updated_snapshot_df, construction_df = add_constructed_nonresponders(
                updated_snapshot_df, construction_df
            )

    updated_snapshot_df, construction_df = replace_values_in_construction(
        updated_snapshot_df, construction_df
    )

    # Run GB specific actions
    if not is_northern_ireland:
        updated_snapshot_df = finalise_forms_gb(updated_snapshot_df)

    updated_snapshot_df = updated_snapshot_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    # Check breakdowns
    if not is_northern_ireland:
        updated_snapshot_df = run_breakdown_validation(
            updated_snapshot_df, config, check="constructed"
        )

    construction_logger.info(f"Construction edited {construction_df.shape[0]} rows.")

    return updated_snapshot_df
