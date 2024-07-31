"""The main file for the construction module."""
import logging
from typing import Callable

import pandas as pd
import numpy as np

from src.construction.construction_read_validate import read_validate_construction_files

from src.construction.construction_helpers import (
    prepare_forms_gb,
    clean_construction_type,
    add_constructed_nonresponders,
    remove_short_to_long_0,
    finalise_forms_gb,
)
from src.construction.construction_validation import (
    concat_construction_dfs,
    validate_short_to_long,
    validate_construction_references,
)

construction_logger = logging.getLogger(__name__)


def run_construction(  # noqa: C901
    snapshot_df: pd.DataFrame,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    is_northern_ireland: bool = False,
) -> pd.DataFrame:
    """Run the construction module.

    The construction module reads the construction file provided by users and
    any non-null values are used to overwrite the snapshot dataframe. This is
    intended for users to do ad hoc updates of data without having to provide
    a new snapshot.

    Args:
        snapshot_df (pd.DataFrame): The staged and vaildated snapshot data.
        config (dict): The pipeline configuration
        check_file_exists (callable): Function to check if file exists. This
            will be the hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs
            or network version depending on settings.
        is_northern_ireland (bool): If true, do construction on Northern Ireland
            data instead of England, Wales and Scotland data.
    Returns:
        updated_snapshot_df (pd.DataFrame): As main_snapshot but with records
            amended and flags added to mark whether a record was constructed.
    """
    if is_northern_ireland:
        run_construction = config["global"]["run_ni_construction"]
        run_postcode_construction = False
    else:
        run_construction = config["global"]["run_all_data_construction"]
        run_postcode_construction = config["global"]["run_postcode_construction"]

    # Skip this module if not needed
    if not run_construction and not run_postcode_construction:
        construction_logger.info("Skipping Construction...")
        return snapshot_df

    construction_df, pc_construction_df = read_validate_construction_files(
        config,
        check_file_exists,
        read_csv,
        is_northern_ireland,
        run_construction,
        run_postcode_construction,
    )

    # merge construction files
    construction_df = concat_construction_dfs(
        df1=construction_df,
        df2=pc_construction_df,
        validate_dupes=True,
        logger=construction_logger,
    )

    # to ensure compatibility, change short_to_long to construction_type
    # short_to_long used for 2022
    if "short_to_long" in construction_df.columns:
        construction_df.rename(columns={"short_to_long": "construction_type"}, inplace=True)
        construction_df.loc[construction_df["construction_type"] == True, "construction_type"] = "short_to_long"

    # clean construction type column
    if "construction_type" in construction_df.columns:
        construction_df.construction_type = construction_df.construction_type.apply(
            lambda x: clean_construction_type(x)
        )
        # validate that 'construction_type' is valid
        valid_types = ["short_to_long", "new", np.NaN]
        if False in list(construction_df.construction_type.isin(valid_types)):
            raise ValueError(
                f"Invalid value for construction_type. Expected one of {valid_types}"
            )

    if not is_northern_ireland:
        validate_short_to_long(construction_df, construction_logger)

    # validate the references passed in construction
    validate_construction_references(
        construction_df=construction_df,
        snapshot_df=snapshot_df,
        logger=construction_logger,
    )

    # Drop columns without constructed values
    construction_df = construction_df.dropna(axis="columns", how="all")

    # Make a copy of the snapshot
    updated_snapshot_df = snapshot_df.copy()

    # Add flags to indicate whether a row was constructed or should be imputed
    updated_snapshot_df["is_constructed"] = False
    updated_snapshot_df["force_imputation"] = False
    construction_df["is_constructed"] = True

    # Run GB specific actions
    if not is_northern_ireland:
        updated_snapshot_df, construction_df = prepare_forms_gb(
            updated_snapshot_df, construction_df
        )

    # NI data has no instance but needs an instance of 1
    if is_northern_ireland:
        construction_df["instance"] = 1

    # Add constructed non-responders (i.e. new rows) to df
    if "new" in construction_df["construction_type"].values:
        updated_snapshot_df, construction_df = add_constructed_nonresponders(
            updated_snapshot_df, construction_df
        )

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

    if "construction_type" in construction_df.columns:
        if "short_to_long" in construction_df["construction_type"].values:
            construction_df.reset_index(inplace=True)
            updated_snapshot_df = remove_short_to_long_0(
                updated_snapshot_df, construction_df
            )

    # Run GB specific actions
    if not is_northern_ireland:
        updated_snapshot_df = finalise_forms_gb(updated_snapshot_df)

    updated_snapshot_df = updated_snapshot_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    construction_logger.info(f"Construction edited {construction_df.shape[0]} rows.")

    return updated_snapshot_df
