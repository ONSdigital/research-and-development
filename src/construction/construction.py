"""The main file for the construction module."""
import logging
import pandas as pd
from typing import Callable

from src.construction.construction_helpers import (
    read_construction_file,
    prepare_forms_gb
)
from src.construction.construction_validation import (
    check_for_duplicates
)
from src.staging.validation import validate_data_with_schema
from src.staging import postcode_validation as pcval

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
        schema_path = "./config/construction_ni_schema.toml"
        run_postcode_construction = False
    else:
        run_construction = config["global"]["run_all_data_construction"]
        run_postcode_construction = config["global"]["run_postcode_construction"]
        schema_path = "./config/all_data_construction_schema.toml"
        postcode_schema_path = "./config/postcode_construction_schema.toml"

    # Skip this module if not needed
    if run_construction is False:
        construction_logger.info("Skipping Construction...")
        return snapshot_df

    # Obtain construction paths 
    paths = config[f"construction_paths"]
    if is_northern_ireland:
        construction_file_path = paths["construction_file_path_ni"]
    else:
        construction_file_path = paths["all_data_construction_file_path"]
        postcode_construction_fpath = paths["postcode_construction_file_path"]

    # Check the construction file exists and has records, then read it
    construction_df = read_construction_file(
        path=construction_file_path,
        logger=construction_logger,
        read_csv_func=read_csv,
        file_exists_func=check_file_exists
    )
    if isinstance(construction_df, type(None)):
        return snapshot_df
    # read in postcode construction file
    if run_postcode_construction:
        pc_construction_df = read_construction_file(
            path=postcode_construction_fpath,
            logger=construction_logger,
            read_csv_func=read_csv,
            file_exists_func=check_file_exists
        )
        if isinstance(pc_construction_df, type(None)):
            run_postcode_construction = False

    # validate and merge schemas
    validate_data_with_schema(construction_df, schema_path)
    check_for_duplicates(
        df=construction_df, 
        columns=["reference", "instance"], 
        logger=construction_logger
    )

    if run_postcode_construction:
        validate_data_with_schema(pc_construction_df, postcode_schema_path)
        check_for_duplicates(
            df=pc_construction_df, 
            columns=["reference", "instance"], 
            logger=construction_logger
        )

    # Validate construction file and drop columns without constructed values
    construction_df = construction_df.dropna(axis="columns", how="all")

    # Make a copy of the snapshot
    updated_snapshot_df = snapshot_df.copy()

    # Add flags to indicate whether a row was constructed or should be imputed
    updated_snapshot_df["is_constructed"] = False
    updated_snapshot_df["force_imputation"] = False
    construction_df["is_constructed"] = True

    # Run GB specific actions
    if not is_northern_ireland:
        updated_snapshot_df, construction_df = (
            prepare_forms_gb(updated_snapshot_df, construction_df)
        )
        
    # NI data has no instance but needs an instance of 1
    if is_northern_ireland:
        construction_df["instance"] = 1

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

    # Run GB specific actions
    if not is_northern_ireland:
        # Long form records with a postcode in 601 use this as the postcode
        long_form_cond = ~updated_snapshot_df["601"].isnull()
        updated_snapshot_df.loc[
            long_form_cond, "postcodes_harmonised"
        ] = updated_snapshot_df["601"]

        # Short form records with nothing in 601 use referencepostcode instead
        short_form_cond = (updated_snapshot_df["601"].isnull()) & (
            ~updated_snapshot_df["referencepostcode"].isnull()
        )
        updated_snapshot_df.loc[
            short_form_cond, "postcodes_harmonised"
        ] = updated_snapshot_df["referencepostcode"]

        # Top up all new postcodes so they're all eight characters exactly
        postcode_cols = ["601", "referencepostcode", "postcodes_harmonised"]
        for col in postcode_cols:
            updated_snapshot_df[col] = updated_snapshot_df[col].apply(
                pcval.format_postcodes
            )

        # Reset shortforms with status 'Form sent out' to instance=None
        form_sent_condition = (updated_snapshot_df.formtype == "0006") & (
            updated_snapshot_df.status == "Form sent out"
        )
        updated_snapshot_df.loc[form_sent_condition, "instance"] = None

    updated_snapshot_df = updated_snapshot_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    construction_logger.info(f"Construction edited {construction_df.shape[0]} rows.")

    return updated_snapshot_df
