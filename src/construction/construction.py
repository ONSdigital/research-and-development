"""The main file for the construction module."""
import logging
import pandas as pd
from typing import Callable

from src.staging.validation import validate_data_with_schema
from src.staging.staging_helpers import postcode_topup
from src.outputs.outputs_helpers import create_period_year

construction_logger = logging.getLogger(__name__)


def run_construction(
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
        path_type = "construction_file_path_ni"
        schema_path = "./config/construction_ni_schema.toml"
    else:
        run_construction = config["global"]["run_construction"]
        path_type = "construction_file_path"
        schema_path = "./config/construction_schema.toml"

    # Skip this module if not needed
    if run_construction is False:
        construction_logger.info("Skipping Construction...")
        return snapshot_df

    # Check the construction file exists and has records, then read it
    network_or_hdfs = config["global"]["network_or_hdfs"]
    paths = config[f"{network_or_hdfs}_paths"]
    construction_file_path = paths[path_type]
    construction_logger.info(f"Reading construction file {construction_file_path}")
    construction_file_exists = check_file_exists(construction_file_path)
    if construction_file_exists:
        try:
            construction_df = read_csv(construction_file_path)
        except pd.errors.EmptyDataError:
            construction_logger.warning(
                f"Construction file {construction_file_path} is empty, skipping..."
            )
            return snapshot_df
    else:
        construction_logger.info(
            "Construction file not found, skipping construction..."
        )
        return snapshot_df

    # Make a copy of the snapshot
    updated_snapshot_df = snapshot_df.copy()

    # Validate construction file and drop columns without constructed values
    validate_data_with_schema(construction_df, schema_path)
    construction_df = construction_df.dropna(axis="columns", how="all")

    # Add flags to indicate whether a row was constructed or should be imputed
    updated_snapshot_df["is_constructed"] = False
    updated_snapshot_df["force_imputation"] = False
    construction_df["is_constructed"] = True

    # Run GB specific actions
    if not is_northern_ireland:

        # Convert formtype to "0001" or "0006" (NI doesn't have a formtype until outputs)
        if "formtype" in construction_df.columns:
            construction_df["formtype"] = construction_df["formtype"].apply(convert_formtype)

        # Prepare the short to long form constructions, if any (N/A to NI)
        if "short_to_long" in construction_df.columns:
            updated_snapshot_df = prepare_short_to_long(
                updated_snapshot_df, construction_df
            )
        # Create period_year column (NI already has it)
        updated_snapshot_df = create_period_year(updated_snapshot_df)
        construction_df = create_period_year(construction_df)
        # Set instance=1 so longforms with status 'Form sent out' match correctly
        form_sent_condition = (updated_snapshot_df.formtype == "0001") & (
            updated_snapshot_df.status == "Form sent out"
        )
        updated_snapshot_df.loc[form_sent_condition, "instance"] = 1
        # Set instance=0 so shortforms with status 'Form sent out' match correctly
        form_sent_condition = (updated_snapshot_df.formtype == "0006") & (
            updated_snapshot_df.status == "Form sent out"
        )
        updated_snapshot_df.loc[form_sent_condition, "instance"] = 0

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
            updated_snapshot_df[col] = updated_snapshot_df[col].apply(postcode_topup)

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


def prepare_short_to_long(updated_snapshot_df, construction_df):
    """Create addional instances for short to long construction"""
    # Check which references are going to converted to long forms
    short_to_long_refs = construction_df.loc[
        construction_df["short_to_long"] == True, "reference"
    ].unique()
    # Create conversion df
    short_to_long_df = updated_snapshot_df[
        updated_snapshot_df["reference"].isin(short_to_long_refs)
    ]

    # Copy instance 0 record to create instance 1 and instance 2
    short_to_long_df1 = short_to_long_df.copy()
    short_to_long_df1["instance"] = 1
    short_to_long_df2 = short_to_long_df.copy()
    short_to_long_df2["instance"] = 2

    # Add new instances to the updated snapshot df
    updated_snapshot_df = pd.concat(
        [updated_snapshot_df, short_to_long_df1, short_to_long_df2]
    )
    return updated_snapshot_df


def convert_formtype(formtype_value):
    if pd.notnull(formtype_value):
        if formtype_value == "1" or formtype_value == "1.0" or formtype_value == "0001":
            return "0001"
        elif formtype_value == "6" or formtype_value == "6.0" or formtype_value == "0006":
            return "0006"
        else:
            return None
    else:
        return None
