"""Useful utilities for the construction module."""
import pathlib
import logging
from typing import Union, Callable, Tuple

import pandas as pd
import numpy as np

from src.outputs.outputs_helpers import create_period_year
from src.staging import postcode_validation as pcval
from src.utils.helpers import convert_formtype


def read_construction_file(
    path: Union[str, pathlib.Path],
    logger: logging.Logger,
    read_csv_func: Callable,
    file_exists_func: Callable,
) -> pd.DataFrame:
    """Read in a construction file, with related logging.

    Args:
        path (Union[str, pathlib.Path]): The path to read the construction file from.
        logger (logging.Logger): The logger to log to.
        read_csv_func (Callable): A function to read in a csv.
        file_exists_func (Callable): A function to check that a file exists.

    Returns:
        pd.DataFrame: The construction file in df format.
    """
    logger.info(f"Attempting to read construction file from {path}...")
    construction_file_exists = file_exists_func(path)
    if construction_file_exists:
        try:
            construction_df = read_csv_func(path)
            logger.info(f"Successfully read construction file from {path}.")
            return construction_df
        except pd.errors.EmptyDataError:
            logger.warning(f"Construction file {path} is empty, skipping...")
            return None
    logger.warning("Construction file not found, skipping construction...")
    return None


def prepare_forms_gb(
    snapshot_df: pd.DataFrame, construction_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare GB forms.

    Args:
        snapshot_df (pd.DataFrame): The snapshot df (all official data)
        construction_df (pd.DataFrame): The construction df (artifical data to be added)

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Both datasets with prepared forms
    """
    # Convert formtype to "0001" or "0006"
    if "formtype" in construction_df.columns:
        construction_df["formtype"] = construction_df["formtype"].apply(
            convert_formtype
        )

    if "construction_type" in construction_df.columns:
        # Prepare the short to long form constructions, if any (N/A to NI)
        if "short_to_long" in construction_df.construction_type.unique():
            snapshot_df = prepare_short_to_long(snapshot_df, construction_df)
    # Create period_year column (NI already has it)
    snapshot_df = create_period_year(snapshot_df)
    construction_df = create_period_year(construction_df)
    # Set instance=1 so longforms with status 'Form sent out' match correctly
    form_sent_condition = (snapshot_df.formtype == "0001") & (
        snapshot_df.status == "Form sent out"
    )
    snapshot_df.loc[form_sent_condition, "instance"] = 1
    # Set instance=0 so shortforms with status 'Form sent out' match correctly
    form_sent_condition = (snapshot_df.formtype == "0006") & (
        snapshot_df.status == "Form sent out"
    )
    snapshot_df.loc[form_sent_condition, "instance"] = 0
    return (snapshot_df, construction_df)


def prepare_short_to_long(updated_snapshot_df, construction_df):
    """Create addional instances for short to long construction"""

    construction_df.loc[construction_df["construction_type"] == "short_to_long", "604"] = "Yes"

    # Check which references are going to be converted to long forms
    # and how many instances they have
    ref_count = construction_df.loc[
        construction_df["construction_type"].str.lower() == "short_to_long",
        "reference",  # noqa: E712
    ].value_counts()

    # Create conversion df
    short_to_long_df = updated_snapshot_df[
        updated_snapshot_df["reference"].isin(ref_count.index)
    ]

    # For every short_to_long reference,
    # this copies the instance 0 the relevant number of times,
    # updating to the corresponding instance number
    for index, value in ref_count.items():
        for instance in range(1, value):
            short_to_long_df_instance = short_to_long_df.loc[
                short_to_long_df["reference"] == index
            ].copy()
            short_to_long_df_instance["instance"] = instance
            updated_snapshot_df = pd.concat(
                [updated_snapshot_df, short_to_long_df_instance]
            )

    return updated_snapshot_df


def clean_construction_type(value: str) -> str:
    """Simple cleaning on construction_type values

    Args:
        value (str): The value to clean.

    Returns:
        str: The cleaned value.
    """
    # basic formatting
    if pd.isna(value):
        return np.NaN
    cleaned = value.lower().strip()
    if cleaned == "":
        return np.NaN
    # remove whitespaces
    cleaned = "_".join(cleaned.split())
    return cleaned


def finalise_forms_gb(updated_snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """Tasks to prepare the GB forms for the next stage in pipeline.

    Args:
        updated_snapshot_df (pd.DataFrame): The updated snapshot df.

    Returns:
        pd.DataFrame: The updated snapshot df with postcodes_harmonised
            and short forms reset.
    """

    constructed_df = updated_snapshot_df[updated_snapshot_df.is_constructed == True]
    not_constructed_df = updated_snapshot_df[
        updated_snapshot_df.is_constructed == False
    ]

    # Long form records with a postcode in 601 use this as the postcode
    long_form_cond = ~constructed_df["601"].isnull()
    constructed_df.loc[long_form_cond, "postcodes_harmonised"] = constructed_df["601"]

    # Short form records with nothing in 601 use referencepostcode instead
    short_form_cond = (constructed_df["601"].isnull()) & (
        ~constructed_df["referencepostcode"].isnull()
    )
    constructed_df.loc[short_form_cond, "postcodes_harmonised"] = constructed_df[
        "referencepostcode"
    ]

    # Top up all new postcodes so they're all eight characters exactly
    postcode_cols = ["601", "referencepostcode", "postcodes_harmonised"]
    for col in postcode_cols:
        constructed_df[col] = constructed_df[col].apply(pcval.format_postcodes)

    updated_snapshot_df = pd.concat([constructed_df, not_constructed_df]).reset_index(
        drop=True
    )

    # Reset shortforms with status 'Form sent out' to instance=None
    form_sent_condition = (updated_snapshot_df.formtype == "0006") & (
        updated_snapshot_df.status == "Form sent out"
    )
    updated_snapshot_df.loc[form_sent_condition, "instance"] = None

    return updated_snapshot_df


def add_constructed_nonresponders(
    updated_snapshot_df: pd.DataFrame, construction_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Add constructed non-responders to the snapshot dataframe.

    Args:
        updated_snapshot_df (pd.DataFrame): The updated snapshot dataframe.
        construction_df (pd.DataFrame): The construction dataframe.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: The updated snapshot dataframe and the
            modified construction dataframe.
    """
    new_rows = construction_df["construction_type"].str.contains("new", na=False)
    rows_to_add = construction_df[new_rows]
    construction_df = construction_df[~new_rows]
    missing_columns = set(updated_snapshot_df.columns) - set(rows_to_add.columns)
    for col in missing_columns:
        rows_to_add[col] = np.nan
    rows_to_add = prep_new_rows(rows_to_add, updated_snapshot_df)
    rows_to_add[updated_snapshot_df.columns]
    updated_snapshot_df = pd.concat([updated_snapshot_df, rows_to_add])
    return updated_snapshot_df, construction_df


def remove_short_to_long_0(
    updated_snapshot_df: pd.DataFrame, construction_df: pd.DataFrame
) -> pd.DataFrame:
    """Remove instance 0 for short to long constructions.

    Args:
        updated_snapshot_df (pd.DataFrame): The updated snapshot df.
        construction_df (pd.DataFrame): The construction df.

    Returns:
        pd.DataFrame: The updated snapshot df with instance 0
            removed for short to long constructions.
    """
    short_to_long_references = construction_df.loc[
        construction_df["construction_type"] == "short_to_long",
        "reference",
    ].unique()

    updated_snapshot_df = updated_snapshot_df[
        ~(
            updated_snapshot_df["reference"].isin(short_to_long_references)
            & (updated_snapshot_df["instance"] == 0)
        )
    ]

    return updated_snapshot_df


def prep_new_rows(
        rows_to_add: pd.DataFrame, 
        updated_snapshot_df: pd.DataFrame
    ) -> pd.DataFrame:
    """Prepare new rows from construction to be added to the snapshot.

    Args:
        rows_to_add (pd.DataFrame): The rows that will be added to the snapshot.
        updated_snapshot_df (pd.DataFrame): The current snapshot of data.

    Raises:
        ValueError: Raised if there are rows with missing formtype/cellnumber.

    Returns:
        pd.DataFrame: The new rows (from construction) containing formtype and 
            cellnumber.
    """
    # iterate through new rows and add formtype/cellnumber from snapshot
    for index, row in rows_to_add.iterrows():
        if pd.isna(row['formtype']) or pd.isna(row['cellnumber']):
            reference = row['reference']
            snapshot_row = updated_snapshot_df[updated_snapshot_df['reference'] == reference].iloc[0]
            if pd.isna(row['formtype']):
                rows_to_add.at[index, 'formtype'] = snapshot_row['formtype']
            if pd.isna(row['cellnumber']):
                rows_to_add.at[index, 'cellnumber'] = snapshot_row['cellnumber']
    # obtain references with missing formtype/cellnumber
    missing_references = rows_to_add[
        rows_to_add['formtype'].isna() | rows_to_add['cellnumber'].isna()
    ]['reference']
    if not missing_references.empty:
        raise ValueError(
            "Missing formtype and/or cellnumber for new reference in construction: "
            f"ref {missing_references.tolist()}"
        )

    return rows_to_add
