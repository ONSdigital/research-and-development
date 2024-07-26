"""Useful utilities for the construction module."""
import pathlib
import logging
from typing import Union, Callable, Tuple

import pandas as pd
import numpy as np

from src.outputs.outputs_helpers import create_period_year


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


def _convert_formtype(formtype_value: str) -> str:
    """Convert the formtype to a standardised format.

    Args:
        formtype_value (str): The value to standardise.

    Returns:
        str: The standardised value for formtype.
    """
    if pd.notnull(formtype_value):
        if formtype_value == "1" or formtype_value == "1.0" or formtype_value == "0001":
            return "0001"
        elif (
            formtype_value == "6" or formtype_value == "6.0" or formtype_value == "0006"
        ):
            return "0006"
        else:
            return None
    else:
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
            _convert_formtype
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
    return value


def remove_short_to_long_0(updated_snapshot_df, construction_df):
    """Remove instance 0 for short to long constructions

    Args:
        updated_snapshot_df (pd.DataFrame): The updated snapshot df.
        construction_df (pd.DataFrame): The construction df.

    Returns:
        pd.DataFrame: The updated snapshot df with instance 0
        removed for short to long constructions.
    """
    short_to_long_references = construction_df.loc[
        construction_df["construction_type"].str.lower() == "short_to_long",
        "reference",
    ].unique()

    updated_snapshot_df = updated_snapshot_df[
        ~(
            updated_snapshot_df["reference"].isin(short_to_long_references)
            & (updated_snapshot_df["instance"] == 0)
        )
    ]

    return updated_snapshot_df
