"""Read and validate construction files the construction module."""
import logging
from typing import Callable, Tuple

import pandas as pd


from src.construction.construction_helpers import (
    read_construction_file,
)
from src.construction.construction_validation import (
    check_for_duplicates,
    validate_columns_not_empty,
)

from src.staging.validation import validate_data_with_schema

construction_logger = logging.getLogger(__name__)


def read_validate_all_construction_files(
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    construction_logger,
    is_northern_ireland: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read and validate construction files for the construction module.

    Function reads config to determine which constructions are required/if it
    is for Northern Ireland. It then reads the construction files and validates.

    Args:
        config (dict): The pipeline configuration.
        check_file_exists (Callable): Function to check if file exists.
        read_csv (Callable): Function to read a csv file.
        construction_logger (logging.Logger): The logger for the construction module.
        is_northern_ireland (bool, optional): If true, do construction
            on Northern Ireland data instead of GB data.

    Returns:
        pd.DataFrame: The construction dataframe.
    """

    # Obtain construction paths
    paths = config["construction_paths"]
    if is_northern_ireland:
        construction_file_path = paths["construction_file_path_ni"]
        schema_path = "./config/construction_ni_schema.toml"
        run_postcode_construction = False
    else:
        construction_file_path = paths["all_data_construction_file_path"]
        schema_path = "./config/all_data_construction_schema.toml"

    # Check the construction file exists and has records, then read it
    if not check_file_exists(construction_file_path):
        raise FileNotFoundError(
            f"Construction file not found at: {construction_file_path}"
        )
    construction_df = read_construction_file(
        path=construction_file_path,
        logger=construction_logger,
        read_csv_func=read_csv,
        file_exists_func=check_file_exists,
    )
    # NI data has no instance but needs an instance of 1
    if is_northern_ireland:
        construction_df["instance"] = 1
        construction_df["construction_type"] = None

    else:
        # Validate construction data and check it doesn't contain duplicates
        validate_data_with_schema(construction_df, schema_path)
        check_for_duplicates(
            df=construction_df,
            columns=["reference", "instance"],
            logger=construction_logger,
        )

    return construction_df


def read_validate_postcode_construction_file(
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    construction_logger
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read and validate construction files for the construction module.

    Function reads config to determine which constructions are required/if it
    is for Northern Ireland. It then reads the construction files and validates.

    Args:
        config (dict): The pipeline configuration.
        check_file_exists (Callable): Function to check if file exists.
        read_csv (Callable): Function to read a csv file.nstruction (bool, optional): If true, run the postcode
            only construction.

    Returns:
        pd.DataFrame: The construction dataframes for postcode constructions.
    """

    # read in postcode construction file
    paths = config["construction_paths"]
    postcode_construction_fpath = paths["postcode_construction_file_path"]
    postcode_schema_path = "./config/postcode_construction_schema.toml"

    if not check_file_exists(postcode_construction_fpath):
        raise FileNotFoundError(
            f"PC construction file not found at: {postcode_construction_fpath}"
        )
    pc_construction_df = read_construction_file(
        path=postcode_construction_fpath,
        logger=construction_logger,
        read_csv_func=read_csv,
        file_exists_func=check_file_exists,
    )

    validate_data_with_schema(pc_construction_df, postcode_schema_path)
    check_for_duplicates(
        df=pc_construction_df,
        columns=["reference", "instance"],
        logger=construction_logger,
    )
    validate_columns_not_empty(
        df=pc_construction_df,
        columns=["601", "referencepostcode"],
        logger=construction_logger,
        _raise=True,
    )
    pc_construction_df["force_imputation"] = True

    return pc_construction_df
