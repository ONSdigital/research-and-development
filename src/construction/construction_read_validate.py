"""Read and validate construction files the construction module."""
import logging
import pandas as pd
from typing import Callable

from src.construction.construction_helpers import (
    read_construction_file,
)
from src.construction.construction_validation import (
    check_for_duplicates,
    validate_columns_not_empty,
)

from src.staging.validation import validate_data_with_schema

construction_logger = logging.getLogger(__name__)


def read_validate_construction_files(
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    is_northern_ireland: bool = False,
    run_construction: bool = False,
    run_postcode_construction: bool = False,
):
    """Read and validate construction files the construction module."""

    # Obtain construction paths
    paths = config["construction_paths"]
    if is_northern_ireland:
        construction_file_path = paths["construction_file_path_ni"]
        schema_path = "./config/construction_ni_schema.toml"
        run_postcode_construction = False
    else:
        construction_file_path = paths["all_data_construction_file_path"]
        postcode_construction_fpath = paths["postcode_construction_file_path"]
        schema_path = "./config/all_data_construction_schema.toml"
        postcode_schema_path = "./config/postcode_construction_schema.toml"

    # Check the construction file exists and has records, then read it
    if run_construction:
        construction_df = read_construction_file(
            path=construction_file_path,
            logger=construction_logger,
            read_csv_func=read_csv,
            file_exists_func=check_file_exists,
        )

        if isinstance(construction_df, type(None)):
            construction_df = pd.DataFrame()
            return construction_df

        else:
            # validate and merge schemas
            validate_data_with_schema(construction_df, schema_path)
            check_for_duplicates(
                df=construction_df,
                columns=["reference", "instance"],
                logger=construction_logger,
            )
    else:
        construction_df = pd.DataFrame()

    # read in postcode construction file
    if run_postcode_construction:
        pc_construction_df = read_construction_file(
            path=postcode_construction_fpath,
            logger=construction_logger,
            read_csv_func=read_csv,
            file_exists_func=check_file_exists,
        )
        if isinstance(pc_construction_df, type(None)):
            run_postcode_construction = False
            pc_construction_df = pd.DataFrame()
    else:
        pc_construction_df = pd.DataFrame()

    if run_postcode_construction:
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

    return construction_df, pc_construction_df
