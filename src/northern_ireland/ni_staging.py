"""Stage and Validate Northern Ireland BERD data."""

import logging
import os
from collections.abc import Callable
import pandas as pd

from src.staging import validation as val
from src.utils.helpers import filename_amender

NIStagingLogger = logging.getLogger(__name__)


def read_ni_files(
    ni_responses_schema: dict,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
) -> tuple:
    """Read in CSV file and schema for NI data."""
    # read in csv file as pandas dataframes
    ni_full_response_file = config["ni_paths"]["ni_full_responses"]

    # raise error if the required files do not exist
    check_file_exists(ni_full_response_file, raise_error=True)

    NIStagingLogger.info("Loading NI data from csv...")
    ni_full_response_df = read_csv(ni_full_response_file)
    NIStagingLogger.success("Finished reading NI data.")

    return ni_full_response_df


def rename_columns(
    ni_full_responses_df: pd.DataFrame,
    ni_responses_schema: dict,
) -> pd.DataFrame:
    """Rename the columns in CSV file for NI data."""
    # rename columns
    ni_responses_rename_dict = {
        ni_responses_schema[i]["old_name"]: i for i in ni_responses_schema.keys()
    }

    ni_responses_df = ni_full_responses_df.rename(columns=ni_responses_rename_dict)

    return ni_responses_df


def run_ni_staging(
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    write_csv: Callable,
) -> pd.DataFrame:
    """Run the Northern Ireland staging and validation module.

    A CSV file is read in as dataframe. Validation is performed to check
    column names and cast to the required datatypes, based on toml schema. The
    columns are renamed, again based on the toml schema. The resulting
    dataframe can optionally be output for qa purposes.
    The staged NI data is passed back to the pipeline for optional NI construction.
    Apart from construction, the NI data is not used for other pipeline modules
    before the outputs module.

    Args:
        config (dict): The pipeline configuration
        check_file_exists (Callable): Function to check if file exists
            This will be the s3, hdfs or network version depending on settings.
        read_csv (Callable): Function to read a csv file.
            This will be the s3, hdfs or network version depending on settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the s3, hdfs or network version depending on settings.
    Returns:
        ni_full_responses (pd.DataFrame): The staged and vaildated NI data.
    """

    # read in the schemas for the NI data
    ni_responses_schema_path = "./config/ni_full_responses.toml"
    ni_responses_schema = val.load_schema(ni_responses_schema_path)

    # # read in the NI csv file
    ni_responses_df = read_ni_files(
        ni_responses_schema, config, check_file_exists, read_csv
    )

    # Rename the columns
    ni_responses_df = rename_columns(ni_responses_df, ni_responses_schema)

    # Responses are given an instance of one for the construction module
    # Instance will be dropped in outputs.
    ni_responses_df["instance"] = 1

    # Validate the dataframe using the schema
    ni_responses_df = val.validate_data_with_schema(
        ni_responses_df, ni_responses_schema_path
    )

    # Optionally output the staged NI data
    if config["global"]["output_ni_full_responses"]:
        NIStagingLogger.info("Starting output of staged NI data...")
        staging_folder = config["ni_paths"]["ni_staging_output_path"]
        staged_filename = filename_amender("staged_NI_full_responses", config)
        write_csv(os.path.join(staging_folder, staged_filename), ni_responses_df)
        NIStagingLogger.success("Finished output of staged NI data.")
    else:
        NIStagingLogger.info("Skipping output of staged NI data...")

    return ni_responses_df
