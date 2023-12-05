"""Stage and Validate Northern Ireland BERD data."""
import logging
from typing import Callable, Tuple
from datetime import datetime
import pandas as pd
import os

from src.staging import validation as val

NIStagingLogger = logging.getLogger(__name__)


def read_ni_files(
    indicative_schema: dict,
    responses_schema: dict,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,        
) -> Tuple:
    """Read in the csv files and schemas for NI data."""
    # read in csv files as pandas dataframes
    paths = config[f"{config['global']['network_or_hdfs']}_paths"]
    responses_file = paths["ni_responses_path"]
    indicative_file = paths["ni_indicative_path"]

    # raise error if the required files do not exist
    check_file_exists(responses_file, raise_error=True)
    check_file_exists(indicative_file, raise_error=True)

    NIStagingLogger.info("Loading NI data from csv...")

    # It may be they don't want to use all the cols in the files
    # but they haven't decdided yet, so this way of selecting cols maybe useful.
    wanted_indicative_cols = [
        indicative_schema[i]['old_name'] for i in indicative_schema.keys()
    ]
    wanted_responses_cols = [
        responses_schema[i]['old_name'] for i in responses_schema.keys()     
    ]

    indicative_df = read_csv(indicative_file, wanted_indicative_cols)
    ni_responses_df = read_csv(responses_file, wanted_responses_cols)

    NIStagingLogger.info("Finished reading NI data.")

    return indicative_df, ni_responses_df


def rename_columns(
        indicative_df: pd.DataFrame,
        ni_responses_df: pd.DataFrame,
        indicative_schema: dict,
        responses_schema: dict,
        ) -> pd.DataFrame:
    # rename columns
    indicative_rename_dict = {
        indicative_schema[i]['old_name'] : i for i in indicative_schema.keys()
    }
    responses_rename_dict = {
        responses_schema[i]['old_name'] : i for i in responses_schema.keys()
    }

    indicative_df = indicative_df.rename(columns = indicative_rename_dict)
    ni_responses_df = ni_responses_df.rename(columns = responses_rename_dict)

    return indicative_df, ni_responses_df


def qa_dataframe_merge(ni_full_responses): 
    """Output a warning if there are miss-matches in the join."""
    missing_resp = ni_full_responses.loc[ni_full_responses["_merge"] == "right_only"]
    missing_indic = ni_full_responses.loc[ni_full_responses["_merge"] == "left_only"]

    #TODO: ask the business area about these miss-matches, do they just want a warning,
    #TODO: or do they want an error?

    if not missing_resp.empty:
        msg = (
            "The following references appear in the indicative data "
            "but not in the responses:"
        )
        NIStagingLogger.info(msg + str(missing_resp.reference.unique()))
    
    if not missing_indic.empty:
        msg = (
            "The following references appear in the responses data "
            "but not in the indicative data:"
        )
        NIStagingLogger.info(msg + str(missing_indic.reference.unique()))
    
    return None
    

def run_ni_staging(
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Run the Northern Ireland staging and validation module.

    Two csv files are read in as dataframes, one containing the survey question 
    responses, and the other indicative data.
    
    Validation is performed to check column names and cast to the required datatypes, 
    based on toml schemas. The columns are renamed, again based on the toml schemas.

    The two dataframes are then merged on reference (there are no "instance" entries in
    the NI data, only one entry per reference.) The resulting dataframe can optionally
    be output for qa purposes.

    The staged NI data is passed back to the pipeline, but is not used except to be 
    joined to the GB data for the outputs module.

    Args:
        config (dict): The pipeline configuration
        check_file_exists (Callable): Function to check if file exists
            This will be the hdfs or network version depending on settings.
        read_csv (Callable): Function to read a csv file.
            This will be the hdfs or network version depending on settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
        run_id (int): The run id for this run.
    Returns:
        ni_full_responses (pd.DataFrame): The staged and vaildated NI data.
    """  
    # read in the schemas for the NI data
    indicative_schema_path = "./config/ni_indicative_schema.toml"
    responses_schema_path = "./config/ni_responses_schema.toml"

    indicative_schema = val.load_schema(indicative_schema_path)
    responses_schema = val.load_schema(responses_schema_path)

    # read in the NI csv files as two dataframes
    indicative_df, ni_responses_df = read_ni_files(
        indicative_schema,
        responses_schema,
        config,
        check_file_exists,
        read_csv
    )

    # Rename the columns of the two dataframes
    indicative_df, ni_responses_df = rename_columns(
        indicative_df, 
        ni_responses_df,
        indicative_schema,
        responses_schema
    )

    # Validate the dataframes using the schema
    val.validate_data_with_schema(indicative_df, indicative_schema_path)
    val.validate_data_with_schema(ni_responses_df, responses_schema_path)

    # join the two dataframes on "reference"
    ni_full_responses = pd.merge(
        ni_responses_df, 
        indicative_df, 
        how="outer", 
        on=["period_year", "reference"], 
        indicator=True
    )

    # check if there are unmatched records
    qa_dataframe_merge(ni_full_responses)

    ni_full_responses = ni_full_responses.drop("_merge", axis=1)

    # Optionally output the staged NI data
    if config["global"]["output_ni_full_responses"]:
        network_or_hdfs = config["global"]["network_or_hdfs"]
        paths = config[f"{network_or_hdfs}_paths"]
        NIStagingLogger.info("Starting output of staged NI data...")
        staging_folder = paths["ni_staging_output_path"]
        tdate = datetime.now().strftime("%Y-%m-%d")
        staged_filename = f"staged_NI_full_responses_{tdate}_v{run_id}.csv"
        write_csv(f"{staging_folder}/{staged_filename}", ni_full_responses)
        NIStagingLogger.info("Finished output of staged NI data.")
    else:
        NIStagingLogger.info("Skipping output of staged NI data...")

    return ni_full_responses