"""Stage and Validate Northern Ireland BERD data."""
import logging
from typing import Callable, Tuple
from datetime import datetime
import pandas as pd
import os

from src.staging import validation as val

NIStagingLogger = logging.getLogger(__name__)


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
    # read in csv files as pandas dataframes
    paths = config[f"{config['global']['network_or_hdfs']}_paths"]
    responses_file = paths["ni_responses_path"]
    indicative_file = paths["ni_indicative_path"]

    # raise error if the required files do not exist
    check_file_exists(responses_file, raise_error=True)
    check_file_exists(indicative_file, raise_error=True)

    ni_responses = read_csv(responses_file)
    ni_indcative = read_csv(indicative_file)

    return "hello"