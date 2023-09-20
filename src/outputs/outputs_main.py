"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any
import toml

from src.outputs.short_form_out import run_shortform_prep
from src.outputs.temp_file_to_be_deleted import combine_dataframes
from src.outputs.manifest_output import Manifest

OutputMainLogger = logging.getLogger(__name__)


def run_output(
    estimated_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame
):
    """Run the outputs module.

    Args:
        estimated_df (pd.DataFrame): The main dataset contains short form output
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper : pd.DataFrame
        The ULTFOC mapper DataFrame.
        delete_file : callable
            The function to use for deleting files.
        hdfs_md5sum : callable
            The function to use for calculating the MD5 checksum of a file on HDFS.
        hdfs_stat_size : callable
            The function to use for getting the size of a file on HDFS.
        isdir : callable
            The function to use for checking if a path is a directory.
        isfile : callable
            The function to use for checking if a path is a file.
        read_header : callable
            The function to use for reading the header of a file.
        write_string_to_file : callable
            The function to use for writing a string to a file.
    """

    OutputMainLogger.info("Starting short form output...")

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Create combined ownership column using mapper
    estimated_df = combine_dataframes(estimated_df, ultfoc_mapper)

    # Creating blank columns for short form output
    short_form_df = run_shortform_prep(estimated_df, round_val=4)

    if config["global"]["output_short_form"]:
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"output_short_form{tdate}_v{run_id}.csv"
        write_csv(f"{output_path}/output_short_form/{filename}", short_form_df)
    OutputMainLogger.info("Finished short form output.")

   