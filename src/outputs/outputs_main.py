"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any
import toml

from src.outputs.short_form_out import run_shortform_prep
from src.outputs.temp_file_to_be_deleted import combine_dataframes

OutputMainLogger = logging.getLogger(__name__)

# Get the shortform schema
short_form_schema = toml.load("src/outputs/output_schemas/frozen_shortform_schema.toml")

# Columns are needed for filtering
short_form_cols = short_form_schema.keys()
short_form_cols = [col[1:] if col.startswith("q") else col for col in short_form_cols]


def run_output(
    estimated_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
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
    """

    OutputMainLogger.info("Starting short form output...")

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Create combined ownership column using mapper
    estimated_df = combine_dataframes(estimated_df, ultfoc_mapper)

    # Creating blank columns for short form output
    short_form_df = run_shortform_prep(estimated_df, round_val=4)

    # Filter to the needed columns only
    # TODO: remove this and replace with Ilyas's method
    short_form_df = short_form_df[short_form_cols]

    if config["global"]["output_short_form"]:
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"output_short_form{tdate}_v{run_id}.csv"
        write_csv(f"{output_path}/output_short_form/{filename}", short_form_df)
    OutputMainLogger.info("Finished short form output.")
