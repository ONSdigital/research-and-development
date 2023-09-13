"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

from src.outputs.short_form_out import run_shortform_prep
from src.outputs.cora_mapping_temp_file_delete import create_cora_status_col
from src.outputs.temp_file_to_be_deleted import combine_dataframes
from src.outputs.cora_mapping_temp_file_delete import create_cora_status_col

OutputMainLogger = logging.getLogger(__name__)


def run_output(
    estimated_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame
):
    """Run the outputs module.

    Args:
        estimated_df (pd.DataFrame): The main dataset contains short form output
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column


    """
    OutputMainLogger.info("Starting short form output...")

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    output_path = config[f"{NETWORK_OR_HDFS}_paths"]["output_path"]
    
    # Create combined ownership column using mapper
    estimated_df = combine_dataframes(estimated_df, ultfoc_mapper)

    # add cora status "form_status" using mapper
    estimated_df = create_cora_status_col(estimated_df, cora_mapper)

    # Creating blank columns for short form output
    short_form_df = run_shortform_prep(estimated_df, round_val=4)

    if config["global"]["output_short_form"]:
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"output_short_form{tdate}_v{run_id}.csv"
        write_csv(f"{output_path}/output_short_form/{filename}", short_form_df)
    OutputMainLogger.info("Finished short form output.")
