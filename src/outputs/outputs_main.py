"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.short_form_out as short
import src.outputs.map_output_cols as map

OutputMainLogger = logging.getLogger(__name__)


def run_output(
    estimated_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    postcode_itl_mapper: pd.DataFrame,
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

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    estimated_df = map.join_fgn_ownership(estimated_df, ultfoc_mapper)

    # Map to the CORA statuses from the statusencoded column
    estimated_df = map.create_cora_status_col(estimated_df, cora_mapper)

    # Map the sizebands based on frozen employment
    estimated_df = map.map_sizebands(estimated_df)

    # Map the itl regions using the postcodes
    estimated_df = map.join_itl_regions(estimated_df, postcode_itl_mapper)

    # Prepare the shortform output dataframe
    short_form_df = short.run_shortform_prep(estimated_df, round_val=4)

    # Create short form output dataframe with required columns from schema
    schema_path = config["schema_paths"]["frozen_shortform_schema"]
    shortform_output = short.create_shortform_df(short_form_df, schema_path)

    if config["global"]["output_short_form"]:
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"output_short_form{tdate}_v{run_id}.csv"
        write_csv(f"{output_path}/output_short_form/{filename}", shortform_output)
    OutputMainLogger.info("Finished short form output.")
