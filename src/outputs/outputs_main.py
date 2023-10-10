"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any
import toml

import src.outputs.short_form_out as short
import src.outputs.map_output_cols as map_o

from src.outputs.short_form import run_short_form


OutputMainLogger = logging.getLogger(__name__)

# Get the shortform schema
short_form_schema = toml.load("src/outputs/output_schemas/frozen_shortform_schema.toml")

# Get the Tau Argus output schema
tau_schema = toml.load("src/outputs/output_schemas/tau_schema.toml")

def run_outputs(
    estimated_df: pd.DataFrame,
    weighted_df: pd.DataFrame,
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
        weighted_df (pd.DataFrame): Dataset with weights computed but not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column


    """
    # Running short form output
    if config["global"]["output_short_form"]:
        OutputMainLogger.info("Starting short form output...")
        run_short_form(
            estimated_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            cora_mapper,
            postcode_itl_mapper,
        )
        OutputMainLogger.info("Finished short form output.")

    # Running TAU output
    if config["global"]["output_short_form"]:
        OutputMainLogger.info("Starting TAU output...")
        # run_short_form(
        #     estimated_df,
        #     config,
        #     write_csv,
        #     run_id,
        #     ultfoc_mapper,
        #     cora_mapper,
        #     postcode_itl_mapper,
        # )
        OutputMainLogger.info(f"Weighted responses loaded, size {weighted_df.size}.")
        OutputMainLogger.info("Finished TAUoutput.")
