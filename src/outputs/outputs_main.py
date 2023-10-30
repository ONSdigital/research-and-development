"""The main file for the Outputs module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any

from src.outputs.short_form import output_short_form
from src.outputs.long_form import output_long_form
from src.outputs.tau import output_tau


OutputMainLogger = logging.getLogger(__name__)


def run_outputs(
    estimated_df: pd.DataFrame,
    weighted_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    postcode_itl_mapper: pd.DataFrame,
    pg_alpha_num: pd.DataFrame,
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
        output_short_form(
            estimated_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            cora_mapper,
            postcode_itl_mapper,
        )
        OutputMainLogger.info("Finished short form output.")

    # Running long form output
    if config["global"]["output_long_form"]:
        OutputMainLogger.info("Starting long form output...")
        output_long_form(
            estimated_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            cora_mapper,
        )
        OutputMainLogger.info("Finished long form output.")

    # Running TAU output
    if config["global"]["output_tau"]:
        OutputMainLogger.info("Starting TAU output...")
        output_tau(
            weighted_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            cora_mapper,
            postcode_itl_mapper,
            pg_alpha_num,
        )
        OutputMainLogger.info("Finished TAU output.")
