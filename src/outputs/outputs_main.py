"""The main file for the Outputs module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any

from src.outputs.short_form import output_short_form
from src.outputs.tau import output_tau
from src.outputs.gb_sas import output_gb_sas
from src.outputs.intram_by_pg import output_intram_by_pg

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
    pg_num_alpha: pd.DataFrame,
    sic_pg_alpha: pd.DataFrame,
    pg_detailed: pd.DataFrame,
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
        postcode_itl_mapper (pd.DataFrame): Links postcode to region code
        pg_alpha_num (pd.DataFrame): Maps alpha PG to numeric PG
        pg_detailed (pd.DataFrame): Detailed descriptons of alpha PG groups


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

# Running GB SAS output
    if config["global"]["output_gb_sas"]:
        OutputMainLogger.info("Starting GB SAS output...")
        output_gb_sas(
            estimated_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            cora_mapper,
            postcode_itl_mapper,
            pg_alpha_num,
        )
        OutputMainLogger.info("Finished GB SAS output.")

    # Running Intram by PG output
    if config["global"]["output_intram_by_pg"]:
        OutputMainLogger.info("Starting  Intram by PG output...")
        output_intram_by_pg(
            estimated_df,
            config,
            write_csv,
            run_id,
            pg_detailed,
        )
        OutputMainLogger.info("Finished  Intram by PG output.")
