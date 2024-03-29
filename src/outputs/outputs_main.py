"""The main file for the Outputs module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any

from src.outputs.form_output_prep import form_output_prep
from src.outputs.frozen_group import output_frozen_group
from src.outputs.short_form import output_short_form
from src.outputs.long_form import output_long_form
from src.outputs.tau import output_tau
from src.outputs.gb_sas import output_gb_sas
from src.outputs.ni_sas import output_ni_sas
from src.outputs.intram_by_pg import output_intram_by_pg
from src.outputs.intram_by_itl1 import output_intram_by_itl1
from src.outputs.intram_uk_itl_1_2 import output_intram_uk_itl_1_2
from src.outputs.intram_by_civil_defence import output_intram_by_civil_defence
from src.outputs.intram_by_sic import output_intram_by_sic
from src.outputs.total_fte import qa_output_total_fte

OutputMainLogger = logging.getLogger(__name__)


def run_outputs(
    estimated_df: pd.DataFrame,
    weighted_df: pd.DataFrame,
    ni_full_responses: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
    sic_pg_num: pd.DataFrame,
    pg_detailed: pd.DataFrame,
    itl1_detailed: pd.DataFrame,
    civil_defence_detailed: pd.DataFrame,
    sic_division_detailed: pd.DataFrame,
    pg_num_alpha: pd.DataFrame,
    sic_pg_alpha: pd.DataFrame,
):

    """Run the outputs module.

    Args:
        estimated_df (pd.DataFrame): The main dataset containing
        short and long form output
        weighted_df (pd.DataFrame): Dataset with weights computed but not applied
        ni_full_responses(pd.DataFrame): Dataset with all NI data
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        postcode_mapper (pd.DataFrame): Links postcode to region code
        itl_mapper (pd.DataFrame): Links region to ITL codes
        sic_pg_num (pd.DataFrame): Maps SIC to numeric PG
        pg_detailed (pd.DataFrame): Detailed descriptons of alpha PG groups
        itl1_detailed (pd.DataFrame): Detailed descriptons of ITL1 regions
        civil_defence_detailed (pd.DataFrame): Detailed descriptons of civil/defence
        sic_division_detailed (pd.DataFrame): Detailed descriptons of SIC divisions
        pg_num_alpha (pd.DataFrame): Mapper for product group conversions (num to alpha)
        sic_pg_num (pd.DataFrame): Mapper for product group conversions
    """

    # Remove instance 0 from weighted df, so that it does not go to Tau outputs
    weighted_df = weighted_df.copy().loc[weighted_df.instance != 0]

    (ni_full_responses, outputs_df, tau_outputs_df) = form_output_prep(
        estimated_df,
        weighted_df,
        ni_full_responses,
        pg_num_alpha,
        sic_pg_num,
    )

    # Running short form output
    if config["global"]["output_short_form"]:
        OutputMainLogger.info("Starting short form output...")
        output_short_form(
            outputs_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            postcode_mapper,
        )
        OutputMainLogger.info("Finished short form output.")

    # Instance 0 should now be removed from all subsequent outputs
    outputs_df = outputs_df.copy().loc[outputs_df.instance != 0]

    # Running long form output
    if config["global"]["output_long_form"]:
        OutputMainLogger.info("Starting long form output...")
        output_long_form(
            outputs_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
        )
        OutputMainLogger.info("Finished long form output.")

    # Filter out records that answer "no R&D" for all subsequent outputs
    tau_outputs_df = tau_outputs_df.copy().loc[~(tau_outputs_df["604"] == "No")]
    outputs_df = outputs_df.copy().loc[~(outputs_df["604"] == "No")]

    # Running TAU output
    if config["global"]["output_tau"]:
        OutputMainLogger.info("Starting TAU output...")
        output_tau(
            tau_outputs_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            postcode_mapper,
        )
        OutputMainLogger.info("Finished TAU output.")

    # Running GB SAS output
    if config["global"]["output_gb_sas"]:
        OutputMainLogger.info("Starting GB SAS output...")
        output_gb_sas(
            outputs_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            postcode_mapper,
        )
        OutputMainLogger.info("Finished GB SAS output.")

    # Running NI SAS output
    if config["global"]["output_ni_sas"]:
        OutputMainLogger.info("Starting NI SAS output...")
        output_ni_sas(
            ni_full_responses,
            config,
            write_csv,
            run_id,
        )
        OutputMainLogger.info("Finished NI SAS output.")

    # Running Intram by PG output
    if config["global"]["output_intram_by_pg"]:
        OutputMainLogger.info("Starting  Intram by PG output...")
        output_intram_by_pg(
            outputs_df,
            config,
            write_csv,
            run_id,
            pg_detailed,
        )
        OutputMainLogger.info("Finished  Intram by PG output.")

    # Running Intram by ITL1
    if config["global"]["output_intram_by_itl1"]:
        OutputMainLogger.info("Starting  Intram by ITL1 output...")
        output_intram_by_itl1(
            outputs_df,
            config,
            write_csv,
            run_id,
            postcode_mapper,
            itl_mapper,
            itl1_detailed,
        )
        OutputMainLogger.info("Finished  Intram by ITL1 output.")

    # Running Intram UK by ITL 1 and 2
    if config["global"]["output_intram_uk_itl2"]:
        OutputMainLogger.info("Starting  Intram UK by ITL 1 and 2 output...")
        output_intram_uk_itl_1_2(
            outputs_df,
            ni_full_responses,
            config,
            write_csv,
            run_id,
            postcode_mapper,
            itl_mapper,
        )
        OutputMainLogger.info("Finished  Intram UK by ITL 1 and 2 output.")

    # Running frozen group
    if config["global"]["output_frozen_group"]:
        OutputMainLogger.info("Starting frozen group output...")
        output_frozen_group(
            outputs_df,
            ni_full_responses,
            ultfoc_mapper,
            config,
            write_csv,
            run_id,
        )
        OutputMainLogger.info("Finished frozen group output.")

    # Running Intram by civil or defence
    if config["global"]["output_intram_by_civil_defence"]:
        OutputMainLogger.info("Starting Intram by civil or defence output...")
        output_intram_by_civil_defence(
            outputs_df,
            config,
            write_csv,
            run_id,
            civil_defence_detailed,
        )
        OutputMainLogger.info("Finished Intram by civil or defence output.")

    # Running Intram by SIC
    if config["global"]["output_intram_by_sic"]:
        OutputMainLogger.info("Starting Intram by SIC output...")
        output_intram_by_sic(
            outputs_df,
            config,
            write_csv,
            run_id,
            sic_division_detailed,
        )
        OutputMainLogger.info("Finished Intram by SIC output.")

    # Running FTE total QA
    if config["global"]["output_fte_total_qa"]:
        qa_output_total_fte(outputs_df, config, write_csv, run_id)
        OutputMainLogger.info("Finished FTE total QA output.")
