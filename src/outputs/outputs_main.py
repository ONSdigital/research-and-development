"""The main file for the Outputs module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any

from src.outputs.status_filtered import output_status_filtered
from src.outputs.short_form import output_short_form
from src.outputs.long_form import output_long_form
from src.outputs.tau import output_tau
from src.outputs.gb_sas import output_gb_sas
from src.outputs.intram_by_pg import output_intram_by_pg
from src.outputs.intram_by_itl1 import output_intram_by_itl1
from src.outputs.intram_by_civil_defence import output_intram_by_civil_defence
from src.outputs.intram_by_sic import output_intram_by_sic
from src.outputs.outputs_helpers import postcode_topup
from src.outputs.total_fte import qa_output_total_fte

OutputMainLogger = logging.getLogger(__name__)


def run_outputs(
    estimated_df: pd.DataFrame,
    weighted_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
    sic_pg_num: pd.DataFrame,
    pg_detailed: pd.DataFrame,
    itl1_detailed: pd.DataFrame,
    civil_defence_detailed: pd.DataFrame,
    sic_division_detailed: pd.DataFrame,
):

    """Run the outputs module.

    Args:
        estimated_df (pd.DataFrame): The main dataset containing
        short and long form output
        weighted_df (pd.DataFrame): Dataset with weights computed but not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column
        postcode_mapper (pd.DataFrame): Links postcode to region code
        itl_mapper (pd.DataFrame): Links region to ITL codes
        sic_pg_num (pd.DataFrame): Maps SIC to numeric PG
        pg_detailed (pd.DataFrame): Detailed descriptons of alpha PG groups
        itl1_detailed (pd.DataFrame): Detailed descriptons of ITL1 regions
        civil_defence_detailed (pd.DataFrame): Detailed descriptons of civil/defence
        sic_division_detailed (pd.DataFrame): Detailed descriptons of SIC divisions
    """

    imputed_statuses = ["TMI", "CF", "MoR"]

    to_keep = estimated_df["imp_marker"].isin(imputed_statuses) | (
        estimated_df["imp_marker"] == "R"
    )

    # filter estimated_df and weighted_df to only include clear or imputed statuses
    outputs_df = estimated_df.copy().loc[to_keep]
    tau_outputs_df = weighted_df.copy().loc[to_keep]
    # filter estimated_df for records not included in outputs
    filtered_output_df = estimated_df.copy().loc[~to_keep]

    # change the value of the status column to 'imputed' for imputed statuses
    condition = outputs_df["status"].isin(imputed_statuses)
    outputs_df.loc[condition, "status"] = "imputed"

    # Running status filtered full dataframe output for QA
    if config["global"]["output_status_filtered"]:
        OutputMainLogger.info("Starting status filtered output...")
        output_status_filtered(
            filtered_output_df,
            config,
            write_csv,
            run_id,
        )
        OutputMainLogger.info("Finished status filtered output.")

    outputs_df = outputs_df.astype({"postcodes_harmonised": "str"})
    outputs_df["postcodes_harmonised"] = outputs_df["postcodes_harmonised"].apply(
        postcode_topup
    )

    weighted_df = weighted_df.astype({"postcodes_harmonised": "str"})
    weighted_df["postcodes_harmonised"] = weighted_df["postcodes_harmonised"].apply(
        postcode_topup
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
            cora_mapper,
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
            cora_mapper,
        )
        OutputMainLogger.info("Finished long form output.")

    # Running TAU output
    if config["global"]["output_tau"]:
        OutputMainLogger.info("Starting TAU output...")
        output_tau(
            tau_outputs_df,
            config,
            write_csv,
            run_id,
            ultfoc_mapper,
            cora_mapper,
            postcode_mapper,
            sic_pg_num,
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
            cora_mapper,
            postcode_mapper,
            sic_pg_num,
        )
        OutputMainLogger.info("Finished GB SAS output.")

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
