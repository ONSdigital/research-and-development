"""The main file for the Imputation module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime
from itertools import chain

from src.imputation import tmi_imputation as tmi
from src.imputation.apportionment import run_apportionment
from src.imputation.short_to_long import run_short_to_long
from src.imputation.MoR import run_mor

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    backdata: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:

    target_vars = [
        "211",
        "305",
        "emp_researcher",
        "emp_technician",
        "emp_other",
        "headcount_res_m",
        "headcount_res_f",
        "headcount_tec_m",
        "headcount_tec_f",
        "headcount_oth_m",
        "headcount_oth_f",
    ]

    sum_cols = ["emp_total", "headcount_tot_m", "headcount_tot_f", "headcount_total"]

    # Get the breakdown columns from the config
    bd_qs_lists = list(config["breakdowns"].values())
    bd_cols = list(chain(*bd_qs_lists))

    # Apportion cols 4xx and 5xx to create FTE and headcount values
    df = run_apportionment(df)

    df = run_mor(df, backdata, target_vars)

    # Convert shortform responses to longform format
    df = run_short_to_long(df)

    df["imp_marker"] = "no_imputation"

    # Create new columns to hold the imputed values
    orig_cols = target_vars + bd_cols + sum_cols
    for col in orig_cols:
        df[f"{col}_imputed"] = df[col]

    # run TMI
    imputed_df, qa_df = tmi.run_tmi(df, target_vars, mapper, config)

    # Output QA files
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    imp_path = config[f"{NETWORK_OR_HDFS}_paths"]["imputation_path"]

    if config["global"]["output_imputation_qa"]:
        ImputationMainLogger.info("Outputting Imputation files.")
        tdate = datetime.now().strftime("%Y-%m-%d")
        trim_qa_filename = f"trimming_qa_{tdate}_v{run_id}.csv"
        full_imp_filename = f"full_responses_imputed_{tdate}_v{run_id}.csv"
        write_csv(f"{imp_path}/imputation_qa/{trim_qa_filename}", qa_df)
        write_csv(f"{imp_path}/imputation_qa/{full_imp_filename}", imputed_df)
    ImputationMainLogger.info("Finished Imputation calculation.")

    # Create names for imputed cols
    imp_cols = [f"{col}_imputed" for col in orig_cols]

    # Update the original breakdown questions and target variables with the imputed
    imputed_df[orig_cols] = imputed_df[imp_cols]

    # Drop imputed values from df
    imputed_df = imputed_df.drop(columns=imp_cols)

    return imputed_df
