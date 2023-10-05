"""The main file for the Imputation module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime

from src.imputation import tmi_imputation as tmi
from src.imputation import expansion_imputation as ximp
from src.imputation.apportionment import run_apportionment

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:

    keyvars = [
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

    df = run_apportionment(df)

    imputed_df, qa_df = tmi.run_tmi(df, keyvars, mapper)

    imputed_df = ximp.run_expansion(imputed_df, config)

    imputed_output_df = imputed_df.loc[imputed_df["formtype"] == "0001"]

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    imp_path = config[f"{NETWORK_OR_HDFS}_paths"]["imputation_path"]

    if config["global"]["output_imputation_qa"]:
        ImputationMainLogger.info("Outputting Imputation files.")
        tdate = datetime.now().strftime("%Y-%m-%d")
        trim_qa_filename = f"trimming_qa_{tdate}_v{run_id}.csv"
        full_imp_filename = f"full_responses_imputed_{tdate}_v{run_id}.csv"
        write_csv(f"{imp_path}/imputation_qa/{trim_qa_filename}", qa_df)
        write_csv(f"{imp_path}/imputation_qa/{full_imp_filename}", imputed_output_df)
    ImputationMainLogger.info("Finished Imputation calculation.")

    return imputed_df
