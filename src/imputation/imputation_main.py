"""The main file for the Imputation module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime

from src.imputation.pg_conversion import pg_to_pg_mapper
from src.imputation import tmi_imputation as tmi

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:

    df = pg_to_pg_mapper(df, mapper)

    keyvars = [
        "211",
        "305",
        "405",
        "406",
        "407",
        "408",
        "409",
        "410",
        "501",
        "502",
        "503",
        "504",
        "505",
        "506",
    ]

    imputed_df, qa_df = tmi.run_tmi(df, keyvars, mapper)
    
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

    return imputed_df
