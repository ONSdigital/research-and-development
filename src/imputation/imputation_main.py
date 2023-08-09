"""The main file for the Imputation module."""
import logging
import pandas as pd

from src.imputation.pg_conversion import pg_to_pg_mapper
from src.imputation import tmi_imputation as tmi

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
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

    final_df = tmi.run_tmi(df, keyvars, mapper)

    return final_df
