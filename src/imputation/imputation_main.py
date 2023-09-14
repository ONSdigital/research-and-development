"""The main file for the Imputation module."""
import logging
import pandas as pd

from src.imputation.apportionment import run_apportionment

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
) -> pd.DataFrame:

    df = run_apportionment(df)

    return df
