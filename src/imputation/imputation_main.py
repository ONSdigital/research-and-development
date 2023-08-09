"""The main file for the Imputation module."""
import logging
import pandas as pd

from src.imputation.pg_conversion import pg_to_pg_mapper, sic_to_pg_mapper

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
) -> pd.DataFrame:

    pg_to_pg_mapper(df, mapper)
    sic_to_pg_mapper(df, mapper)

    return df
