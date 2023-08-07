"""The main file for the Imputation module."""
import logging
import pandas as pd
from typing import Callable

from src.imputation.pg_conversion import pg_to_pg_mapper, sic_to_pg_mapper

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    config: dict,
    read_mapper: Callable,
) -> pd.DataFrame:

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # Conditionally load paths
    paths = config[f"{network_or_hdfs}_paths"]
    mapper_path = paths["mapper_path"]

    mapper = read_mapper(mapper_path)
    pg_to_pg_mapper(df, mapper)
    sic_to_pg_mapper(df, mapper)

    return df
