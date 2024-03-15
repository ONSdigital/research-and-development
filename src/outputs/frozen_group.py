"""The main file for the frozen group output."""

import logging
from typing import Callable, Dict, Any, List

import pandas as pd
from datetime import datetime

from src.outputs.outputs_helpers import aggregate_output, create_output_df
import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.utils.helpers import get_numeric_cols

OutputMainLogger = logging.getLogger(__name__)


def output_frozen_group(
    df_gb: pd.DataFrame,
    df_ni: pd.DataFrame,
    ultfoc_mapper: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    deduplicate: bool = True
) -> None:
    """Creates a "frozen group" output  for the entire UK. In BERD (GB) data,
    creates foreign ownership and cora status. Selects the columns we need for
    this type of output. Combines BERD and NI data. Adds size bands. De-
    duplicates the dataframe, combining the values of multiple sites in one
    record. Adds blank columns and zero columns - they need to be in the output
    for back-compatibility, but we don't have any data. output CSV files.

    Args:
        df_gb (pd.DataFrame): The GB microdata with weights applied
        df_ni (pd.DataFrame): The NI microdata; weights are 1
        ultfoc_mapper (pd.DataFrame): Ultimate foreign owner mappper.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        deduplicate (bool): If true, the data is deduplicated by aggregation.

    Returns:
        None

    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    df_gb = map_o.map_FG_cols_to_numeric(df_gb)
    df_ni = map_o.map_FG_cols_to_numeric(df_ni)

    # Categorical columns that we have in BERD and NI data
    category_columns = [
        "period_year", "reference", "200", "201", "formtype",
        "employment", "ultfoc", "form_status",
        "wowenterprisereference", "rusic", "251", "307", "308","309",
    ]

    # Numerical value columns that we have in BERD and NI data
    # These are the same as the columns we impute so we use a function from imputation.
    value_columns = get_numeric_cols(config)

    # Columns that we don't have that should have pd.NA values
    blank_columns = [
        "freeze_id", "period", "cell_id", "period_contributor_id",
        "data_source",
    ]

    # Columns that we don't have that should have zero values
    # The numerical questions starting with q, like q208, are needed for the
    # output. If it's without q, then we have it in our data.
    zero_columns = config["other_variables"]["zero_questions"]

    # Join foriegn ownership column using ultfoc mapper for GB
    df_gb = map_o.join_fgn_ownership(df_gb, ultfoc_mapper, formtype=["0001", "0006"])

    # Map to the CORA statuses from the statusencoded column
    df_gb = map_o.create_cora_status_col(df_gb)

    # Select the columns we need
    need_columns = category_columns + value_columns
    df_gb_need = df_gb[need_columns]
    df_ni_need = df_ni[need_columns]

    # Concatinate GB and NI
    df = pd.concat([df_gb_need, df_ni_need], ignore_index=True, axis=0)

    # Deduplicate by aggregation
    if deduplicate:
        df_agg = aggregate_output(
            df,
            category_columns,
            value_columns,
            "sum"
        )
    else:
        df_agg = df

    # Add size bands
    df_agg = map_o.map_sizebands(df_agg)

    # Create blank and zero columns
    df_agg = df_agg.reindex(
        columns=df_agg.columns.tolist() + blank_columns + zero_columns
    )

    # Create blank columns and assign blank values
    for bcol in blank_columns:
        df_agg[bcol] = pd.NA

    # Create columns for zero values and assign zero values
    for zcol in zero_columns:
        df_agg[zcol] = 0

    # Create frozen group output dataframe with required columns from schema
    schema_path = config["schema_paths"]["frozen_group_schema"]
    schema_dict = load_schema(schema_path)
    output = create_output_df(df_agg, schema_dict)

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_frozen_group_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_frozen_group/{filename}", output)

    return None
