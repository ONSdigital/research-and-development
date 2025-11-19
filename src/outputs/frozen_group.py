"""The main file for the frozen group output."""

import logging
from typing import Any
from collections.abc import Callable

import pandas as pd

from src.outputs.outputs_helpers import create_output_df
import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.utils.helpers import filename_amender
from src.utils.breakdown_validation import get_all_wanted_columns

OutputMainLogger = logging.getLogger(__name__)


def output_frozen_group(
    df_gb: pd.DataFrame,
    df_ni: pd.DataFrame,
    config: dict[str, Any],
    intram_tot_dict: dict[str, int],
    write_csv: Callable,
    deduplicate: bool = False,
) -> dict[str, int]:
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
        intram_tot_dict: (dict): Intram totals for various outputs for QA
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        deduplicate (bool): If true, the data is deduplicated by aggregation.

    Returns:
        None

    """
    output_path = config["outputs_paths"]["outputs_master"]

    df_gb = map_o.map_FG_cols_to_numeric(df_gb)
    if df_ni is not None:
        if not df_ni.empty:
            df_ni = map_o.map_FG_cols_to_numeric(df_ni)

    # Categorical columns that we have in BERD and NI data
    category_columns = [
        "period_year",
        "reference",
        "200",
        "201",
        "formtype",
        "employment",
        "ultfoc",
        "form_status",
        "wowenterprisereference",
        "rusic",
        "251",
        "307",
        "308",
        "309",
    ]

    # Numerical value columns that we have in BERD and NI longform data
    value_columns = get_all_wanted_columns(config, "longform")

    # Columns that we don't have that should have pd.NA values
    blank_columns = [
        "freeze_id",
        "period",
        "cell_id",
        "period_contributor_id",
        "data_source",
    ]

    # Columns that we don't have that should have zero values
    # The numerical questions starting with q, like q208, are needed for the
    # output. If it's without q, then we have it in our data.
    zero_columns = [
        "q208",
        "q213",
        "q215",
        "q217",
        "q224",
        "q230",
        "q231",
        "q232",
        "q233",
        "q234",
        "q235",
        "q236",
        "q238",
        "q239",
        "q240",
        "q241",
        "q252",
        "q253",
        "q254",
        "q255",
        "q256",
        "q257",
        "q258",
    ]

    # Map to the CORA statuses from the statusencoded column
    df_gb = map_o.create_cora_status_col(df_gb)

    # Select the columns we need
    need_columns = category_columns + value_columns
    df_gb_need = df_gb[need_columns]
    if df_ni is not None:
        if not df_ni.empty:
            df_ni_need = df_ni[need_columns]
        else:
            df_ni_need = df_ni
    else:
        df_ni_need = df_ni

    # Concatinate GB and NI
    df = pd.concat([df_gb_need, df_ni_need], ignore_index=True, axis=0)

    # Deduplicate by aggregation
    # TODO: this code fails in DAP for PNP. Think whether it's necessary and
    # TODO then refactor this, using a list of columns from the config
    # TODO and considering whether there are extra cols in the PNP case.
    if deduplicate:
        df_agg = df.groupby(category_columns).agg("sum").reset_index()
    else:
        df_agg = df

    # Add size bands
    df_agg = map_o.map_sizebands(df_agg)

    # Create blank columns and assign blank values
    for bcol in blank_columns:
        df_agg[bcol] = pd.NA

    # Create columns for zero values and assign zero values
    for zcol in zero_columns:
        df_agg[zcol] = 0

    # Update intram totals dict for comparison of aggregates across outputs
    intram_tot_dict["frozen_group"] = round(df_agg["211"].sum(), 0)

    # Create frozen group output dataframe with required columns from schema
    schema_path = config["schema_paths"]["frozen_group_schema"]
    schema_dict = load_schema(schema_path)
    output = create_output_df(df_agg, schema_dict)

    # Outputting the CSV file
    filename = filename_amender("output_frozen_group", config)
    write_csv(f"{output_path}/output_frozen_group/{filename}", output)

    return intram_tot_dict
