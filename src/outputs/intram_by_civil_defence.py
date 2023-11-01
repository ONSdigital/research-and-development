"""The main file for the Intram by Civil or Defence output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

from src.outputs.outputs_helpers import aggregate_output


OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_civil_defence(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    civil_defence_detailed
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with weights not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Group by PG and aggregate intram
    key_col = "200"
    value_col = "211"
    agg_method = "sum"

    df_agg = aggregate_output(df, [key_col], [value_col], agg_method)

    print(df_agg)

    # Merge with labels and ranks
    df_merge = civil_defence_detailed.merge(
        df_agg,
        how="left",
        left_on="c_d",
        right_on=key_col)
    df_merge[value_col] = df_merge[value_col].fillna(0)

    #! replace period and value

    print(df_merge.head())

    # # Select and rename the correct columns
    # detail = "Total Intramural Expenditure"
    # notes = "Notes"
    # value_title = "2023 (Current period)"
    # df_merge = df_agg[[detail, value_col, notes]].rename(
    #     columns={value_col: value_title})

    # print(df_merge)


    # # Outputting the CSV file with timestamp and run_id
    # tdate = datetime.now().strftime("%Y-%m-%d")
    # filename = f"output_intram_by_civil_defence{tdate}_v{run_id}.csv"
    # write_csv(f"{output_path}/output_intram_by_civil_defence/{filename}", df_merge)
