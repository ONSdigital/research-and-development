"""The main file for the Intram by PG output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

from src.outputs.outputs_helpers import aggregate_output


OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_pg(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
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
    key_col = "201"
    value_col = "211"
    agg_method = "sum"

    df_agg = aggregate_output(df, [key_col], [value_col], agg_method)

    # Create Total and concatinate it to df_agg
    value_tot = df_agg[value_col].sum()
    df_tot = pd.DataFrame({key_col: ["total"], value_col: value_tot})
    df_agg = pd.concat([df_agg, df_tot])

    # Merge with labels and ranks
    # For debugging - begin
    mypath = (r"R:\BERD Results System Development 2023\DAP_emulation\mappers\pg_detailed.csv")
    pg_detailed = pd.read_csv(mypath)
    # For debugging - end
    df_merge = pg_detailed.merge(
        df_agg,
        how="left",
        left_on="pg_alpha",
        right_on=key_col)
    df_merge[value_col] = df_merge[value_col].fillna(0)

    # Sort by rank
    df_merge.sort_values("rank", axis=0, ascending=True)

    # Select and rename the correct coluns
    detail = "Detailed product groups (Alphabetical product groups A-AH)"
    notes = "Notes"
    value_title = "2023 (Current period)"
    df_merge = df_merge[[detail, value_col, notes]].rename(
        columns={value_col: value_title})

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_intram_by_pg_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_intram_by_pg/{filename}", df_merge)
