"""The main file for the BERD Intram by Civil or Defence output."""
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Callable, Dict, Any

OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_civil_defence(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    civil_defence_detailed: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with weights not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        civil_defence_detailed (pd.DataFrame): Detailed schema of C/D output


    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    period = config["years"]["survey_year"]
    period_str = str(period)

    # Group by civil/defence (200) and aggregate intram (211)
    key_col = "200"
    value_col = "211"

    df_agg = df.groupby([key_col]).agg({value_col: "sum"}).reset_index()

    # Merge with output table
    df_merge = civil_defence_detailed.merge(
        df_agg, how="left", left_on="CD", right_on=key_col
    )

    # Replace placeholder "period" with year from config
    df_merge["B"] = df_merge["B"].replace("period", period_str)

    # Copy summed values to correct column
    df_merge["B"] = np.where(df_merge["211"].notnull(), df_merge["211"], df_merge["B"])

    # Drop the columns/rows not required for output
    df_merge = df_merge.drop(columns=["CD", "200", "211"])
    df_merge.columns = df_merge.iloc[0]
    df_for_output = df_merge.iloc[1:]

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_intram_by_civil_defence{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_intram_by_civil_defence/{filename}", df_for_output)
