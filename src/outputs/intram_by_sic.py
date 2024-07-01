"""The main file for the BERD Intram by SIC output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_sic(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    sic_div_detailed: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with weights not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        sic_div_detailed (pd.DataFrame): Format of the SIC output as mapper

    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    period = config["years"]["survey_year"]

    # Create sic_division column from rusic
    df["rusic_string"] = df["rusic"].astype(str).str.zfill(5)
    df["sic_division"] = df["rusic_string"].str[:2]

    # Group by sic_division and aggregate intram
    key_col = "sic_division"
    value_col = "211"

    df_agg = df.groupby([key_col]).agg({value_col: "sum"}).reset_index()

    # Create Total and concatinate it to df_agg
    value_tot = df_agg[value_col].sum()
    df_tot = pd.DataFrame({key_col: ["All"], value_col: value_tot})
    df_agg = pd.concat([df_agg, df_tot])

    # Combine relevant sic divisions and total
    # Spaces required in front of 01-03, 05-09 & 11-12 to
    # stop automatic conversion to dates in output
    sic_div_combos = {
        " 01-03": ["01", "02", "03"],
        " 05-09": ["05", "06", "07", "08", "09"],
        " 11-12": ["11", "12"],
        "37-39": ["37", "38", "39"],
        "41-43": ["41", "42", "43"],
        "45-47": ["45", "46", "47"],
        "52-53": ["52", "53"],
        "55-56": ["55", "56"],
        "64-66": ["64", "65", "66"],
        "84-85": ["84", "85"],
        "86-88": ["86", "87", "88"],
        "90-93": ["90", "91", "92", "93"],
        "94-99": ["94", "95", "96", "97", "98", "99"],
    }

    for key, value in sic_div_combos.items():
        df_combo = df_agg[df_agg["sic_division"].isin(value)]
        value_combo = df_combo[value_col].sum()
        df_combo = pd.DataFrame({"sic_division": [key], value_col: value_combo})
        df_agg = pd.concat([df_agg, df_combo])
        # Drop the individual sic division after combining
        df_agg = df_agg[~df_agg["sic_division"].isin(value)]

    # Merge with formatting mapper
    df_merge = sic_div_detailed.merge(
        df_agg, how="left", left_on="SIC", right_on="sic_division"
    )
    df_merge[value_col] = df_merge[value_col].fillna(0)

    # Sort by ranking for correct order
    df_merge = df_merge.sort_values("ranking", axis=0, ascending=True)

    # Replace 211 column heading with year from config
    df_merge.rename(columns={"211": period}, inplace=True)

    # Select and order the relevant columns
    selected_columns = ["SIC", "Industry description", period, "Notes"]
    df_selected = df_merge[selected_columns]

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_intram_by_sic_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_intram_by_sic/{filename}", df_selected)
