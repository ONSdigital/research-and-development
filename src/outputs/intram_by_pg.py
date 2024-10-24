"""The main file for the BERD Intram by PG output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_pg(
    gb_df: pd.DataFrame,
    ni_df: pd.DataFrame,
    pg_detailed: pd.DataFrame,
    config: Dict[str, Any],
    intram_tot_dict: Dict[str, int],
    write_csv: Callable,
    run_id: int,
    uk_output: bool = False,
) -> Dict[str, int]:
    """Run the outputs module.

    Args:
        gb_df (pd.DataFrame): The dataset main
        ni_df (pd.DataFrame): The NI datasets
        pg_detailed (pd.DataFrame): Detailed info for the product groups.
        config (dict): The configuration settings.
        intram_tot_dict (dict): Dictionary with the intramural totals.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        uk_output (bool): If True, the output will include NI data.

    Returns:
        intram_tot_dict (dict): Dictionary with the intramural totals.
    """
    # assign columns for easier use
    key_col = "201"
    value_col = "211"

    if uk_output:
        cols_to_keep = [col for col in gb_df.columns if col in ni_df.columns]
        gb_df = gb_df[cols_to_keep]
        ni_df = ni_df[cols_to_keep]
        # append the NI data to the GB data
        gb_df = gb_df.append(ni_df)

    # Group by PG and aggregate intram
    df_agg = gb_df.groupby([key_col]).agg({value_col: "sum"}).reset_index()

    # Create Total and concatinate it to df_agg
    value_tot = df_agg[value_col].sum()
    df_tot = pd.DataFrame({key_col: ["total"], value_col: value_tot})
    df_agg = pd.concat([df_agg, df_tot])

    # Merge with labels and ranks
    df_merge = pg_detailed.merge(
        df_agg, how="left", left_on="pg_alpha", right_on=key_col
    )
    df_merge[value_col] = df_merge[value_col].fillna(0)

    # Sort by rank
    df_merge.sort_values("ranking", axis=0, ascending=True)

    # Select and rename the correct columns
    detail = "Detailed product groups (Alphabetical product groups A-AH)"
    notes = "Notes"
    value_title = "2023 (Current period)"
    df_merge = df_merge[[detail, value_col, notes]].rename(
        columns={value_col: value_title}
    )

    # calculate the intram total for QA across different outputs
    intram_tot_dict[f"intram_by_pg_{'uk' if uk_output else 'gb'}"] = round(value_tot, 0)

    save_file_as_csv(df_merge, config, write_csv, run_id, uk_output)

    return intram_tot_dict

def save_file_as_csv(
        df_merge,
        config,
        write_csv,
        run_id,
        uk_output):
    # Outputting the CSV file with timestamp and run_id
    output_path = config["outputs_paths"]["outputs_master"]

    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    filename = (
        f"{survey_year}_output_intram_by_pg_{'uk' if uk_output else 'gb'}"
        f"_{tdate}_v{run_id}.csv"
    )
    write_csv(
        f"{output_path}/output_intram_by_pg_{'uk' if uk_output else 'gb'}/{filename}",
        df_merge,
    )