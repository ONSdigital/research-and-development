"""The main file for the BERD Intram by PG output."""

import logging
import pandas as pd

from typing import Any
from collections.abc import Callable

from src.utils.helpers import filename_amender

OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_pg(
    gb_df: pd.DataFrame,
    ni_df: pd.DataFrame,
    pg_detailed: pd.DataFrame,
    config: dict[str, Any],
    intram_tot_dict: dict[str, int],
    write_csv: Callable,
    uk_output: bool = False,
) -> dict[str, int]:
    """Run the outputs module.

    Args:
        gb_df (pd.DataFrame): The dataset main
        ni_df (pd.DataFrame): The NI datasets
        pg_detailed (pd.DataFrame): Detailed info for the product groups.
        config (dict): The configuration settings.
        intram_tot_dict (dict): dictionary with the intramural totals.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
        uk_output (bool): If True, the output will include NI data.

    Returns:
        intram_tot_dict (dict): dictionary with the intramural totals.
    """
    df_merge, value_tot = _generate_intarm_by_pg(
        gb_df, ni_df, pg_detailed, config, uk_output
    )

    _save_output_intram_as_csv(df_merge, config, write_csv, uk_output)

    # calculate the intram total for QA across different outputs
    intram_tot_dict[f"intram_by_pg_{'uk' if uk_output else 'gb'}"] = round(value_tot, 0)

    return intram_tot_dict


def _generate_intarm_by_pg(
    gb_df: pd.DataFrame,
    ni_df: pd.DataFrame,
    pg_detailed: pd.DataFrame,
    config: dict[str, Any],
    uk_output: bool = False,
):
    """Generate the intramural by PG output dataframe and intramural by PG total.

    Args:
        gb_df (pd.DataFrame): The GB dataset
        ni_df (pd.DataFrame): The NI dataset
        pg_detailed (pd.DataFrame): Detailed info for the product groups.
        config (dict): The configuration settings.
        uk_output (bool): If True, the output will include NI data.

    Returns:
        df_merge(pd.DataFrame): The intramural by PG output dataframe.
        value_total (int): The intramural by PG total.
    """
    # assign columns for easier use
    key_col = "201"
    value_col = "211"

    if uk_output:
        cols_to_keep = [col for col in gb_df.columns if col in ni_df.columns]
        gb_df = gb_df[cols_to_keep]
        ni_df = ni_df[cols_to_keep]
        # append the NI data to the GB data
        gb_df = pd.concat([gb_df, ni_df], ignore_index=True)

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
    survey_year = config["survey"]["survey_year"]
    df_merge = df_merge[[detail, value_col, notes]].rename(
        columns={value_col: survey_year}
    )
    return df_merge, value_tot


def _save_output_intram_as_csv(
    df_merge: pd.DataFrame,
    config: dict[str, Any],
    write_csv: Callable,
    uk_output: bool = False,
):
    """Save the intramural by PG output as a CSV file.

    Args:
        df_merge (pd.DataFrame): The dataframe to be saved.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
        uk_output (bool): If True, the output will include NI data.

    Returns:
        None
    """

    # Outputting the CSV file
    output_path = config["outputs_paths"]["outputs_master"]

    filename = f"output_intram_by_pg_{'uk' if uk_output else 'gb'}"
    filename = filename_amender(filename, config)

    write_csv(
        f"{output_path}/output_intram_by_pg_{'uk' if uk_output else 'gb'}/{filename}",
        df_merge,
    )
