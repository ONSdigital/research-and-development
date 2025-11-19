"""The main file for the BERD Intram by Civil or Defence output."""

import logging
import pandas as pd

from typing import Any
from collections.abc import Callable

from src.utils.helpers import filename_amender

OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_civil_defence(
    df: pd.DataFrame,
    config: dict[str, Any],
    write_csv: Callable,
) -> pd.DataFrame:
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main.
        df_for_output (pd.DataFrame): The summed dataset for output
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
    Returns:
        None.
    """

    df_for_output = generate_intram_by_civil_defence(df)

    # Outputting the CSV file
    _save_output_intram_civil_def_as_csv(df_for_output, config, write_csv)

    return None


def generate_intram_by_civil_defence(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate the intramural.
    Args:
        df (pd.DataFrame): The dataset main.
    Returns: df_for output (pd.Dataframe: Total intram expenditure by Civil/Defence)
    """
    # Generating the Total Intramural Expenditure by Civil or Defence

    # Group by civil/defence (200) and aggregate intram (211)
    key_col = "200"
    value_col = "211"

    df_agg = df.groupby([key_col]).agg({value_col: "sum"}).reset_index()

    # Replace C and D with Civil or Defence
    df_agg["200"] = df_agg["200"].replace({"C": "Civil", "D": "Defence"})

    # Rename Columns with dictionary
    columns = {"200": "Catergory", "211": "Total Intramural Expenditure"}

    df_for_output = df_agg.rename(columns=columns)

    return df_for_output


def _save_output_intram_civil_def_as_csv(
    df_for_output: pd.DataFrame,
    config: dict[str, Any],
    write_csv: Callable,
):
    """Save the intramural by civil_defence output as a CSV file.

    Args:
        df_for_output(pd.DataFrame): The dataframe to be saved.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.

    Returns:
        None
    """

    # Outputting the CSV file
    output_path = config["outputs_paths"]["outputs_master"]

    filename = filename_amender(
        filename="output_intram_by_civil_defence", config=config
    )

    write_csv(f"{output_path}/output_intram_by_civil_defence/{filename}", df_for_output)
