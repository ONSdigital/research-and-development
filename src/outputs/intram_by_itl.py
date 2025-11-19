"""The main file for the UK Intram by ITL 1 and 2 output."""

# Standard Library Imports
import logging
import os
import re

from typing import Any
from collections.abc import Callable
from rdsa_utils.typing import PathLike

from src.utils.helpers import filename_amender
from src.utils.breakdown_validation import get_all_wanted_columns

# Third Party Imports
import pandas as pd


OutputMainLogger = logging.getLogger(__name__)


def save_detailed_csv(
    df: pd.DataFrame,
    output_dir: PathLike,
    config: dict[str, Any],
    title: str,
    write_csv: Callable,
    overwrite: bool = True,
) -> dict[str, int]:
    """Save a df as a csv with a detailed filename.

    Args:
        df (pd.DataFrame): The dataframe to save
        output_dir (PathLike): The directory to save the dataframe to.
        survey_year (str): The year that the data is from (from config).
        title (str): The filename to save the df as (excluding date, run id).
        write_csv (Callable): A function to write to a csv file.
        overwrite (bool, optional): Whether or not to overwrite any current
            files saved under the same name. Defaults to True.

    Raises:
        FileExistsError: An error raised if overwrite is false and the file
            already exists.

    Returns:
        dict[str, int]: A dictionary of intramural totals.
    """
    save_name = filename_amender(filename=title, config=config)
    save_path = os.path.join(output_dir, save_name)
    if not overwrite and os.path.exists(save_path):
        raise FileExistsError(
            f"File '{save_path}' already exists. Pass overwrite=True if you "
            "want to overwrite this file."
        )
    write_csv(save_path, df)


def rename_itl(df: pd.DataFrame, itl: int, year) -> pd.DataFrame:
    """Renames ITL columns in a dataframe. Puts current year in total column name.


    Args:
        df (pd.DataFrame): The dataframe containing the ITL columns.
        itl (int): The ITL level.
        year (int): The current year from config.


    Returns:
        pd.DataFrame: A df with the renamed ITL columns.
    """
    renamer = {"211": f"Year {year} Total q211"}
    for col in df.columns:
        cd = re.search(rf"^ITL{itl}[0-9]*CD$", col)
        if cd:
            renamer[cd.group()] = f"Area Code (ITL{itl})"
            continue
        nm = re.search(rf"^ITL{itl}[0-9]*NM$", col)
        if nm:
            renamer[nm.group()] = f"Region (ITL{itl})"
        df = df.rename(mapper=renamer, axis=1)
    return df


def aggregate_itl(
    gb_df: pd.DataFrame,
    ni_df: pd.DataFrame,
    config,
    uk_output: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregates a dataframe to an ITL level.

    Args:
        gb_df (pd.DataFrame): The GB microdata with weights applied.
        ni_df (pd.DataFrame): The NI microdata (weights are 1).
        config (dict[str, Any]): Pipeline configuation settings.
        uk_output (bool, optional): Whether to output UK or GB data. Defaults to False.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: The ITL1 and ITL2 dataframes.
    """
    current_year = config["survey"]["survey_year"]
    geo_cols = config["mappers"]["geo_cols"]
    # return a list of the emp_xx and hc_xx columns
    emp_cols = get_all_wanted_columns(config, "employment_lf")

    base_cols = ["formtype", "211"]
    df = gb_df[base_cols + emp_cols + geo_cols].copy()

    # conditionally include NI responses to produce UK
    if uk_output:
        ni_df = ni_df[base_cols + emp_cols + geo_cols].copy()

        df = pd.concat([df, ni_df], ignore_index=True).copy()

    # Create the aggregation dictionary
    agg_dict = {"211": "sum"}
    agg_dict.update({col: "sum" for col in emp_cols})

    # Aggregate to ITL2 and ITL1 (Keep 3 and 4 letter codes)
    itl2 = df.groupby(geo_cols).agg(agg_dict).reset_index()
    itl1 = itl2.drop(geo_cols[:2], axis=1).copy()
    itl1 = itl1.groupby(geo_cols[2:]).agg(agg_dict).copy().reset_index()

    # Clean data ready for export
    itl2 = itl2.drop(geo_cols[2:], axis=1)
    itl1 = rename_itl(itl1, 1, current_year)
    itl2 = rename_itl(itl2, 2, current_year)

    return itl1, itl2


def output_intram_by_itl(
    gb_df: pd.DataFrame,
    ni_df: pd.DataFrame,
    config: dict[str, Any],
    intram_tot_dict: dict[str, int],
    write_csv: Callable,
    uk_output: bool = False,
):
    """Generate outputs aggregated to ITL levels 1 and 2.

    Args:
        gb_df (pd.DataFrame): GB microdata with weights applied.
        ni_df (pd.DataFrame): NI microdata (weights are 1),
        config (dict[str, Any]): Project config.
        intram_tot_dict (dict[str, int]): dictionary with the intramural totals.
        write_csv (Callable): A function to write to a csv file.
        uk_output (bool, optional): Whether to output UK or GB data. Defaults to False.
    """
    # Declare Config Values
    OUTPUT_PATH = config["outputs_paths"]["outputs_master"]

    # Aggregate to ITL2 and ITL1 (Keep 3 and 4 letter codes)
    itl1, itl2 = aggregate_itl(gb_df, ni_df, config, uk_output)

    # Export UK outputs
    area = "gb" if not uk_output else "uk"
    itl_dfs = [itl1, itl2]
    for i, itl_df in enumerate(itl_dfs, start=1):
        # update the dictionary of intramural totals
        # get the name of the column which contains the string "211"
        col_name = itl_df.columns[itl_df.columns.str.contains("211")][0]
        intram_tot_dict[f"{area}_itl{i}"] = round(itl_df[col_name].sum(), 0)

        # Save the ITL data

        output_dir = f"{OUTPUT_PATH}/output_intram_{area}_itl{i}/"
        save_detailed_csv(
            itl_df,
            output_dir,
            config,
            f"output_intram_{area}_itl{i}",
            write_csv,
            overwrite=True,
        )

    return intram_tot_dict
