"""The main file for the UK Intram by ITL 1 and 2 output."""

# Standard Library Imports
import logging
import pathlib
import os
import re
from datetime import datetime
from typing import Callable, Dict, Any, Union

# Third Party Imports
import pandas as pd


OutputMainLogger = logging.getLogger(__name__)


def save_detailed_csv(
    df: pd.DataFrame,
    dir: Union[pathlib.Path, str],
    survey_year: str,
    title: str,
    run_id: int,
    write_csv: Callable,
    overwrite: bool = True,
):
    """Save a df as a csv with a detailed filename.

    Args:
        df (pd.DataFrame): The dataframe to save
        dir (Union[pathlib.Path, str]): The directory to save the dataframe to.
        survey_year (str): The year that the data is from (from config).
        title (str): The filename to save the df as (excluding date, run id).
        run_id (int): The current run ID.
        write_csv (Callable): A function to write to a csv file.
        overwrite (bool, optional): Whether or not to overwrite any current
            files saved under the same name. Defaults to True.

    Raises:
        FileExistsError: An error raised if overwrite is false and the file
            already exists.

    """
    date = datetime.now().strftime("%y-%m-%d")
    save_name = f"{survey_year}_{title}_{date}_v{run_id}.csv"
    save_path = os.path.join(dir, save_name)
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


def output_intram_by_itl(
    df_gb: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
    df_ni: pd.DataFrame = None,
):
    """Generate outputs aggregated to ITL levels 1 and 2.

    Args:
        df_gb (pd.DataFrame): GB microdata with weights applied.
        config (Dict[str, Any]): Project config.
        write_csv (Callable): A function to write to a csv file.
        run_id (int): The current run ID.
        postcode_mapper (pd.DataFrame): Postcode to regional code mapping df.
        itl_mapper (pd.DataFrame): Regional code to ITL levels mapping df.
        df_ni (pd.DataFrame): NI microdate (weights are 1), defaults to None.

    """
    # Declare Config Values
    OUTPUT_PATH = config["outputs_paths"]["outputs_master"]
    CURRENT_YEAR = config["years"]["survey_year"]

    # Subset GB Data
    df = df_gb[["postcodes_harmonised", "formtype", "211"]]

    if df_ni is not None:
        if not df_ni.empty:
            # Clean NI data and join
            df_ni["postcodes_harmonised"] = pd.NA
            df_ni = df_ni[["postcodes_harmonised", "formtype", "211"]]
            df = df.append(df_ni, ignore_index=True).copy()

    # Aggregate to ITL2 and ITL1 (Keep 3 and 4 letter codes)
    GEO_COLS = ["ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"]
    BASE_COLS = ["postcodes_harmonised", "formtype", "211"]
    df = df[GEO_COLS + BASE_COLS]
    itl2 = df.groupby(GEO_COLS).agg({"211": "sum"}).reset_index()
    itl1 = itl2.drop(GEO_COLS[:2], axis=1).copy()
    itl1 = itl1.groupby(GEO_COLS[2:]).agg({"211": "sum"}).copy().reset_index()

    # Clean data rady for export
    itl2 = itl2.drop(GEO_COLS[2:], axis=1)
    itl1 = rename_itl(itl1, 1, CURRENT_YEAR)
    itl2 = rename_itl(itl2, 2, CURRENT_YEAR)

    # Export UK outputs
    area = "gb" if df_ni is None else "uk"
    itl_dfs = [itl1, itl2]
    for i, itl_df in enumerate(itl_dfs, start=1):
        output_dir = f"{OUTPUT_PATH}/output_intram_{area}_itl{i}/"
        save_detailed_csv(
            df=itl_df,
            dir=output_dir,
            survey_year=config["years"]["survey_year"],
            title=f"output_intram_{area}_itl{i}",
            run_id=run_id,
            write_csv=write_csv,
            overwrite=True,
        )
