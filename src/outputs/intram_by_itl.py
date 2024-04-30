"""The main file for the UK Intram by ITL 1 and 2 output."""

# Standard Library Imports
import logging
import pathlib
import os
from datetime import datetime
from typing import Callable, Dict, Any, Union

# Third Party Imports
import pandas as pd

# Local Imports
import src.outputs.map_output_cols as map_o

OutputMainLogger = logging.getLogger(__name__)


def save_detailed_csv(
        df: pd.DataFrame,
        dir: Union[pathlib.Path, str],
        title: str,
        run_id: int,
        write_csv: Callable,
        overwrite: bool = True
        ): 
    """Save a df as a csv with a detailed filename.

    Args:
        df (pd.DataFrame): The dataframe to save
        dir (Union[pathlib.Path, str]): The directory to save the dataframe to.
        title (str): The filename to save the df as (excluding data, run id).
        run_id (int): The current run ID.
        write_csv (Callable): A function to write to a csv file.
        overwrite (bool, optional): Whether or not to overwrite any current 
            files saved under the same name. Defaults to True.

    Raises:
        FileExistsError: An error raised if overwrite is false and the file
            already exists.

    """
    date = datetime.now().strftime("%Y-%m-%d")
    save_name = f"{title}_{date}_v{run_id}.csv"
    save_path = os.path.join(dir, save_name)
    if not overwrite and os.path.exists(save_path):
        raise FileExistsError(
            f"File '{save_path}' already exists. Pass overwrite=True if you "
            "want to overwrite this file."
        )
    write_csv(save_path, df)


def rename_itl(df: pd.DataFrame, itl: int) -> pd.DataFrame:
    """Rename ITL columns in a dataframe.

    Args:
        df (pd.DataFrame): The dataframe containing the ITL columns.
        itl (int): The ITL level.

    Returns:
        pd.DataFrame: A df with the renamed ITL columns.
    """
    renamer = {
        f"ITL{itl}21CD": f"Area Code (ITL{itl})",
        f"ITL{itl}21NM": f"Region (ITL{itl})",
        "211": "Year Total q211"
    }
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
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    OUTPUT_PATH = config[f"{NETWORK_OR_HDFS}_paths"]["output_path"]
    
    # Subset GB Data
    df_gb = df_gb[["postcodes_harmonised", "formtype", "211"]]

    if df_ni is not None:
        # Clean NI data and join
        df_ni["postcodes_harmonised"] = pd.NA
        df_ni = df_ni[["postcodes_harmonised", "formtype", "211"]]
        df_gb = df_gb.append(df_ni, ignore_index=True).copy()
    
    df = df_gb

    # Join Aggregation Cols
    df = map_o.join_itl_regions(df, postcode_mapper)
    df = df.merge(itl_mapper, how="left", left_on="itl", right_on="LAU121CD")

    # Aggregate to ITL2 and ITL1 (Keep 3 and 4 letter codes)
    GEO_COLS = ["ITL221CD", "ITL221NM", "ITL121CD", "ITL121NM"]
    BASE_COLS = ["postcodes_harmonised", "formtype", "211"]
    df = df[GEO_COLS + BASE_COLS]
    itl2 = df.groupby(GEO_COLS).agg({"211": "sum"}).reset_index()
    itl1 = itl2.drop(GEO_COLS[:2], axis=1).copy()
    itl1 = itl1.groupby(GEO_COLS[2:]).agg({"211": "sum"}).copy().reset_index()

    # Clean data rady for export
    itl2.drop(GEO_COLS[2:], axis=1, inplace=True)
    itl1 = rename_itl(itl1, 1)
    itl2 = rename_itl(itl2, 2)

    # Export UK outputs
    area = "gb" if not df_ni is not None else "uk"
    itl_dfs = [{"1": itl1}, {"2": itl2}]
    for itl in itl_dfs:
        i = [k for k in itl][0]
        itl_df = itl[i]
        output_dir = f"{OUTPUT_PATH}/output_intram_{area}_itl{i}/"
        save_detailed_csv(
            df=itl_df,
            dir=output_dir,
            title=f"output_intram_{area}_itl{i}",
            run_id=run_id,
            write_csv=write_csv,
            overwrite=True
        )
    
    


