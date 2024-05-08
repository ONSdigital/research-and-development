"""The main file for the UK Intram by ITL 1 and 2 output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o

OutputMainLogger = logging.getLogger(__name__)


def output_intram_uk_itl_1_2(
    df_gb: pd.DataFrame,
    df_ni: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
):
    """Creates and saves total intram value by ITL 1 and 2 levels for the
    entire UK. Combines BERD and NI data, maps regional codes to postcodes,
    then maps regional codes to local authority unit codes. 
    In a for loop, iterating through level 1 and 2, aggregates the intram and
    saves two output CSV files.

    Args:
        df_gb (pd.DataFrame): The GB microdata with weights applied
        df_ ni (pd.DataFrame): The NI microdata; weights are 1
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        postcode_mapper (pd.DataFrame): Maps postcodes to regional codes
        itl_mapper (pd.DataFrame): Maps regional codes to ITL Levels 1 and 2

    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Select columns
    col_needed = ["postcodes_harmonised", "formtype", "211"]
    df_gb = df_gb[col_needed]
    df_ni["postcodes_harmonised"] = pd.NA
    df_ni = df_ni[col_needed]

    # Concatinate GB and NI
    df = df_gb.append(df_ni, ignore_index=True)

    # Join region code
    df = map_o.join_itl_regions(df, postcode_mapper)

    # Name of the value column to be aggregated
    value_col = "211"

    # Name of local authority unit code column in the ITL mapper
    # This shold match the "itl" in postcodes mapper
    # In GB/NI SAS and TAU, this is "reg_code"
    reg_code = "LAU121CD"

    # Dictionary of codes and textual descriptions at itl levels 1 and 2
    itl_levels_dict = {
        1: {"itl_code": "ITL121CD", "itl_text": "ITL121NM"},
        2: {"itl_code": "ITL221CD", "itl_text": "ITL221NM"}
    }

    # Merge the mapper of ITL codes and texts at all levels
    df = df.merge(itl_mapper, how="left", left_on="itl", right_on=reg_code)
    
    # Create aggregates at all levels
    for level in [1, 2]:
        
        # Retrieve the code and text at approppriate level
        itl_code = itl_levels_dict[level]["itl_code"]
        itl_text = itl_levels_dict[level]["itl_text"]
    
        # Group by ITL and aggregate the values by summation
        df_agg = df.groupby(
            [itl_code, itl_text]
        ).agg({value_col: "sum"}).reset_index()

        # Sort by ITL code
        df_agg = df_agg.sort_values(itl_code, axis=0, ascending=True)

        # Rename the code, text and value columns
        code = f"Area Code (ITL{level})"
        detail = f"Region (ITL{level})"
        value_title = "2022 Total q211"
        df_agg = df_agg[[itl_code, itl_text, value_col]].rename(
            columns={
                itl_code: code,
                itl_text: detail,
                value_col: value_title
            }
        )

        # Outputting the CSV file with timestamp and run_id
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"output_intram_uk_itl{level}_{tdate}_v{run_id}.csv"
        write_csv(f"{output_path}/output_intram_uk_itl{level}/{filename}", df_agg)
