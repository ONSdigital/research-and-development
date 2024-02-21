"""The main file for the Intram by PG output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

from src.outputs.outputs_helpers import aggregate_output
import src.outputs.map_output_cols as map_o

OutputMainLogger = logging.getLogger(__name__)


def output_intram_uk_itl2(
    df_gb: pd.DataFrame,
    df_ni: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
):
    """Run the outputs module.

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

    # Join itl level 1
    reg_code = "LAU121CD"
    itl_code = "ITL221CD"
    itl_text = "ITL221NM"
    itl1_mapper = itl_mapper[[reg_code, itl_code, itl_text]]
    df = df.merge(itl1_mapper, how="left", left_on="itl", right_on=reg_code)

    # Group by ITL level 1 and aggregate intram
    value_col = "211"
    agg_method = "sum"
    df_agg = aggregate_output(df, [itl_code, itl_text], [value_col], agg_method)

 
    # # Merge with labels and ranks
    # df_merge = itl2_detailed.merge(
    #     df_agg, how="left", left_on=itl_code, right_on=itl_code
    # )
    # df_merge[value_col] = df_merge[value_col].fillna(0)

    # Sort by ITL2 code
    df_agg = df_agg.sort_values(itl_code, axis=0, ascending=True)

    # Rename the correct columns
    code = "Area Code (ITL2)"
    detail = "Region (ITL2)"
    value_title = "2022 Total q211"
    df_merge = df_agg[[itl_code, itl_text, value_col]].rename(
        columns={
            itl_code: code,
            itl_text: detail,
            value_col: value_title
        }
    )

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_intram_uk_itl2_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_intram_uk_itl2/{filename}", df_merge)
