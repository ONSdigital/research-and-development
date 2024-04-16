"""The main file for the BERD Intram by ITL 1 output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o

OutputMainLogger = logging.getLogger(__name__)


def output_intram_by_itl1(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    postcode_mapper: pd.DataFrame,
    itl_mapper: pd.DataFrame,
    itl1_detailed: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with weights not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        postcode_mapper (pd.DataFrame): Maps postcodes to regional codes
        itl_mapper (pd.DataFrame): Maps regional codes to ITL Level 1
        itl1_detailed (pd.DataFrame): Details of ITL Level 1

    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Join region code
    df = map_o.join_itl_regions(df, postcode_mapper)

    # Join itl level 1
    reg_code = "LAU121CD"
    itl_code = "ITL121CD"
    itl1_mapper = itl_mapper[[reg_code, itl_code]]
    df = df.merge(itl1_mapper, how="left", left_on="itl", right_on=reg_code)

    # Group by ITL level 1 and aggregate intram
    value_col = "211"
    agg_method = "sum"
    df_agg = df.groupby([itl_code]).agg("sum")

    # Create UK total
    value_uk = df_agg[value_col].sum()
    df_uk = pd.DataFrame({itl_code: ["TLA"], value_col: value_uk})

    # Create England total
    itls_eng = ["TLC", "TLD", "TLE", "TLF", "TLG", "TLH", "TLI", "TLJ", "TLK"]
    df_eng = df_agg[df_agg[itl_code].isin(itls_eng)]
    value_eng = df_eng[value_col].sum()
    df_eng = pd.DataFrame({itl_code: ["TLB"], value_col: value_eng})

    # Concatinate totals
    df_agg = pd.concat([df_agg, df_uk])
    df_agg = pd.concat([df_agg, df_eng])

    # Merge with labels and ranks
    df_merge = itl1_detailed.merge(
        df_agg, how="left", left_on=itl_code, right_on=itl_code
    )
    df_merge[value_col] = df_merge[value_col].fillna(0)

    # Sort by rank
    df_merge = df_merge.sort_values("ranking", axis=0, ascending=True)

    # Select and rename the correct columns
    code = "Area Code (ITL1)"
    detail = "Country or region (ITL1)"
    notes = "Notes"
    value_title = "2023"
    df_merge = df_merge[[code, detail, value_col, notes]].rename(
        columns={value_col: value_title}
    )

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_intram_by_itl1_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_intram_by_itl1/{filename}", df_merge)
