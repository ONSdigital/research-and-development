"""The GB SAS for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df, regions
from src.staging.pg_conversion import sic_to_pg_mapper

GbSasLogger = logging.getLogger(__name__)


def output_gb_sas(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    postcode_mapper: pd.DataFrame,
    sic_pg_num: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with estimation weights
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column
        postcode_mapper (pd.DataFrame): maps the postcode to region code
        pg_alpha_num (pd.DataFrame): mapper of numeric PG to alpha PG

    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    GbSasLogger.debug(f"1: df shape: {df.shape}")
    GbSasLogger.debug(f"1: 211 total: {df['211'].sum()}")
    # Filter out records that answer "no R&D"
    df = df.copy().loc[~((df["604"] == "No") & (df["211"] > 0))]
    GbSasLogger.debug(f"2: df shape: {df.shape}")
    GbSasLogger.debug(f"2: 211 total: {df['211'].sum()}")

    # Filter regions for GB only
    df = df.copy().loc[df["region"].isin(regions()["GB"])]
    GbSasLogger.debug(f"3: df shape: {df.shape}")
    GbSasLogger.debug(f"3: 211 total: {df['211'].sum()}")

    # Prepare the columns needed for outputs:

    GbSasLogger.debug(f"4: df shape: {df.shape}")
    # Join foriegn ownership column using ultfoc mapper
    df = map_o.join_fgn_ownership(df, ultfoc_mapper)
    GbSasLogger.debug(f"5: df shape: {df.shape}")
    GbSasLogger.debug(f"5: 211 total: {df['211'].sum()}")

    # Fill in numeric PG for short forms and imputed long forms
    df = sic_to_pg_mapper(
        df,
        sic_pg_num,
        target_col="pg_numeric",
        from_col="SIC 2007_CODE",
        to_col="2016 > Form PG",
        formtype=["0006", "0001"],
    )

    GbSasLogger.debug(f"6: df shape: {df.shape}")
    GbSasLogger.debug(f"6: 211 total: {df['211'].sum()}")

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df, cora_mapper)
    GbSasLogger.debug(f"7: df shape: {df.shape}")
    GbSasLogger.debug(f"7: 211 total: {df['211'].sum()}")

    # Map the sizebands based on frozen employment
    df = map_o.map_sizebands(df)
    GbSasLogger.debug(f"8: df shape: {df.shape}")
    GbSasLogger.debug(f"8: 211 total: {df['211'].sum()}")

    # Map the itl regions using the postcodes
    df = map_o.join_itl_regions(df, postcode_mapper)
    GbSasLogger.debug(f"9: df shape: {df.shape}")
    GbSasLogger.debug(f"9: 211 total: {df['211'].sum()}")

    # Create C_lnd_bl
    df["C_lnd_bl"] = df["219"] + df["220"]
    GbSasLogger.debug(f"10: df shape: {df.shape}")
    GbSasLogger.debug(f"10: 211 total: {df['211'].sum()}")

    # Create ovss_oth
    df["ovss_oth"] = (
        df["243"] + df["244"] + df["245"] + df["246"] + df["247"] + df["249"]
    )
    GbSasLogger.debug(f"11: df shape: {df.shape}")
    GbSasLogger.debug(f"11: 211 total: {df['211'].sum()}")

    # Create oth_sc
    df["oth_sc"] = df["242"] + df["248"] + df["250"]
    GbSasLogger.debug(f"12: df shape: {df.shape}")
    GbSasLogger.debug(f"12: 211 total: {df['211'].sum()}")

    # Create GB SAS output dataframe with required columns from schema
    schema_path = config["schema_paths"]["gb_sas_schema"]
    schema_dict = load_schema(schema_path)
    output = create_output_df(df, schema_dict)
    GbSasLogger.debug(f"13: df shape: {df.shape}")
    GbSasLogger.debug(f"13: 211 total: {df['211'].sum()}")

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_gb_sas_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_gb_sas/{filename}", output)
