"""The NI SAS for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any
import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df
from src.staging.pg_conversion import sic_to_pg_mapper

OutputMainLogger = logging.getLogger(__name__)


def output_ni_sas(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    sic_pg_num: pd.DataFrame,
):

    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with estimation weights
        ni_full_responses (pd.DataFrame): NI dataset
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column
        postcode_mapper (pd.DataFrame): maps the postcode to region code
        pg_alpha_num (pd.DataFrame): mapper of numeric PG to alpha PG

    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Prepare the columns needed for outputs:

    # Fill in numeric PG where missing
    df = sic_to_pg_mapper(
        df,
        sic_pg_num,
        target_col="pg_numeric",
        from_col="SIC 2007_CODE",
        to_col="2016 > Form PG",
        formtype=["0003"],
    )

    # Map the sizebands based on frozen employment
    df = map_o.map_sizebands(df)

    # Create C_lnd_bl
    df["C_lnd_bl"] = df["219"] + df["220"]

    # Create ovss_oth
    df["ovss_oth"] = (
        df["243"] + df["244"] + df["245"] + df["246"] + df["247"] + df["249"]
    )

    # Create oth_sc
    df["oth_sc"] = df["242"] + df["248"] + df["250"]

    # Create NI SAS output dataframe with required columns from schema
    schema_path = config["schema_paths"]["ni_sas_schema"]
    schema_dict = load_schema(schema_path)
    output = create_output_df(df, schema_dict)

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_ni_sas_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_ni_sas/{filename}", output)
