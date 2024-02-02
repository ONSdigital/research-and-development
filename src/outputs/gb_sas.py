"""The GB SAS for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df, regions

GbSasLogger = logging.getLogger(__name__)


def output_gb_sas(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    postcode_mapper: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with estimation weights
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        postcode_mapper (pd.DataFrame): maps the postcode to region code
    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Filter out records that answer "no R&D"
    df = df.copy().loc[~(df["604"] == "No")]

    # Filter regions for GB only
    df1 = df.copy().loc[df["region"].isin(regions()["GB"])]

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    df1 = map_o.join_fgn_ownership(df1, ultfoc_mapper)

    # Map to the CORA statuses from the statusencoded column
    df1 = map_o.create_cora_status_col(df1)

    # Map the sizebands based on frozen employment
    df1 = map_o.map_sizebands(df1)

    # Map the itl regions using the postcodes
    df1 = map_o.join_itl_regions(df1, postcode_mapper)

    # Create C_lnd_bl
    df1["C_lnd_bl"] = df1["219"] + df1["220"]

    # Create ovss_oth
    df1["ovss_oth"] = (
        df1["243"] + df1["244"] + df1["245"] + df1["246"] + df1["247"] + df1["249"]
    )

    # Create oth_sc
    df1["oth_sc"] = df1["242"] + df1["248"] + df1["250"]

    # Create GB SAS output dataframe with required columns from schema
    schema_path = config["schema_paths"]["gb_sas_schema"]
    schema_dict = load_schema(schema_path)
    output = create_output_df(df1, schema_dict)

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_gb_sas_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_gb_sas/{filename}", output)
