"""The GB SAS for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df, regions
from src.staging.pg_conversion import sic_to_pg_mapper

OutputMainLogger = logging.getLogger(__name__)


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

    # Filter regions for GB only
    df = df[df["region"].isin(regions()["GB"])]

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    df = map_o.join_fgn_ownership(df, ultfoc_mapper)

    # Fill in numeric PG for short forms and imputed long forms
    df = sic_to_pg_mapper(df,
                          sic_pg_num,
                          target_col="pg_numeric",
                          from_col="SIC 2007_CODE",
                          to_col="2016 > Form PG",
                          formtype=["0006", "0001"],
                          )

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df, cora_mapper)

    # Map the sizebands based on frozen employment
    df = map_o.map_sizebands(df)

    #! revert this back
    # # Map the itl regions using the postcodes
    # df = map_o.join_itl_regions(df, postcode_mapper)
    df["itl"] = "TEMP"

    # Map q713 and q714 to numeric format
    df = map_o.map_to_numeric(df)

    # Create C_lnd_bl
    df["C_lnd_bl"] = df["219"] + df["220"]

    # Create ovss_oth
    df["ovss_oth"] = (
        df["243"] + df["244"] + df["245"] + df["246"] + df["247"] + df["249"]
    )

    # Create oth_sc
    df["oth_sc"] = df["242"] + df["248"] + df["250"]


    # Create GB SAS output dataframe with required columns from schema
    schema_path = config["schema_paths"]["gb_sas_schema"]
    schema_dict = load_schema(schema_path)
    output = create_output_df(df, schema_dict)

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_gb_sas_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_gb_sas/{filename}", output)
