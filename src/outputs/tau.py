"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any
import toml
import pandas as pd

import src.outputs.short_form_out as short
import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema, validate_data_with_schema
from src.outputs.outputs_helpers import create_output_df


OutputMainLogger = logging.getLogger(__name__)

# Get the shortform schema
short_form_schema = toml.load("src/outputs/output_schemas/frozen_shortform_schema.toml")

def run_tau(
    weighted_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    postcode_itl_mapper: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        weighted_df (pd.DataFrame): The dataset with weights not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column


    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    df = map_o.join_fgn_ownership(weighted_df, ultfoc_mapper)

    # Create a columns for numeric product grouo

    # debugging begin
    mapper_df = pd.read_csv(r"R:\BERD Results System Development 2023\DAP_emulation\mappers\pg_alpha_num.csv")
    # debugging end

    df = map_o.join_pg_numeric(df, mapper_df, cols_pg=["201"])

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df, cora_mapper)

    # Map the sizebands based on frozen employment
    df = map_o.map_sizebands(df)

    # Map the itl regions using the postcodes
    df = map_o.join_itl_regions(df, postcode_itl_mapper)

    # Map q713 and q714 to numeric format
    df = map_o.map_to_numeric(df)

    # Create C_lnd_bl
    df["C_lnd_bl"] = df["219"] + df["220"]    

    # Create ovss_oth
    df["ovss_oth"] = df["243"] + df["244"] + df["245"] + df["246"] + df["247"] + df["249"]

    # Create oth_sc
    df["oth_sc"] = df["242"] + df["248"] + df["250"]


    
    # for debugging
    import os
    mydir = r"D:\data\res_dev\outputs"
    myfile = "tau_output_raw.csv"
    mypath = os.path.join(mydir, myfile)
    df.to_csv(mypath, index=None)
    # End of debugging
    
    # Create tau output dataframe with required columns from schema
    schema_path = config["schema_paths"]["tau_schema"]
    tau_output = create_output_df(df, schema_path)

    # debugging start
    myfile = "tau_output_out.csv"
    mypath = os.path.join(mydir, myfile)
    tau_output.to_csv(mypath, index=None)
    # debugging end
  

    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_tau_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_tau/{filename}", tau_output)

