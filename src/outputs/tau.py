"""The main file for the Tau output module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any
import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df

OutputMainLogger = logging.getLogger(__name__)


def output_tau(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    postcode_itl_mapper: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with weights not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        postcode_itl_mapper (pd.DataFrame): maps the postcode to region code
    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    df = map_o.join_fgn_ownership(df, ultfoc_mapper, formtype=["0001", "0006"])

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df)

    # Map the sizebands based on frozen employment
    df = map_o.map_sizebands(df)

    # Map the itl regions using the postcodes
    df = map_o.join_itl_regions(df, postcode_itl_mapper)

    # Map q713 and q714 to numeric format
    df = map_o.map_to_numeric(df)

    # Create C_lnd_bl
    df["C_lnd_bl"] = df[["219", "220"]].fillna(0).sum(axis=1)

    # Create ovss_oth
    df["ovss_oth"] = (
        df[["243", "244", "245", "246", "247", "249"]].fillna(0).sum(axis=1)
    )

    # Create oth_sc
    df["oth_sc"] = df[["242", "248", "250"]].fillna(0).sum(axis=1)

    # Create tau output dataframe with required columns from schema
    schema_path = config["schema_paths"]["tau_schema"]
    schema_dict = load_schema(schema_path)
    tau_output = create_output_df(df, schema_dict)

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_tau_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_tau/{filename}", tau_output)
