"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any
import toml

import src.outputs.short_form_out as short
import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema, validate_data_with_schema


OutputMainLogger = logging.getLogger(__name__)

# Get the shortform schema
short_form_schema = toml.load("src/outputs/output_schemas/frozen_shortform_schema.toml")

# Get the Tau Argus output schema
tau_schema = toml.load("src/outputs/output_schemas/tau_schema.toml")

def create_output_df(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """Creates the dataframe for outputs with
    the required columns. The naming of the columns comes
    from the schema provided.

    Args:
        df (pd.DataFrame): Dataframe containing all columns
        schema (str): Toml schema containing the old and new
        column names for the outputs

    Returns:
        (pd.DataFrame): A dataframe consisting of only the
        required short form output data
    """
    # Load schema using pre-built function
    output_schema = load_schema(schema)

    # Create dict of current and required column names
    colname_dict = {
        output_schema[column_nm]["old_name"]: column_nm
        for column_nm in output_schema.keys()
    }

    # Create subset dataframe with only the required outputs
    output_df = df[df.columns.intersection(colname_dict.keys())]

    # Rename columns to match the output specification
    output_df.rename(
        columns={key: colname_dict[key] for key in colname_dict}, inplace=True
    )

    # Rearrange to match user defined output order
    output_df = output_df[colname_dict.values()]

    # Validate datatypes before output
    validate_data_with_schema(output_df, schema)

    return output_df


def run_short_output(
    estimated_df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    postcode_itl_mapper: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        estimated_df (pd.DataFrame): The main dataset contains short form output
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column


    """

    OutputMainLogger.info("Starting short form output...")

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    estimated_df = map_o.join_fgn_ownership(estimated_df, ultfoc_mapper)

    # Map to the CORA statuses from the statusencoded column
    estimated_df = map_o.create_cora_status_col(estimated_df, cora_mapper)

    # Map the sizebands based on frozen employment
    estimated_df = map_o.map_sizebands(estimated_df)

    # Map the itl regions using the postcodes
    estimated_df = map_o.join_itl_regions(estimated_df, postcode_itl_mapper)

    # Map q713 and q714 to numeric format
    estimated_df = map_o.map_to_numeric(estimated_df)

    # Prepare the shortform output dataframe
    short_form_df = short.run_shortform_prep(estimated_df, round_val=4)

    # Create short form output dataframe with required columns from schema
    schema_path = config["schema_paths"]["frozen_shortform_schema"]
    shortform_output = create_output_df(short_form_df, schema_path)

    if config["global"]["output_short_form"]:
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"output_short_form_{tdate}_v{run_id}.csv"
        write_csv(f"{output_path}/output_short_form/{filename}", shortform_output)
    OutputMainLogger.info("Finished short form output.")

def run_tau_output(
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
        weighted_df (pd.DataFrame): The main dataset contains long and short responses with weights
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column


    """

    OutputMainLogger.info("Starting Tau output...")

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    weighted_df = map_o.join_fgn_ownership(weighted_df, ultfoc_mapper)

    # Map to the CORA statuses from the statusencoded column
    # estimated_df = map_o.create_cora_status_col(estimated_df, cora_mapper)

    # Map the sizebands based on frozen employment
    weighted_df = map_o.map_sizebands(weighted_df)

    # Map the itl regions using the postcodes
    weighted_df = map_o.join_itl_regions(weighted_df, postcode_itl_mapper)

    # Map q713 and q714 to numeric format
    weighted_df = map_o.map_to_numeric(weighted_df)

    # Prepare the shortform output dataframe
    #short_form_df = short.run_shortform_prep(estimated_df, round_val=4)

    # Create short form output dataframe with required columns from schema
    schema_path = config["schema_paths"]["tau_schema"]
    tau_output = create_output_df(weighted_df, schema_path)

    if config["global"]["output_tau"]:
        tdate = datetime.now().strftime("%Y-%m-%d")
        filename = f"output_tau_{tdate}_v{run_id}.csv"
        write_csv(f"{output_path}/output_tau/{filename}", tau_output)
    OutputMainLogger.info("Finished Tau output.")
