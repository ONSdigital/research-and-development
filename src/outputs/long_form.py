"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df, create_period_year

OutputMainLogger = logging.getLogger(__name__)


def output_long_form(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
):
    """Run the outputs module on long forms.

    Args:
        df (pd.DataFrame): The main dataset for long form output
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

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df, cora_mapper)

    # Filter for long-forms/NI (status mapping has already been done)
    df = df.loc[((df["formtype"] == "0001") | (df["formtype"] == "0003"))]

    # Create a 'year' column
    df = create_period_year(df)

    # Join foriegn ownership column using ultfoc mapper
    df = map_o.join_fgn_ownership(df, ultfoc_mapper)

    # Create long form output dataframe with required columns from schema
    schema_path = config["schema_paths"]["longform_schema"]
    schema_dict = load_schema(schema_path)
    longform_output = create_output_df(df, schema_dict)

    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"long_form_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_long_form/{filename}", longform_output)
