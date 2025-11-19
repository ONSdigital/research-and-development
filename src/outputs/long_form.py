"""The main file for the Outputs module."""

import logging
import pandas as pd

from typing import Any
from collections.abc import Callable

import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df
from src.utils.helpers import filename_amender

OutputMainLogger = logging.getLogger(__name__)


def output_long_form(
    df: pd.DataFrame,
    config: dict[str, Any],
    write_csv: Callable,
):
    """Run the outputs module on long forms.

    Args:
        df (pd.DataFrame): The main dataset for long form output
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
         ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.

    """
    output_path = config["outputs_paths"]["outputs_master"]

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df)

    # Filter for long-forms/NI (status mapping has already been done)
    df = df.loc[((df["formtype"] == "0001") | (df["formtype"] == "0003"))]

    # Create long form output dataframe with required columns from schema
    schema_path = config["schema_paths"]["long_form_schema"]
    schema_dict = load_schema(schema_path)
    longform_output = create_output_df(df, schema_dict)

    filename = filename_amender("long_form", config)
    write_csv(f"{output_path}/output_long_form/{filename}", longform_output)
