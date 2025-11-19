"""The GB SAS for the Outputs module."""

import logging
import pandas as pd
from typing import Any
from collections.abc import Callable

import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.outputs.outputs_helpers import create_output_df, regions
from src.utils.helpers import filename_amender

GbSasLogger = logging.getLogger(__name__)


def output_gb_sas(
    df: pd.DataFrame,
    config: dict[str, Any],
    intram_tot_dict: dict[str, int],
    write_csv: Callable,
) -> dict[str, int]:
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with estimation weights
        config (dict): The configuration settings.
        intram_tot_dict (dict): dictionary with the intramural totals.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
    """
    output_path = config["outputs_paths"]["outputs_master"]

    # Filter regions for GB only
    df1 = df.copy().loc[df["region"].isin(regions()["GB"])]

    # Prepare the columns needed for outputs:

    # Map to the CORA statuses from the statusencoded column
    df1 = map_o.create_cora_status_col(df1)

    # Map the sizebands based on frozen employment
    df1 = map_o.map_sizebands(df1)

    # Create C_lnd_bl
    df1["C_lnd_bl"] = df1[["219", "220"]].fillna(0).sum(axis=1)

    # Create ovss_oth
    df1["ovss_oth"] = (
        df1[["243", "244", "245", "246", "247", "249"]].fillna(0).sum(axis=1)
    )

    # Create oth_sc
    df1["oth_sc"] = df1[["242", "248", "250"]].fillna(0).sum(axis=1)

    # caluclate the intram total for QA across different outputs
    intram_tot_dict["GB_sas"] = round(df1["211"].sum(), 0)

    # Create GB SAS output dataframe with required columns from schema
    schema_path = config["schema_paths"]["gb_sas_schema"]
    schema_dict = load_schema(schema_path)
    output = create_output_df(df1, schema_dict)

    # Outputting the CSV file with
    filename = filename_amender("output_gb_sas", config)
    write_csv(f"{output_path}/output_gb_sas/{filename}", output)

    return intram_tot_dict
