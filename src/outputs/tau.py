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
    intram_tot_dict: Dict[str, int],
    write_csv: Callable,
    run_id: int,
) -> Dict[str, int]:
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The dataset main with weights not applied
        config (dict): The configuration settings.
        intram_tot_dict (dict): Dictionary with the intramural totals.
        write_csv (Callable): Function to write to a csv file.
          This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    Returns:
        intram_tot_dict (dict): Dictionary with the intramural totals.
    """
    output_path = config["outputs_paths"]["outputs_master"]
    # Prepare the columns needed for outputs:

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df)

    # Map the sizebands based on column "employment"
    df = map_o.map_sizebands(df)

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

    # get Intram toatl with estimation weights applied for gb only
    gb_tau = df.copy().loc[df["formtype"].isin(["0001", "0006"])]

    intram_tot = round((gb_tau["211"] * gb_tau["a_weight"]).sum(), 0)
    intram_tot_dict["GB_Tau_estimated"] = intram_tot

    # Create tau output dataframe with required columns from schema
    schema_path = config["schema_paths"]["tau_schema"]
    schema_dict = load_schema(schema_path)
    tau_output = create_output_df(df, schema_dict)

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    filename = f"{survey_year}_output_tau_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_tau/{filename}", tau_output)

    return intram_tot_dict
