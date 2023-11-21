"""The main file for the Imputation module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime
from itertools import chain

from src.imputation.apportionment import run_apportionment
from src.imputation.short_to_long import run_short_to_long
from src.imputation.MoR import run_mor
from src.imputation.sf_expansion import run_sf_expansion
from src.imputation import tmi_imputation as tmi

ImputationMainLogger = logging.getLogger(__name__)


def add_trim_column(df: pd.DataFrame, column_name: str = 'manual_trim', value: bool = False) -> pd.DataFrame:
    """
    Adds a new column to a DataFrame with a default value.

    Args:
        df (pd.DataFrame): The DataFrame to add the new column to.
        column_name (str, optional): The name of the new column. Defaults to 'manual_trim'.
        value (bool, optional): The default value for the new column. Defaults to False.

    Returns:
        pd.DataFrame: The DataFrame with the new column added.

    Raises:
        ValueError: If the DataFrame is empty or the column already exists in the DataFrame.
    """
    if df.empty:
        raise ValueError("The DataFrame is empty.")
    
    if column_name in df.columns:
        raise ValueError(f"A column with name {column_name} already exists in the DataFrame.")

    df[column_name] = value
    return df


def get_latest_csv(directory: str) -> str:
    """
    Gets the latest CSV file in a directory.

    Args:
        directory (str): The directory to search for CSV files.

    Returns:
        str: The path of the latest CSV file, or an empty string if no CSV files are found.
    """
    list_of_files = glob.glob(os.path.join(directory, "*.csv"))  # get list of all csv files
    if not list_of_files:  # if list is empty, return empty string
        return None
    latest_file = max(list_of_files, key=os.path.getctime) # get the latest file
    
    return latest_file

# check if any files are in imputation/manual_trimming and check if load_manual_imputation is True
# if so load the file and any records which are marked True in the manual_trim column will be 
# excluded from the imputation process and will be output as is. They will be marked as 'manual_trim' in the imp_marker column
def load_manual_imputation(config: Dict[str, Any], 
                           df: pd.DataFrame,
                           isfile_func: callable) -> pd.DataFrame:
    """
    Loads a manual trimming file if it exists and adds a manual_trim column to the DataFrame.

    Args:
        config (Dict[str, Any]): The configuration dictionary.
        df (pd.DataFrame): The dataframe to be imputed.
        isfile_func (callable): The function to use to check if the file exists.
    Returns:
        pd.DataFrame: The DataFrame with the manual_trim column added.
    """
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    imp_path = config[f"{NETWORK_OR_HDFS}_paths"]["imputation_path"]

    new_man_trim_file = get_latest_csv(f"{imp_path}/manual_trimming/")
    
    if config["global"]["load_manual_imputation"] and isfile_func(new_man_trim_file):
        manual_trim_df = pd.read_csv(new_man_trim_file)
        df = df.merge(manual_trim_df, on=["reference", "instance"], how="left")
        df = df.drop(columns=["manual_trim_x"])
        df = df.rename(columns={"manual_trim_y": "manual_trim"})
        df["manual_trim"] = df["manual_trim"].fillna(False)
    else:
        df = add_trim_column(df)
    return df


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    backdata: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:

    # Get the target values and breakdown columns from the config
    lf_target_vars = config["imputation"]["lf_target_vars"]
    sum_cols = config["imputation"]["sum_cols"]
    bd_qs_lists = list(config["breakdowns"].values())
    bd_cols = list(chain(*bd_qs_lists))

    # Apportion cols 4xx and 5xx to create FTE and headcount values
    df = run_apportionment(df)
        
    # Convert shortform responses to longform format
    df = run_short_to_long(df)

    # Initialise imp_marker column with a value of 'R' for clear responders
    # and a default value "no_imputation" for all other rows for now.
    clear_responders_mask = df.status.isin(["Clear", "Clear - overridden"])
    df.loc[clear_responders_mask, "imp_marker"] = "R"
    df.loc[~clear_responders_mask, "imp_marker"] = "no_imputation"

    # remove records that have had construction applied before imputation
    if "is_constructed" in df.columns:
        constructed_df = df.copy().loc[df["is_constructed"].isin([True])]
        constructed_df["imp_marker"] = "constructed"

        df = df.copy().loc[~df["is_constructed"].isin([True])]

    # Create new columns to hold the imputed values
    orig_cols = lf_target_vars + bd_cols + sum_cols
    for col in orig_cols:
        df[f"{col}_imputed"] = df[col]

    # Run MoR
    if backdata is not None:
        df, links_df = run_mor(df, backdata, orig_cols, lf_target_vars, config)

    # Run TMI for long forms and short forms
    imputed_df, qa_df = tmi.run_tmi(df, mapper, config)

    # Run short form expansion
    imputed_df = run_sf_expansion(imputed_df, config)

    # join constructed rows back to the imputed df
    if "is_constructed" in df.columns:
        imputed_df = pd.concat(imputed_df, constructed_df)
        imputed_df = imputed_df.sort_values(
            ["reference", "instance"], ascending=[True, True]
        ).reset_index(drop=True)

    # Output QA files
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    imp_path = config[f"{NETWORK_OR_HDFS}_paths"]["imputation_path"]

    # Add a manual_trim column to the QA df
    qa_df = add_column(qa_df, column_name="manual_trim", value=False)

    if config["global"]["output_imputation_qa"]:
        ImputationMainLogger.info("Outputting Imputation files.")
        tdate = datetime.now().strftime("%Y-%m-%d")
        trim_qa_filename = f"trimming_qa_{tdate}_v{run_id}.csv"
        links_filename = f"links_qa_{tdate}_v{run_id}.csv"
        full_imp_filename = f"full_responses_imputed_{tdate}_v{run_id}.csv"
        write_csv(f"{imp_path}/imputation_qa/{trim_qa_filename}", qa_df)
        write_csv(f"{imp_path}/imputation_qa/{links_filename}", links_df)
        write_csv(f"{imp_path}/imputation_qa/{full_imp_filename}", imputed_df)
    ImputationMainLogger.info("Finished Imputation calculation.")

    # Create names for imputed cols
    imp_cols = [f"{col}_imputed" for col in orig_cols]

    # Update the original breakdown questions and target variables with the imputed
    imputed_df[orig_cols] = imputed_df[imp_cols]

    # Drop imputed values from df
    imputed_df = imputed_df.drop(columns=imp_cols)

    return imputed_df
