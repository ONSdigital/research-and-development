"""Functions for the Mean of Ratios (MoR) methods."""
import pandas as pd
import re

from src.imputation.apportionment import run_apportionment

good_statuses = ["Clear", "Clear - overridden"]
bad_statuses = ["Form sent out", "Check needed"]


def run_mor(df, backdata, target_vars):
    """Function to implement Mean of Ratios method.

    This is implemented by first carrying forward data from last year
    for non-responders, and then calculating and applying growth rates
    for each imputation class.

    Args:
        df (pd.DataFrame): Processed full responses DataFrame
        backdata (pd.DataFrame): One period of backdata.
        target_vars ([string]): List of variables to impute.

    Returns:
        pd.DataFrame: df with MoR applied
    """
    to_impute_df, remainder_df, backdata = mor_preprocessing(df, backdata)

    # Carry forwards method
    carried_forwards_df = carry_forwards(to_impute_df, backdata, target_vars)

    # TODO Remove the `XXX_prev` columns (left in for QA)
    return pd.concat([remainder_df, carried_forwards_df])


def mor_preprocessing(df, backdata):
    """Apply pre-processing ready for MoR

    Args:
        df (pd.DataFrame): full responses for the current year
        backdata (pd.Dataframe): backdata file read in during staging.
    """
    # TODO move this to imputation main
    # Select only values to be imputed and remove duplicate instances
    imputation_condition = (
        (df["formtype"] == "0001")
        & (df["status"].isin(bad_statuses))
        & (df["instance"] == 0 | pd.isnull(df["instance"]))
    )
    to_impute_df = df.copy().loc[imputation_condition, :]
    remainder_df = df.copy().loc[~imputation_condition, :]

    # Convert backdata column names from qXXX to XXX
    p = re.compile(r"q\d{3}")
    cols = [col for col in list(backdata.columns) if p.match(col)]
    to_rename = {col: col[1:] for col in cols}
    backdata = backdata.rename(columns=to_rename)

    backdata = run_apportionment(backdata)
    # Only pick up useful backdata
    backdata = backdata.loc[(backdata["status"].isin(good_statuses)), :]

    return to_impute_df, remainder_df, backdata


def carry_forwards(df, backdata, target_vars):
    """Carry forwards matcing `backdata` values into `df` for
    each column in `target_vars`.

    Records are matched based on 'reference'.

    Args:
        df (pd.DataFrame): Processed full responses DataFrame.
        backdata (_type_): One period of backdata.
        target_vars (_type_): Variables to be imputed.

    Returns:
        pd.DataFrame: df with values carried forwards
    """
    df = pd.merge(df,
                  backdata,
                  how="left",
                  on="reference",
                  suffixes=("", "_prev"),
                  indicator=True)
    copy_vars = target_vars

    # Copy values from relevant columns where references match
    match_condition = df["_merge"] == "both"
    for var in copy_vars:
        df.loc[match_condition, f"{var}_imputed"] = df.loc[match_condition, f"{var}_prev"]
    df.loc[match_condition, "imp_marker"] = "CF"
    df.loc[match_condition, "instance"] = df.loc[match_condition, "instance_prev"]
    
    # Drop merge related columns
    to_drop = [column for column in df.columns if column.endswith('_prev')]
    to_drop += ['_merge']
    df = df.drop(to_drop, axis=1)
    return df
