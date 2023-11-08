"""Functions for the Mean of Ratios (MoR) methods."""
import pandas as pd
import re

from src.imputation.apportionment import run_apportionment

good_statuses = ["Clear", "Clear - overridden"]
bad_statuses = ["Form sent out", "Check needed"]


def run_mor(df, backdata, impute_vars):
    """Function to implement Mean of Ratios method.

    This is implemented by first carrying forward data from last year
    for non-responders, and then calculating and applying growth rates
    for each imputation class.

    Args:
        df (pd.DataFrame): Processed full responses DataFrame
        backdata (pd.DataFrame): One period of backdata.
        impute_vars ([string]): List of variables to impute.

    Returns:
        pd.DataFrame: df with MoR applied
    """
    to_impute_df, remainder_df, backdata = mor_preprocessing(df, backdata)

    # Carry forwards method
    carried_forwards_df = carry_forwards(to_impute_df, backdata, impute_vars)

    # TODO Remove the `XXX_prev` columns (left in for QA)
    return pd.concat([remainder_df, carried_forwards_df])


def mor_preprocessing(df, backdata):
    """Apply pre-processing ready for MoR

    Args:
        df (pd.DataFrame): full responses for the current year
        backdata (pd.Dataframe): backdata file read in during staging.
    """
    # TODO move this to imputation main
    # Select only values to be imputed

    imputation_cond = (df["formtype"] == "0001") & (df["status"].isin(bad_statuses))
    to_impute_df = df.copy().loc[imputation_cond, :]
    remainder_df = df.copy().loc[~imputation_cond, :]

    # Convert backdata column names from qXXX to XXX
    p = re.compile(r"q\d{3}")
    cols = [col for col in list(backdata.columns) if p.match(col)]
    to_rename = {col: col[1:] for col in cols}
    backdata = backdata.rename(columns=to_rename)

    backdata = run_apportionment(backdata)

    clear_status_cond = backdata["status"].isin(good_statuses)
    # identify backdata rows that only consist of postcodes to eliminate this
    postcode_only_cond = backdata["211"].isnull() & backdata["405"].isnull()
    # Only pick up useful backdata
    backdata = backdata.loc[clear_status_cond & ~postcode_only_cond, :]

    return to_impute_df, remainder_df, backdata


def carry_forwards(df, backdata, impute_vars):
    """Carry forwards matcing `backdata` values into `df` for
    each column in `impute_vars`.

    Records are matched based on 'reference'.

    NOTE:
    As a result of the left join, where there is a match, each of the n instances
    in the backdata is joined to each of the m instances in the original df,
    resulting in n*m rows.
    For rows where there is a match, we only want to keep one instance
    For "Form sent out" statuses, the only instance is null, so we keep that one
    For "Check needed" statuses, we keep instance 0
    Where there is no match, we keep all rows.

    Args:
        df (pd.DataFrame): Processed full responses DataFrame.
        backdata (_type_): One period of backdata.
        impute_vars (_type_): Variables to be imputed.

    Returns:
        pd.DataFrame: df with values carried forwards
    """
    # log number of records before and after MoR
    df = pd.merge(
        df, backdata, how="left", on="reference", suffixes=("", "_prev"), indicator=True
    )

    # keep only the rows needed, see function docstring for details.
    no_match_cond = df["_merge"] == "left_only"
    instance_cond = (df["instance"] == 0) | pd.isnull(df["instance"])
    keep_cond = no_match_cond | instance_cond

    df = df.copy().loc[keep_cond, :]

    # Copy values from relevant columns where references match
    match_cond = df["_merge"] == "both"

    # replace the values of certain columns with the values from the back data
    # TODO: Check with methodology or BAU as to which other cols to take from backdata
    # TODO: By default, columns not updated such as 4xx, 5xx, 6xx
    # TODO: will contain the current data, instance 0.
    replace_vars = ["instance", "200", "201"]
    for var in replace_vars:
        df.loc[match_cond, var] = df.loc[match_cond, f"{var}_prev"]
    for var in impute_vars:
        df.loc[match_cond, f"{var}_imputed"] = df.loc[match_cond, f"{var}_prev"]
    df.loc[match_cond, "imp_marker"] = "CF"

    # Drop merge related columns
    to_drop = [column for column in df.columns if column.endswith("_prev")]
    to_drop += ["_merge"]
    df = df.drop(to_drop, axis=1)
    return df
