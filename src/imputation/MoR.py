"""Functions for the Mean of Ratios (MoR) methods."""
import itertools
import pandas as pd
import re

from src.imputation.apportionment import run_apportionment
from src.imputation.tmi_imputation import create_imp_class_col, trim_bounds

good_statuses = ["Clear", "Clear - overridden"]
bad_statuses = ["Form sent out", "Check needed"]


def run_mor(df, backdata, impute_vars, lf_target_vars, config):
    """Function to implement Mean of Ratios method.

    This is implemented by first carrying forward data from last year
    for non-responders, and then calculating and applying growth rates
    for each imputation class.

    Args:
        df (pd.DataFrame): Processed full responses DataFrame
        backdata (pd.DataFrame): One period of backdata.
        impute_vars ([string]): List of variables to impute.
        lf_target_vars ([string]): List of long form target vars.

    Returns:
        pd.DataFrame: df with MoR applied
    """
    to_impute_df, remainder_df, backdata = mor_preprocessing(df, backdata)

    # Carry forwards method
    carried_forwards_df = carry_forwards(to_impute_df, backdata, impute_vars)

    links_df = calculate_links(remainder_df, backdata, lf_target_vars)
    mor_df = calculate_MoR(links_df, lf_target_vars, config)

    carried_forwards_df = apply_MoR(carried_forwards_df, mor_df, lf_target_vars)
    # TODO Remove the `XXX_prev` columns (left in for QA)
    return pd.concat([remainder_df, carried_forwards_df]).reset_index(drop=True)


def mor_preprocessing(df, backdata):
    """Apply pre-processing ready for MoR

    Args:
        df (pd.DataFrame): full responses for the current year
        backdata (pd.Dataframe): backdata file read in during staging.
    """
    # Convert backdata column names from qXXX to XXX
    p = re.compile(r"q\d{3}")
    cols = [col for col in list(backdata.columns) if p.match(col)]
    to_rename = {col: col[1:] for col in cols}
    backdata = backdata.rename(columns=to_rename)

    # TODO move this to imputation main
    # Select only values to be imputed
    df = create_imp_class_col(df, "200", "201", use_cellno=False)
    backdata = create_imp_class_col(backdata, "200", "201", use_cellno=False)

    imputation_cond = (df["formtype"] == "0001") & (df["status"].isin(bad_statuses))
    to_impute_df = df.copy().loc[imputation_cond, :]
    remainder_df = df.copy().loc[~imputation_cond, :]

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

    df.loc[match_cond] = create_imp_class_col(df,
                                              "200_prev",
                                              "201_prev",
                                              use_cellno=False)

    # Drop merge related columns
    to_drop = [column for column in df.columns
               if (column.endswith("_prev")) & (column not in impute_vars)]
    to_drop += ["_merge"]
    df = df.drop(to_drop, axis=1)
    return df


def calculate_links(current_df, prev_df, target_vars):
    """Calculate the links between previous and current data.

    Args:
        current_df (_type_): _description_
        prev_df (_type_): _description_
        target_vars (_type_): _description_
    """
    link_df = pd.merge(current_df,
                       prev_df,
                       on=["reference", "imp_class"],
                       how="inner",
                       suffixes=("", "_prev"))
    target_vars_prev = [f"{var}_prev" for var in target_vars]
    link_df = (link_df[["reference", "imp_class"] + target_vars + target_vars_prev]
               .groupby(["reference", "imp_class"])
               .sum()
               )
    # Calculate the ratios for the relevant variables
    for target in target_vars:
        link_df[f"{target}_link"] = link_df[target] / link_df[f"{target}_prev"]

    # TODO How to deal with infs or 0/0
    return link_df.reset_index()


def calculate_MoR(link_df, target_vars, config):
    """Calculate the Means of Ratios (links) for each imp_class

    Args:
        link_df (pd.DataFrame): DataFrame of links for each target variable
        target_vars ([string]): List of target variables to use.
    """
    link_vars = [f"{var}_link" for var in target_vars]
    # Apply trimming and calculate means for each imp class
    link_df = link_df[["imp_class", "reference"] + link_vars].groupby("imp_class")
    link_df = link_df.apply(group_calc_mor, target_vars, config)

    # Reorder columns to make QA easier
    column_order = (["imp_class", "reference"]
                    + list(itertools.chain(
                        *[(f"{var}_link", f"{var}_link_trim", f"{var}_mor")
                          for var in target_vars]))
                    )
    return link_df[column_order].reset_index(drop=True)


def group_calc_mor(group, target_vars, config):
    """Apply the MoR method to each group

    Args:
        group (pd.core.groupby.DataFrameGroupBy): Imputation class group
        link_vars ([string]): List of the linked variables.
        config (Dict): Confuration settings
    """
    for var in target_vars:
        group = trim_bounds(group, f"{var}_link", config)
        group[f"{var}_mor"] = (
            group.loc[~group[f"{var}_link_trim"], f"{var}_link"].mean()
        )
    return group


def apply_MoR(cf_df, mor_df, target_vars):
    """Apply the links to the carried forwards values.

    Args:
        cf_df (pd.DataFrame): DataFrame of carried forwards values
        mor_df (pd.DataFrame): DataFrame containing calculated links.
        target_vars ([string]): List of target variables
    """
    # Reduce the mor_df so we only have the variables we need and one row
    # per imputation class
    mor_df = (mor_df[["imp_class"] + [f"{var}_mor" for var in target_vars]]
              .groupby("imp_class").first())

    cf_df = pd.merge(cf_df, mor_df, on="imp_class", how="left", indicator=True)

    # Mask for values that are CF and also have a MoR link
    matched_mask = (cf_df["_merge"] == "both") & (cf_df["imp_marker"] == "CF")

    for var in target_vars:
        # Only apply MoR where the link is non null/0
        no_zero_mask = pd.notnull(cf_df[f"{var}_mor"]) & (cf_df[f"{var}_mor"] != 0)
        full_mask = matched_mask & no_zero_mask
        # Apply the links to the previous values
        cf_df.loc[full_mask, f"{var}_imputed"] = (
            cf_df.loc[full_mask, f"{var}_imputed"] * cf_df.loc[full_mask, f"{var}_mor"]
        )
        cf_df.loc[matched_mask, "imp_marker"] = "MoR"

    return cf_df
