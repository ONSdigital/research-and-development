"""Functions for the Mean of Ratios (MoR) methods."""
import itertools
import pandas as pd
import numpy as np
import re

from src.staging.staging_helpers import postcode_topup
from src.imputation.apportionment import run_apportionment
from src.imputation.tmi_imputation import (
    create_imp_class_col,
    trim_bounds,
    calculate_totals,
)


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
        pd.DataFrame: df with MoR applied.
        pd.DataFrame: QA DataFrame showing how imputation links are calculated.
    """

    to_impute_df, remainder_df, backdata = mor_preprocessing(df, backdata)

    # Carry forwards method
    carried_forwards_df = carry_forwards(to_impute_df, backdata, impute_vars)

    gr_df = calculate_growth_rates(remainder_df, backdata, lf_target_vars)
    links_df = calculate_links(gr_df, lf_target_vars, config)

    carried_forwards_df = apply_links(
        carried_forwards_df, links_df, lf_target_vars, config
    )
    # Calculate totals as with TMI
    carried_forwards_df = calculate_totals(carried_forwards_df)

    imputed_df = pd.concat([remainder_df, carried_forwards_df]).reset_index(drop=True)
    imputed_df = imputed_df.drop("cf_group_size", axis=1)

    return imputed_df, links_df


def mor_preprocessing(df, backdata):
    """Apply pre-processing ready for MoR

    Args:
        df (pd.DataFrame): full responses for the current year
        backdata (pd.Dataframe): backdata file read in during staging.
    """
    # Convert backdata column names from qXXX to XXX
    # Note that this is only applicable when using the backdata on the network
    p = re.compile(r"q\d{3}")
    cols = [col for col in list(backdata.columns) if p.match(col)]
    to_rename = {col: col[1:] for col in cols}
    backdata = backdata.rename(columns=to_rename)

    # Add a QA column for the group size
    df["cf_group_size"] = np.nan

    # TODO move this to imputation main
    # Select only values to be imputed
    df = create_imp_class_col(df, "200", "201")
    backdata = create_imp_class_col(backdata, "200", "201")

    imputation_cond = (df["formtype"] == "0001") & (df["status"].isin(bad_statuses))
    to_impute_df = df.copy().loc[imputation_cond, :]
    remainder_df = df.copy().loc[~imputation_cond, :]

    backdata = run_apportionment(backdata)

    clear_status_cond = backdata["status"].isin(good_statuses)

    # Only pick up clear statuses from backdata
    backdata = backdata.loc[clear_status_cond, :]

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
    For "Form sent out" statuses, the only instance has been set to 1,
        so we keep that one.
    For "Check needed" statuses, we keep instance 0 only.
    Where there is no match, we keep all rows.

    Args:
        df (pd.DataFrame): Processed full responses DataFrame.
        backdata (pd.DataFrame): One period of backdata.
        impute_vars ([string]): Variables to be imputed.

    Returns:
        pd.DataFrame: df with values carried forwards
    """
    # log number of records before and after MoR
    df = pd.merge(
        df, backdata, how="left", on="reference", suffixes=("", "_prev"), indicator=True
    )
    # ensure the instance columns are still type "int" after merge
    df = df.astype({"instance": "Int64", "instance_prev": "Int64"})

    # keep only the rows needed, see function docstring for details.
    no_match_cond = df["_merge"] == "left_only"
    form_sent_out_cond = (df["status"] == "Form sent out") & (df["instance"] == 1)
    check_needed_cond = (df["status"] == "Check needed") & (df["instance"] == 0)
    keep_cond = no_match_cond | form_sent_out_cond | check_needed_cond

    df = df.copy().loc[keep_cond, :]

    # Copy values from relevant columns where references match
    match_cond = df["_merge"] == "both"

    # Apply the postcode top-up to clean the postcodes in col 601 of the back data
    df.loc[match_cond, "601_prev"] = df.loc[match_cond, "601_prev"].apply(
        postcode_topup
    )

    # Replace the values of certain columns with the values from the back data
    replace_vars = ["instance", "200", "201", "601", "602", "604"]
    for var in replace_vars:
        df.loc[match_cond, var] = df.loc[match_cond, f"{var}_prev"]

    # Update the postcodes_harmonised column from the updated column 601
    df.loc[match_cond, "postcodes_harmonised"] = df.loc[match_cond, "601"]
    
    # Update the varibles to be imputed by the corresponding previous values
    for var in impute_vars:
        df.loc[match_cond, f"{var}_imputed"] = df.loc[match_cond, f"{var}_prev"]

        # fill nulls with zeros if col 211 is not null
        fillna_cond = ~df["211"].isnull()
        df.loc[match_cond & fillna_cond, f"{var}_imputed"] = df.loc[
            match_cond & fillna_cond, f"{var}_imputed"
        ].fillna(0)

    df.loc[match_cond, "imp_marker"] = "CF"

    df.loc[match_cond] = create_imp_class_col(df, "200_prev", "201_prev")

    # Drop merge related columns
    to_drop = [
        column
        for column in df.columns
        if (column.endswith("_prev"))
        & (re.search("(.*)_prev|.*", column).group(1) not in impute_vars)
    ]
    to_drop += ["_merge"]
    df = df.drop(to_drop, axis=1)
    return df


def calculate_growth_rates(current_df, prev_df, target_vars):
    """Calculate the growth rates between previous and current data.

    Args:
        current_df (pd.DataFrame): pre-processed current data.
        prev_df (pd.DataFrame): pre-processed backdata.
        target_vars ([string]): target vars to impute.
    """
    # Only calculate links for long form responders
    current_df = current_df.copy().loc[current_df["formtype"] == "0001", :]
    # prev_df = prev_df.copy().loc[prev_df["formtype"] == "0001", :]

    # Ensure we only have one row per reference/imp_class for previous and current data
    prev_df = (
        prev_df[["reference", "imp_class"] + target_vars]
        .groupby(["reference", "imp_class"])
        .sum()
    ).reset_index()

    current_df = (
        current_df[["reference", "imp_class"] + target_vars]
        .groupby(["reference", "imp_class"])
        .sum()
    ).reset_index()

    # Merge the clear current and previous data
    gr_df = pd.merge(
        current_df,
        prev_df,
        on=["reference", "imp_class"],
        how="inner",
        suffixes=("", "_prev"),
        validate="one_to_one",
    )

    # Calculate the ratios for the relevant variables
    for target in target_vars:
        mask = (gr_df[f"{target}_prev"] != 0) & (gr_df[target] != 0)
        gr_df.loc[mask, f"{target}_gr"] = (
            gr_df.loc[mask, target] / gr_df.loc[mask, f"{target}_prev"]
        )

    return gr_df


def calculate_links(gr_df, target_vars, config):
    """Calculate the Means of Ratios (links) for each imp_class

    Args:
        gr_df (pd.DataFrame): DataFrame of growth rates for each target variable
        target_vars ([string]): List of target variables to use.
        config (Dict): Confuration settings.
    """
    # Apply trimming and calculate means for each imp class
    gr_df = gr_df.groupby("imp_class")
    gr_df = gr_df.apply(group_calc_link, target_vars, config)

    # Reorder columns to make QA easier
    column_order = ["imp_class", "reference", "cf_group_size"] + list(
        itertools.chain(
            *[
                (var, f"{var}_prev", f"{var}_gr", f"{var}_gr_trim", f"{var}_link")
                for var in target_vars
            ]
        )
    )
    return gr_df[column_order].reset_index(drop=True)


def get_threshold_value(config: dict) -> int:
    """Read, validate and return threshold value from the config."""
    threshold_num = config["imputation"]["mor_threshold"]
    if (type(threshold_num) == int) & (threshold_num >=0):
        return threshold_num
    else:
        raise Exception("The variable 'mor_threshold' in the 'imputation' section "
                        "of the config must be zero or a positive integer.")
        

def group_calc_link(group, target_vars, config):
    """Apply the MoR method to each group

    Args:
        group (pd.core.groupby.DataFrameGroupBy): Imputation class group
        link_vars ([string]): List of the linked variables.
        config (Dict): Confuration settings
    """
    valid_group_size = True

    for var in target_vars:
        # Create mask to not use 0s in mean calculation
        non_null_mask = pd.notnull(group[f"{var}_gr"])

        group = group.sort_values(f"{var}_gr")

        group[f"{var}_gr_trim"] = False
        group.loc[non_null_mask, f"{var}_gr_trim"] = (
            trim_bounds(group.loc[non_null_mask, :], f"{var}_gr", config)
            .loc[:, f"{var}_gr_trim"]
            .values
        )

        num_valid_vars = sum(~group[f"{var}_gr_trim"] & non_null_mask)

        if var == "211":
            group["cf_group_size"] = num_valid_vars
            threshold_num = get_threshold_value(config)

            if num_valid_vars < threshold_num:
                valid_group_size = False

        # If the group is a valid size, and there are non-null, non-zero values for this
        # 'var', then calculate the mean        
        if valid_group_size & (sum(~group[f"{var}_gr_trim"] & non_null_mask) != 0):
            group[f"{var}_link"] = group.loc[
                ~group[f"{var}_gr_trim"] & non_null_mask, f"{var}_gr"
            ].mean()
        # Otherwise the link is set to 1
        else:
            group[f"{var}_link"] = 1.0
    return group


def apply_links(cf_df, links_df, target_vars, config):
    """Apply the links to the carried forwards values.

    Args:
        cf_df (pd.DataFrame): DataFrame of carried forwards values.
        links_df (pd.DataFrame): DataFrame containing calculated links.
        target_vars ([string]): List of target variables.
        config (Dict): Dictorary of configuration.
    """
    # Reduce the mor_df so we only have the variables we need and one row
    # per imputation class
    links_df = (
        links_df[["imp_class"] + [f"{var}_link" for var in target_vars]]
        .groupby("imp_class")
        .first()
    )

    cf_df = pd.merge(cf_df, links_df, on="imp_class", how="left", indicator=True)

    # Mask for values that are CF and also have a MoR link
    matched_mask = (cf_df["_merge"] == "both") & (cf_df["imp_marker"] == "CF")

    # Apply MoR for the target variables
    for var in target_vars:
        # Only apply MoR where the link is non null/0
        no_zero_mask = pd.notnull(cf_df[f"{var}_link"]) & (cf_df[f"{var}_link"] != 0)
        full_mask = matched_mask & no_zero_mask
        # Apply the links to the previous values
        cf_df.loc[full_mask, f"{var}_imputed"] = (
            cf_df.loc[full_mask, f"{var}_imputed"] * cf_df.loc[full_mask, f"{var}_link"]
        )
        cf_df.loc[matched_mask, "imp_marker"] = "MoR"

    # Apply MoR for the breakdown variables under 211 and 305
    q_targets = ["211", "305"]
    for var in q_targets:
        for breakdown in config["breakdowns"][var]:
            # As above but using different elements to multiply
            no_zero_mask = pd.notnull(cf_df[f"{var}_link"]) & (
                cf_df[f"{var}_link"] != 0
            )
            full_mask = matched_mask & no_zero_mask
            # Apply the links to the previous values
            cf_df.loc[full_mask, f"{breakdown}_imputed"] = (
                cf_df.loc[full_mask, f"{breakdown}_imputed"]
                * cf_df.loc[full_mask, f"{var}_link"]
            )
            cf_df.loc[matched_mask, "imp_marker"] = "MoR"

    # Drop _merge column
    cf_df = cf_df.drop("_merge", axis=1)
    return cf_df
