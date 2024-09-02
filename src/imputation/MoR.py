"""Functions for the Mean of Ratios (MoR) methods."""
import itertools
import re
import pandas as pd
import numpy as np

from src.imputation.tmi_imputation import create_imp_class_col, trim_bounds
from src.construction.construction_helpers import convert_formtype

good_statuses = ["Clear", "Clear - overridden"]
bad_statuses = ["Form sent out", "Check needed"]


def run_mor(df, backdata, impute_vars, config):
    """Function to implement Mean of Ratios method.

    This is implemented by first carrying forward data from last year
    for non-responders, and then calculating and applying growth rates
    for each imputation class.

    Args:
        df (pd.DataFrame): Processed full responses DataFrame
        backdata (pd.DataFrame): One period of backdata.
        impute_vars ([string]): List of variables to impute.

    Returns:
        pd.DataFrame: df with MoR applied.
        pd.DataFrame: QA DataFrame showing how imputation links are calculated.
    """

    to_impute_df, remainder_df, backdata = mor_preprocessing(df, backdata)

    # Carry forwards method
    carried_forwards_df = carry_forwards(to_impute_df, backdata, impute_vars)

    # apply MoR for long form responders
    imputed_df_long, links_df_long = calculate_mor(
        carried_forwards_df, remainder_df, backdata, config, "long"
    )

    # apply MoR for short form responders
    imputed_df_short, links_df_short = calculate_mor(
        carried_forwards_df, remainder_df, backdata, config, "short"
    )

    imputed_df = pd.concat(
        [remainder_df, imputed_df_long, imputed_df_short]
    ).reset_index(drop=True)
    imputed_df = imputed_df.drop("cf_group_size", axis=1)

    links_df = pd.concat([links_df_long, links_df_short]).reset_index(drop=True)

    return imputed_df, links_df


def mor_preprocessing(df, backdata):
    """Apply pre-processing ready for MoR

    Args:
        df (pd.DataFrame): full responses for the current year
        backdata (pd.Dataframe): backdata file read in during staging.
    """
    # Add a QA column for the group size
    df["cf_group_size"] = np.nan

    # TODO move this to imputation main
    # Create imp_class column
    df = create_imp_class_col(df, "200", "201")

    # ensure the "formtype" column is in the correct format
    df["formtype"] = df["formtype"].apply(convert_formtype)
    backdata["formtype"] = backdata["formtype"].apply(convert_formtype)

    stat_cond = df["status"].isin(bad_statuses)
    sf_cond = (df["formtype"] == "0006") & (df["selectiontype"] == "C")
    lf_cond = df["formtype"] == "0001"
    imputation_cond = stat_cond & (sf_cond | lf_cond)
    to_impute_df = df.copy().loc[imputation_cond, :]
    remainder_df = df.copy().loc[~imputation_cond, :]

    # Ensure backdata is as we require
    wanted_cond = backdata["imp_marker"].isin(["R", "CF", "MoR", "TMI"])
    backdata = backdata.copy().loc[wanted_cond, :]

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

    # Replace the values of certain columns with the values from the back data
    replace_vars = ["instance", "200", "201", "601", "602", "604"]
    for var in replace_vars:
        df.loc[match_cond, var] = df.loc[match_cond, f"{var}_prev"]

    # Update the postcodes_harmonised column from the updated column 601
    df.loc[match_cond, "postcodes_harmonised"] = df.loc[match_cond, "601"]

    # Update the imputation classes based on the new 200 and 201 values
    df = create_imp_class_col(df, "200", "201")

    # Update the varibles to be imputed by the corresponding previous values
    for var in impute_vars:
        df.loc[match_cond, f"{var}_imputed"] = df.loc[match_cond, f"{var}_prev"].fillna(
            0
        )

        # fill nulls with zeros if col 211 is not null
        fillna_cond = ~df["211"].isnull()
        df.loc[match_cond & fillna_cond, f"{var}_imputed"] = df.loc[
            match_cond & fillna_cond, f"{var}_imputed"
        ].fillna(0)

    df.loc[match_cond, "imp_marker"] = "CF"

    # other columns we would like to keep from the backdata for QA purposes
    more_cols = ["formtype", "imp_class", "imp_marker"]

    # Drop merge related columns
    to_drop = [
        column
        for column in df.columns
        if (column.endswith("_prev"))
        & (re.search("(.*)_prev|.*", column).group(1) not in (impute_vars + more_cols))
    ]
    to_drop += ["_merge"]
    df = df.drop(to_drop, axis=1)
    return df


def filter_for_links(df: pd.DataFrame, is_current: bool) -> pd.DataFrame:
    """Filter the data to only include the relevant rows for calculating links.

    Args:
        df (pd.DataFrame): DataFrame of data to filter.
        is_current (bool): Whether the data is the current or previous period.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    # Filter out imputation classes that are missing either "200" or "201"
    nan_mask = df["imp_class"].str.contains("nan").apply(lambda x: not x)
    # Select only clear, or equivalently, imp_marker R.
    # Exclude PRN cells in the current period.
    if is_current:
        mask = (df["imp_marker"] == "R") & (df["selectiontype"] != "P") & nan_mask
    else:
        mask = (df["imp_marker"] == "R") & nan_mask

    return df.loc[mask, :]


def calculate_growth_rates(current_df, prev_df, target_vars):
    """Calculate the growth rates between previous and current data.

    Growth rates are caclucated for "matched pairs": where the reference and imp_class
    are the same in both the current and previous data. This is done for clear
    responders only (imp_marker = R).

    PRN sampled cells (which only occur in short forms) are not included for the current
    period, however a matched pair could still be valid if the reference was PRN in the
    previous period.

    Args:
        current_df (pd.DataFrame): pre-processed current data.
        prev_df (pd.DataFrame): pre-processed backdata.
        target_vars ([string]): target vars to impute.
    """
    # Select only clear, or equivalently, imp_marker R.
    # Exclude PRN cells in the current period.
    prev_df = filter_for_links(prev_df.copy(), is_current=False)
    current_df = filter_for_links(current_df.copy(), is_current=True)

    # Ensure we only have one row per reference/imp_class for previous and current data
    prev_df = (
        prev_df[["reference", "imp_class", "imp_marker"] + target_vars]
        .groupby(["reference", "imp_class"])
        .sum()
    ).reset_index()

    current_df = (
        current_df[["reference", "imp_class", "imp_marker"] + target_vars]
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
        # Calculate a growth rate if both the current and previous values are non-zero
        valid_mask = (gr_df[f"{target}_prev"] != 0) & (gr_df[target] != 0)
        gr_df.loc[valid_mask, f"{target}_gr"] = (
            gr_df.loc[valid_mask, target] / gr_df.loc[valid_mask, f"{target}_prev"]
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
    if (type(threshold_num) == int) & (threshold_num >= 0):
        return threshold_num
    else:
        raise Exception(
            "The variable 'mor_threshold' in the 'imputation' section "
            "of the config must be zero or a positive integer."
        )


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
        trimmed_bounds, qa = trim_bounds(group.loc[non_null_mask, :], f"{var}_gr", config)
        group.loc[non_null_mask, f"{var}_gr_trim"] = (
            trimmed_bounds
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


def apply_links(cf_df, links_df, target_vars, config, formtype):
    """Apply the links to the carried forwards values.

    Args:
        cf_df (pd.DataFrame): DataFrame of carried forwards values.
        links_df (pd.DataFrame): DataFrame containing calculated links.
        target_vars ([string]): List of target variables.
        config (Dict): Dictorary of configuration.
        formtype (str): whether the formtype is long or short.
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

    # Apply MoR for the breakdown variables
    q_targets = list(config["breakdowns"])
    if formtype == "long":
        q_targets = [
            q for q in q_targets if q in config["imputation"]["lf_target_vars"]
        ]
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


def calculate_mor(cf_df, remainder_df, backdata, config, formtype):
    """Apply the MoR method to long form responders.

    Args:
        cf_df (pd.DataFrame): DataFrame of carried forwards values to impute.
        remainder_df (pd.DataFrame): DataFrame of remaining values.
        backdata (pd.DataFrame): One period of backdata.
        config (Dict): The configuration settings for the pipeline.
        formtype (str): The formtype of the data being imputed, long or short.

    Returns:
        pd.DataFrame: df with MoR applied for long forms
        pd.DataFrame: QA DataFrame showing how imputation links are calculated.
    """
    if formtype == "long":
        target_vars = config["imputation"]["lf_target_vars"]
        cf_df = cf_df.copy().loc[cf_df["formtype"] == "0001", :]
        remainder_df = remainder_df.copy().loc[remainder_df["formtype"] == "0001", :]
        backdata = backdata.copy().loc[backdata["formtype"] == "0001", :]

    elif formtype == "short":
        target_vars = list(config["breakdowns"])
        cf_df = cf_df.copy().loc[(cf_df["formtype"] == "0006"), :]
        remainder_df = remainder_df.copy().loc[(remainder_df["formtype"] == "0006"), :]
        backdata = backdata.copy().loc[(backdata["formtype"] == "0006"), :]

    else:
        raise ValueError("formtype must be 'long' or 'short'")

    gr_df = calculate_growth_rates(remainder_df, backdata, target_vars)
    links_df = calculate_links(gr_df, target_vars, config)

    if formtype == "long":
        links_df["formtype"] = "0001"
    else:
        links_df["formtype"] = "0006"

    imputed_df = apply_links(cf_df, links_df, target_vars, config, formtype)

    return imputed_df, links_df
