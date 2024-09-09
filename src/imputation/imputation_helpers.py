"""Utility functions  to be used in the imputation module."""
import logging
import pandas as pd
from typing import List, Dict, Tuple, Callable
from itertools import chain

from src.staging.validation import load_schema

ImputationHelpersLogger = logging.getLogger(__name__)


def get_imputation_cols(config: dict) -> list:
    """Return a list of numeric columns to use for imputation.

    These include columns of the form 2xx, 3xx, also the columns of the form
    emp_xx (which have been apportioned across product groups from the 4xx columns) and
    headcount_xx (which have been apportioned across proudct gropues from the 5xx cols)

    Args:
        config (dict): The pipeline configuration settings.

    Returns:
        numeric_cols (list): A list of all the columns imputation is applied to.
    """
    master_cols = list(config["breakdowns"].keys())
    bd_qs_lists = list(config["breakdowns"].values())
    bd_cols = list(chain(*bd_qs_lists))

    sum_cols = config["imputation"]["sum_cols"]
    other_sum_cols = [c for c in sum_cols if c not in master_cols]

    numeric_cols = master_cols + bd_cols + other_sum_cols

    return numeric_cols


def create_notnull_mask(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a mask for string values in column col that are not null."""
    return df[col].str.len() > 0


def create_mask(df: pd.DataFrame, options: List) -> pd.Series:
    """Create a dataframe mask based on listed options - retrun Bool column.

    Options include:
        - 'clear_status': rows with one of the clear statuses
        - 'instance_zero': rows with instance = 0
        - 'instance_nonzero': rows with instance != 0
        - 'no_r_and_d' : rows where q604 = 'No'
        - 'postcode_only': rows in which there are no numeric values, only postcodes.
    """
    clear_mask = df["status"].isin(["Clear", "Clear - overridden"])
    instance_mask = df.instance == 0
    no_r_and_d_mask = df["604"] == "No"
    postcode_only_mask = df["211"].isnull() & ~df["601"].isnull()

    # Set initial values for the mask series as a column in the dataframe
    df = df.copy()
    df["mask_col"] = False

    if "clear_status" in options:
        df["mask_col"] = df["mask_col"] & clear_mask

    if "instance_zero" in options:
        df["mask_col"] = df["mask_col"] & instance_mask

    elif "instance_nonzero" in options:
        df["mask_col"] = df["mask_col"] & ~instance_mask

    if "no_r_and_d" in options:
        df["mask_col"] = df["mask_col"] & no_r_and_d_mask

    if "postcode_only" in options:
        df["mask_col"] = df["mask_col"] & postcode_only_mask

    if "excl_postcode_only" in options:
        df["mask_col"] = df["mask_col"] & ~postcode_only_mask

    return df["mask_col"]


def instance_fix(df: pd.DataFrame):
    """Set instance to 1 for longforms with status 'Form sent out.'

    References with status 'Form sent out' initially have a null in the instance
    column.
    """
    mask = (df.formtype == "0001") & (df.status == "Form sent out")
    df.loc[mask, "instance"] = 1

    return df


def copy_first_to_group(df: pd.DataFrame, col_to_update: str) -> pd.Series:
    """Copy item in insance 0 to all other instances in a given reference.

    Example:

    For long form entries, questions 405 - 412 and 501 - 508 are recorded
    in instance 0. A series is returned representing the updated column with
    values from instance 0 copied to all other instances of a reference.

    Note: this is achieved using .transform("first"), which takes the value at
    instance 0 and inserts it to all memebers of the group.

    initial dataframe:
        reference | instance    | col
    ---------------------------------
        1         | 0           | 333
        1         | 1           | nan
        1         | 2           | nan

    returns the series
        col
        ---
        333
        333
        333

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
        col_to_update (str): The name of the column being updated

    Returns:
        pd.Series: A single column dataframe with the values in instance 0
        copied to other instances for the same reference.
    """
    updated_col = df.groupby("reference")[col_to_update].transform("first")
    return updated_col


def get_mult_604_mask(df: pd.DataFrame) -> pd.Series:
    """Return mask for long form references with "No" in col 604 but >1 instance.

    Fill nulls as where any of the columns in the mask has a null value,
    the mask will be null"""
    mult_604_mask = (
        (df["formtype"] == "0001") & (df["604"] == "No") & (df["instance"] != 0)
    ).fillna(False)
    return mult_604_mask


def fix_604_error(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Filter out rows with 604 error and create qa dataframe with the rows with errors.

    Return the filtered data frame and a second qa dataframe with
    all references with no R&D but more than one instance for output.

    Note:
        Occasionally we have noticed that an instance 1 containing a small amount of
        data has been created for a "no R&D" reference, in error.
        These entries were not identified and removed from the pipeline as
        they don't have a "No" in column 604. To fix this, we copy the "No" from
        instance 0 to all instances, then ensure only instance 0 remains before
        creating a fresh instance 1.

    Note: this is achieved using .transform("first"), which takes the value at
    instance 0 and inserts it to all memebers of the group.

    initial dataframe:
        reference | instance    | "604"
    ---------------------------------
        1         | 0           | "No"
        1         | 1           | nan
        2         | 0           | "Yes"
        2         | 1           | nan

    returned filtered dataframe:
        reference | instance    | "604"
    ---------------------------------
        1         | 0           | "No"
        2         | 0           | "Yes"
        2         | 1           | "Yes"

    returned qa dataframe:
        reference | instance    | "604"
    ---------------------------------
        1         | 0           | "No"
        1         | 1           | "No"


    args:
        df (pd.DataFrame): The dataframe being prepared for imputation.

    returns:
        (pd.DataFrame): The dataframe with only instance 0 for "no r&d" refs.
        (pd.DataFrame): The dataframe with references with > 1 insance but no r&d.
    """
    # Copy the "Yes" or "No" in col 604 to all other instances
    df["604"] = copy_first_to_group(df, "604")

    mult_604_mask = get_mult_604_mask(df)

    # get list of references with no R&D but more than one instance.
    mult_604_df = df.copy().loc[mult_604_mask]
    mult_604_ref_list = list(mult_604_df["reference"].unique())

    # create qa dataframe containing all rows for instances with 604 error (inc inst 0)
    mult_604_qa_df = df.copy().loc[df.reference.isin(mult_604_ref_list)]

    # finally we remove unwanted rows
    filtered_df = df.copy().loc[~(mult_604_mask)]

    return filtered_df, mult_604_qa_df


def check_604_fix(df) -> pd.DataFrame:
    """Check the refs with no R&D have one instance 0 and one instance 1 only."""
    mult_604_mask = get_mult_604_mask(df)
    filtered_df = df.copy().loc[mult_604_mask][["reference", "instance"]]
    filtered_df["ref_count"] = filtered_df.groupby("reference").transform(sum)

    check_df = filtered_df.copy().loc[filtered_df.ref_count > 1]

    filtered_df = df.copy().drop_duplicates(subset=["reference", "instance"])
    return filtered_df, check_df


def create_r_and_d_instance(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, List]:
    """Create a duplicate of long form records with no R&D and set instance to 1.

    These references initailly have one entry with instance 0.
    A copy will be created with instance set to 1. During imputation, all target values
    in this row will be set to zero, so that the reference "counts" towards the means
    calculated in TMI.

    args:
        df (pd.DataFrame): The dataframe being prepared for imputation.

    returns:
        (pd.DataFrame): The same dataframe with an instance 1 for "no R&D" refs.
    """
    # Ensure that in the case longforms with "no R&D" we only have one row
    df, mult_604_qa_df = fix_604_error(df)

    no_rd_mask = (df.formtype == "0001") & (df["604"] == "No")
    filtered_df = df.copy().loc[no_rd_mask]
    filtered_df["instance"] = 1

    updated_df = pd.concat([df, filtered_df], ignore_index=True)
    updated_df = updated_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    # check that the fix has worked and drop duplicates for now if not
    final_df, check_df = check_604_fix(updated_df)
    # TODO: it shouldn't be necessary to drop duplicates if the fix works properly.
    ImputationHelpersLogger.info("The following references are 'No R&D' ")
    ImputationHelpersLogger.info("but have too many rows- duplicates will be dropped:")
    ImputationHelpersLogger.info(check_df)

    return final_df, mult_604_qa_df


def split_df_on_trim(df: pd.DataFrame, trim_bool_col: str) -> pd.DataFrame:
    """Splits the dataframe in based on if it was trimmed or not"""

    if not df.empty:
        # TODO: remove this temporary fix to cast Nans to False
        df[trim_bool_col].fillna(False, inplace=True)

        df_not_trimmed = df.loc[~df[trim_bool_col]]
        df_trimmed = df.loc[df[trim_bool_col]]

        return df_trimmed, df_not_trimmed

    else:
        # return two empty dfs
        return df, df


def split_df_on_imp_class(df: pd.DataFrame, exclusion_list: List = ["817", "nan"]):

    # Exclude the records from the reference list
    exclusion_str = "|".join(exclusion_list)

    # Create the filter
    exclusion_filter = df["imp_class"].str.contains(exclusion_str)
    # Where imputation class is null, `NaN` is returned by the
    # .str.contains(exclusion_str) so we need to swap out the
    # returned `NaN`s with True, so it gets filtered out
    exclusion_filter = exclusion_filter.fillna(True)

    # Filter out imputation classes that include "817" or "nan"
    filtered_df = df[~exclusion_filter]  # df has 817 and nan filtered out
    excluded_df = df[exclusion_filter]  # df only has 817 and nan records

    return filtered_df, excluded_df


def fill_sf_zeros(df: pd.DataFrame) -> pd.DataFrame:
    """Fill nulls with zeros in short from numeric questions."""
    sf_questions = [str(q) for q in range(701, 712) if q != 708]

    sf_mask = df["formtype"] == "0006"
    clear_mask = df["status"].isin(["Clear", "Clear - overridden"])

    for q in sf_questions:
        df.loc[(sf_mask & clear_mask), q] = df.copy()[q].fillna(0)

    return df


def calculate_totals(df):
    """Calculate the employment and headcount totals for the imputed columns.

    This should be carried out for long form entries only as emp_total and
    headcount_total are themselves target variables in short forms.

    Imputation is applied to "target variables", and after this, imputed values for
    "breakdown variables" are calculated. After both MoR and TMI imputation have been
    carried out, but before short form expansion imputation, the totals for employment
    and headcount are calculated.

    Args:
        df (pd.DataFrame): The dataframe with imputed data

    Returns:
        pd.DataFrame: The dataframe with the totals calculated
    """
    mask = df["formtype"] == "0001"

    df.loc[mask, "emp_total_imputed"] = (
        df.loc[mask, "emp_researcher_imputed"]
        + df.loc[mask, "emp_technician_imputed"]
        + df.loc[mask, "emp_other_imputed"]
    )

    df.loc[mask, "headcount_tot_m_imputed"] = (
        df.loc[mask, "headcount_res_m_imputed"]
        + df.loc[mask, "headcount_tec_m_imputed"]
        + df.loc[mask, "headcount_oth_m_imputed"]
    )

    df.loc[mask, "headcount_tot_f_imputed"] = (
        df.loc[mask, "headcount_res_f_imputed"]
        + df.loc[mask, "headcount_tec_f_imputed"]
        + df.loc[mask, "headcount_oth_f_imputed"]
    )

    df.loc[mask, "headcount_total_imputed"] = (
        df.loc[mask, "headcount_tot_m_imputed"]
        + df.loc[mask, "headcount_tot_f_imputed"]
    )

    return df


def tidy_imputation_dataframe(
    df: pd.DataFrame,
    to_impute_cols: List,
) -> pd.DataFrame:
    """Remove rows and columns not needed after imputation."""
    # Create lists for the qa cols
    imp_cols = [f"{col}_imputed" for col in to_impute_cols]

    # Create mask for rows that have been imputed
    imputed_mask = df["imp_marker"].isin(["TMI", "CF", "MoR", "R"])
    # Update columns with imputed version
    for col in to_impute_cols:
        df.loc[imputed_mask, col] = df.loc[imputed_mask, f"{col}_imputed"]

    # Remove all qa columns
    to_drop = [
        col
        for col in df.columns
        if (
            col.endswith("prev")
            | col.endswith("imputed")
            | col.endswith("link")
            | col.endswith("sf_exp_grouping")
            | col.endswith("trim")
        )
    ]

    to_drop += ["200_original", "pg_sic_class", "empty_pgsic_group", "empty_pg_group"]
    to_drop += ["200_imp_marker"]

    df = df.drop(columns=to_drop)

    return df


def create_new_backdata(backdata: pd.DataFrame, config) -> pd.DataFrame:
    """Create a new backdata dataframe with the required columns from schema.

    Use the backdata toml schema to select the required columns from the backdata.
    filter for the clear and imputed statuses.

    Args:
        backdata (pd.DataFrame): The backdata dataframe.

    Returns:
        pd.DataFrame: The filtered backdata with only the required columns.
    """
    # filter for the clear and imputed statuses
    imp_markers_to_keep: list = ["R", "TMI", "CF", "MoR", "constructed"]
    backdata = backdata.loc[backdata["imp_marker"].isin(imp_markers_to_keep)]

    # get the wanted columns from the backdata schema
    schema = load_schema("./config/backdata_schema.toml")
    wanted_cols = list(schema.keys())

    return backdata[wanted_cols]
