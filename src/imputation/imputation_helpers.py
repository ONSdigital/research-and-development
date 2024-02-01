"""Utility functions  to be used in the imputation module."""
import logging
import pandas as pd
from typing import List, Dict, Callable
from itertools import chain

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


def create_mask(df:pd.DataFrame, option: str)-> pd.Series:
    """Create a dataframe mask based on 'option' - retrun Bool column.
    
    option include:
        - 'clear_status': rows with one of the clear statuses
        - 'to_impute_status': rows with 'Form sent out' or 'Check needed'
        - 'instance_zero': rows with instance = 0
        - 'instance_nonzero': rows with instance != 0
        - 'no_r_and_d' : rows where q604 = 'No'
        - 'postcode_only': rows in which there are no numeric values, only postcodes.
    """
    clear_mask = df["status"].isin(["Clear", "Clear - overridden"])
    to_impute_status_mask = df["status"].isin(["Form sent out", "Check needed"])
    instance_mask = df.instance == 0
    no_r_and_d_mask = df["604"]=="No"
    postcode_only_mask = df["211"].isnull() & ~df["601"].isnull()

    df = df.copy()
    
    if option == "clear_status":
        mask = clear_mask

    elif option == "to_impute_status":
        mask = to_impute_status_mask

    elif option == "instance_zero":
        mask = instance_mask

    elif option == "instance_nonzero":
        mask = ~instance_mask

    elif option == "no_r_and_d":
        mask = no_r_and_d_mask
        
    elif option == "postcode_only":
        mask = postcode_only_mask

    elif option == "excl_postcode_only":
        mask = ~postcode_only_mask

    else:
        raise ValueError(f"create_mask function parameter {option} is not valid.")

    return mask


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


def fix_604_error(df: pd.DataFrame) -> pd.Series:
    """Copy 'Yes' or 'No' in insance 0 for q604 to all other instances for each ref.

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

    returned dataframe:
        reference | instance    | "604"
    ---------------------------------
        1         | 0           | "No"
        2         | 0           | "Yes"
        2         | 1           | "Yes"

    args:
        df (pd.DataFrame): The dataframe being prepared for imputation.

    returns:
        (pd.DataFrame): The dataframe with only instance 0 for "no r&d" refs.
    """
    # Copy the "Yes" or "No" in col 604 to all other instances
    df["604"] = copy_first_to_group(df, "604")

    # For long form references with "No" in col 604, keep only instance 0
    to_remove_mask = (
        (df["formtype"] == "0001") & (df["604"] == "No") & (df["instance"] != 0)
    )

    # Note: where any of the columns in the mask has a null value, the mask will be null
    to_remove_mask = to_remove_mask.fillna(False)

    # output the references that contained data in error
    removed_df = df.copy().loc[to_remove_mask][["reference", "instance", "604"]]
    if not removed_df.empty:
        ImputationHelpersLogger.info(
            "The following 'No R&D' references have had invalid records removed: \n"
            f"{removed_df}"
        )

    # finally we remove unwanted rows
    filtered_df = df.copy().loc[~(to_remove_mask)]

    return filtered_df


def instance_fix(df: pd.DataFrame):
    """Set instance to 1 for longforms with status 'Form sent out.'

    References with status 'Form sent out' initially have a null in the instance
    column.
    """
    mask = (df.formtype == "0001") & (df.status == "Form sent out")
    df.loc[mask, "instance"] = 1

    return df


def create_r_and_d_instance(df: pd.DataFrame) -> pd.DataFrame:
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
    df = fix_604_error(df)

    no_rd_mask = (df.formtype == "0001") & (df["604"] == "No")
    filtered_df = df.copy().loc[no_rd_mask]
    filtered_df["instance"] = 1

    updated_df = pd.concat([df, filtered_df], ignore_index=True)
    updated_df = updated_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)
    return updated_df


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


def tidy_imputation_dataframe(
    df: pd.DataFrame,
    config: Dict,
    logger: logging.Logger,
    to_impute_cols: List,
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Remove rows and columns not needed after imputation."""
    # Create lists for the qa cols
    imp_cols = [f"{col}_imputed" for col in to_impute_cols]

    # Update the original breakdown questions and target variables with the imputed
    df[to_impute_cols] = df[imp_cols]

    # Remove all qa columns
    to_drop = [
        col
        for col in df.columns
        if (
            col.endswith("prev") | col.endswith("imputed") | col.endswith("link")
            | col.endswith("sf_exp_grouping") | col.endswith("trim") 
        )
    ]

    to_drop += ["200_original", "pg_sic_class", "empty_pgsic_group", "empty_pg_group"]
    to_drop += ["200_imp_marker"]
    
    df = df.drop(columns=to_drop)

    return df
