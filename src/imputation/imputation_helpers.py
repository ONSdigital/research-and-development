"""Utility functions  to be used in the imputation module."""

import logging
import pandas as pd
import numpy as np

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


def create_imp_class_col(
    df: pd.DataFrame,
    column_list: list[str],
    class_name: str = "imp_class",
    use_cellno: bool = True,
) -> pd.DataFrame:
    """Creates a column for the imputation class.

    This is done by concatenating the R&D business type, C or D from  q200
    and the product group from  q201.

    special case for cell number 817 is added as a suffix.

    Args:
        df (pd.DataFrame): Full dataframe
        column_list: list of column names that will be concatenated to form the class.
        class_name (str): The name of the column to save the class to.
            Defaults to "imp_class"
        use_cellno (bool): Whether to use the cellno column or not. Default to True.

    Returns:
        pd.DataFrame: Dataframe which contains a new column with the
            imputation classes.
    """
    # check that column_list is a non-empty list and its elements are in the dataframe
    if not column_list:
        raise ValueError("column_list is empty")
    if not all(col in df.columns for col in column_list):
        raise ValueError("column_list contains columns not in the dataframe")

    # Ensure cols are treated as objects to handle mixed data types and missing values
    df_copy = df[column_list].copy().astype(object).fillna("nan").astype(str)

    # create a new column with the concatenation of the columns in column_list with  "_"
    df[class_name] = df_copy.agg("_".join, axis=1)

    if use_cellno:
        df.loc[df.cellnumber == 817, class_name] = df[class_name] + "_817"

    return df


def create_notnull_mask(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a mask for string values in column col that are not null."""
    return df[col].str.len() > 0


def create_mask(df: pd.DataFrame, options: list[str]) -> pd.Series:
    """Create a dataframe mask based on listed options - return Bool column.

    Options include:
        - 'clear_status': rows with one of the clear statuses
        - 'instance_zero': rows with instance = 0
        - 'instance_nonzero': rows with instance != 0
        - 'no_r_and_d': rows where q604 = 'No'
        - 'postcode_only': rows in which there are no numeric values, only postcodes.
        - 'excl_postcode_only': rows excluding those with only postcodes.
        - 'exclude_nan_classes': rows excluding those with "nan" in the imp_class col.
        - 'prn_only': PRN rows, ie, rows with selectiontype = 'P'
        - 'census_only': Census rows, ie, rows with selectiontype 'C'
        - 'longform_only': Longform rows, ie, rows with formtype = '0001'
        - 'shortform_only': Shortform rows, ie, rows with formtype = '0006'
        - 'bad_status': rows with a status that is not in the clear statuses
        - 'mor_imputed' : rows with an imp_marker of 'MoR' or 'CF'
        - 'not_mor_imputed' : rows without an imp_marker of 'MoR' or 'CF'

    Args:
        df (pd.DataFrame): The input dataframe.
        options (list[str]): list of options to create the mask.

    Returns:
        pd.Series: Boolean mask based on the options.
    """
    df = df.copy()  # Ensure the original DataFrame is not modified

    # Define masks for each option
    masks = {
        "clear_status": df["status"].isin(["Clear", "Clear - overridden"]),
        "instance_zero": df.instance == 0,
        "instance_nonzero": df.instance > 0,
        "no_r_and_d": df["604"] == "No",
        "postcode_only": df["211"].isnull() & df["601"].notnull(),
        "excl_postcode_only": ~(df["211"].isnull() & df["601"].notnull()),
        "exclude_nan_classes": ~df["imp_class"].str.contains("nan", na=True),
        "prn_only": df["selectiontype"] == "P",
        "census_only": df["selectiontype"] == "C",
        "longform_only": df["formtype"] == "0001",
        "shortform_only": df["formtype"] == "0006",
        "bad_status": df["status"].isin(["Check needed", "Form sent out"]),
        "mor_imputed": df["imp_marker"].isin(["MoR", "CF"]),
        "not_mor_imputed": ~df["imp_marker"].isin(["MoR", "CF"]),
    }

    # Initialize the mask to True
    mask = pd.Series(True, index=df.index)

    # Apply the masks based on the options
    for option in options:
        if option in masks:
            mask &= masks[option]

    return mask


def special_filter(df: pd.DataFrame, options: list[str]) -> pd.DataFrame:
    """Filter the dataframe based on a list of options commonly used in the pipeline.

    Args:
        df (pd.DataFrame): The input dataframe.
        options (list[str]): list of options to filter the dataframe.

    Returns:
        pd.DataFrame: The filtered dataframe.
    """
    mask = create_mask(df, options)
    df = df.copy().loc[mask]
    return df


def instance_fix(df: pd.DataFrame):
    """Set instance to 1 for longforms with status 'Form sent out.'

    References with status 'Form sent out' initially have a null in the instance
    column.
    """
    mask = (df.formtype == "0001") & (df.status == "Form sent out")

    if "is_constructed" in df.columns:
        mask = mask & (df.is_constructed.isin([False]))

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


def fix_604_error(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
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
) -> tuple[pd.DataFrame, pd.DataFrame]:
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

    # In the case where there is "no R&D", we create a copy of instance 0
    # and update to instance = 1. In this way we create an "instance 1" which we can
    # popultae with zeros for imputation purposes (see docstring above).
    no_rd_mask = (df.formtype == "0001") & (df["604"] == "No")
    filtered_df = df.copy().loc[no_rd_mask]
    filtered_df["instance"] = 1

    updated_df = pd.concat([df, filtered_df], ignore_index=True)
    updated_df = updated_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    # check that the fix has worked and drop duplicates for now if not
    final_df, check_df = check_604_fix(updated_df)

    return final_df, mult_604_qa_df


def split_df_on_trim(
    df: pd.DataFrame, trim_bool_col: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Splits the dataframe in based on if it was trimmed or not"""

    if not df.empty:
        df_copy = df.copy()
        df_copy[trim_bool_col] = df_copy[trim_bool_col].fillna(False)
        df_copy[trim_bool_col] = df_copy[trim_bool_col].astype(bool)

        df_not_trimmed = df_copy.loc[~df_copy[trim_bool_col]]
        df_trimmed = df_copy.loc[df_copy[trim_bool_col]]

        return df_trimmed, df_not_trimmed

    else:
        # return two empty dfs
        return df, df


def split_df_on_imp_class(df: pd.DataFrame, exclusion_list: list = ["817", "nan"]):
    """Split the dataframe based on the imputation class.

    Removes records where the imputation class includes strings in the passed list.
    Many records include a "nan" in either q200 (R&D type- Civil or Defence) and q201
    (Product Group)- these will generally be filtered out from the imputation classes.

    Where short forms are under consideration, "817" imputation classes will be excluded

    Args:
        df (pd.DataFrame): The dataframe to split
        exclusion_list (list, optional): A list of imputation classes to exclude.

    Returns:
        pd.DataFrame: The filtered dataframe with the invalid imp classes removed
        pd.DataFrame: The excluded dataframe
    """
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


def apply_fill_zeros(df: pd.DataFrame, target_variables: list) -> pd.DataFrame:
    """Applies the fill zeros function to clear longform responders.

    A mask is created to identify clear responders, excluding instance zero rows,
    but exclude "postcode only" rows.

    Zeros are then filled for the target values based on this mask.

    Args:
        df (pd.DataFrame): The dataframe imputation is carried out on.
        target_variables (list): A list of the target variables.

    Returns:
        pd.DataFrame: The same dataframe with required nulls filled with zeros.
    """
    # Condition to exclude rows conaining no data and only postcodes
    excl_postcode_only_mask = ~(df["211"].isnull() & create_notnull_mask(df, "601"))

    zerofill_mask = (
        (df["formtype"] == "0001")
        & (df["instance"] != 0)
        & (df["status"].isin(["Clear", "Clear - overridden"]))
        & excl_postcode_only_mask
    )

    for var in target_variables:
        df.loc[zerofill_mask, var] = df.loc[zerofill_mask, var].fillna(0)

    return df


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


def breakdown_checks_after_imputation(df: pd.DataFrame) -> None:
    """After imputation check required columns still sum correctly.

    Args:
        df (pd.DataFrame): The dataframe with imputed values.

    Returns:
        None
    """
    # create dictionary of checks: the last col in the list is the total col
    # the sum of the other cols should equal the total


def tidy_imputation_dataframe(df: pd.DataFrame, config) -> pd.DataFrame:
    """Update cols with imputed values and remove rows and columns no longer needed.

    Args:
        df (pd.DataFrame): The dataframe with imputed values.
        config (dict): The pipeline configuration settings.

    Returns:
        pd.DataFrame: The dataframe with the imputed values applied and qa cols dropped.
    """
    to_impute_cols = get_imputation_cols(config)

    # Check that the imputed columns exist in the dataframe
    missing_cols = [col for col in to_impute_cols if f"{col}_imputed" not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing imputed columns for: {missing_cols}")

    # Update columns with imputed version for the whole dataframe
    for col in to_impute_cols:
        df[col] = df[f"{col}_imputed"]

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

    if config["survey"]["survey_type"] == "PNP":
        to_drop += ["area"]

    else:
        to_drop += ["200_original", "pg_sic_class", "empty_pgsic_group"]
        to_drop += ["empty_pg_group", "200_imp_marker"]

    df = df.drop(columns=to_drop)

    return df


def create_new_backdata(backdata: pd.DataFrame, config) -> pd.DataFrame:
    """Create a new backdata dataframe with the required columns from schema.

    The new backdata is created from the current year and is output to be used when
    running the pipeline in a future year. Eg, if the current run is 2023, the
    this new backdata will be used for 2024.
    Use the backdata toml schema to select the required columns from the backdata.
    filter for the clear and imputed statuses.

    Args:
        backdata (pd.DataFrame): The backdata dataframe.

    Returns:
        pd.DataFrame: The filtered backdata with only the required columns.
    """
    # filter for the clear and imputed statuses
    imp_markers_to_keep: list = ["R", "TMI", "CF", "MoR"]
    backdata = backdata.loc[backdata["imp_marker"].isin(imp_markers_to_keep)]

    # get the wanted columns from the backdata schema
    schema = load_schema("./config/backdata_schema.toml")
    wanted_cols = list(schema.keys())

    return backdata[wanted_cols]


# Add a column for imputation marker
def imputation_marker(df: pd.DataFrame) -> pd.DataFrame:
    """Initialize 'imp_marker' column with 'R' for clear responders or 'no_imputation.'
    Args:
        df (pd.DataFrame): the main dataset to add the imp_marker column to
    Returns:
        pd.DataFrame: dataframe with the imp_marker column updated
    """
    # Initialise imp_marker column with a value of 'R' for clear responders
    # and a default value "no_imputation" for all other rows for now.

    clear_responders_mask = df.status.isin(["Clear", "Clear - overridden"])
    df.loc[clear_responders_mask, "imp_marker"] = "R"
    df.loc[~clear_responders_mask, "imp_marker"] = "no_imputation"
    # update the imp_marker for constructed rows where force_imputation is False
    if ("is_constructed" in df.columns) and ("force_imputation" in df.columns):
        df.loc[
            (df["is_constructed"].isin([True]) & df["force_imputation"].isin([False])),
            "imp_marker",
        ] = "constructed"

    return df


def get_bool_columns(dfs: list[pd.DataFrame]) -> set:
    """Identify boolean-like columns in a list of dataframes.

    Args:
        dfs (list[pd.DataFrame]): List of dataframes to check.

    Returns:
        set: A set of boolean-like column names.
    """
    bool_columns = set()
    for df in dfs:
        for col in df.columns:
            if (
                df[col].notna().any()  # Ensure the col has at least one non-null value
                and df[col].dtype in [bool, "boolean"]
                and df[col].fillna(False).astype(str).isin(["True", "False"]).all()
            ):
                bool_columns.add(col)
            elif (
                df[col].notna().any()
                and df[col].dtype in [object, str, "string"]
                and df[col].fillna("False").astype(str).isin(["True", "False"]).all()
            ):
                bool_columns.add(col)
    return bool_columns


def concat_with_bool(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate a list of dataframes and ensure boolean-like columns are properly
    cast.

    Args:
        dfs (list[pd.DataFrame]): list of dataframes to concatenate.

    Returns:
        pd.DataFrame: The concatenated dataframe with updated boolean columns.
    """
    # Dynamically identify boolean-like columns in all DataFrames
    all_bool_columns = get_bool_columns(dfs)

    # Ensure boolean-like columns are cast to bool in all DataFrames
    dfs_bool = []
    for df in dfs:
        df = df.copy()  # Avoid modifying the original DataFrame
        for col in all_bool_columns:
            if (col in df.columns) and (df[col].dtype in [bool, "boolean"]):
                df[col] = df[col].fillna(False).astype(bool)
            elif (col in df.columns) and (df[col].dtype in [object, str, "string"]):
                df[col] = (
                    df[col]
                    .fillna("False")
                    .astype(str)
                    .map({"True": True, "False": False})
                )
        dfs_bool.append(df)

    # Concatenate the DataFrames
    concatenated_df = pd.concat(dfs_bool, ignore_index=True)

    # Ensure boolean-like columns retain their type in the concatenated DataFrame
    for col in all_bool_columns:
        if col in concatenated_df.columns:
            concatenated_df[col] = concatenated_df[col].fillna(False).astype(bool)

    return concatenated_df


def imputation_prep(df: pd.DataFrame, config: dict):
    """Create extra columns for imputation and fix 604 data issue.

    Args:
        df (pd.DataFrame): the main dataset to prepare for imputation
        config (dict): the configuration settings.

    Returns:
        pd.DataFrame: dataframe with extra columns added and data issues fixed.
    """
    # Create an 'instance' of value 1 for non-responders and refs with 'No R&D'
    df = instance_fix(df)
    df, wrong_604_qa_df = create_r_and_d_instance(df)

    # Add a column for imputation marker
    df = imputation_marker(df)

    # Get a list of all the target values and breakdown columns from the config
    to_impute_cols = get_imputation_cols(config)

    # Create imp_class column
    if config["survey"]["survey_type"] == "BERD":
        df = create_imp_class_col(df, ["200", "201"])
    elif config["survey"]["survey_type"] == "PNP":
        df = create_imp_class_col(df, ["area"], use_cellno=False)
        # fill nulls in question 200 (civil or defence) with "C"
        df = remove_defence_for_pnp(df, to_impute_cols)

    # fill zeros
    df = apply_fill_zeros(df, to_impute_cols)

    # Create new columns to hold the imputed values
    for col in to_impute_cols:
        df[f"{col}_imputed"] = df[col]

    return df, wrong_604_qa_df


def update_defence_rows(df: pd.DataFrame, to_impute_cols: list) -> pd.DataFrame:
    """
    Update rows with defence data to so the impute variables are null.

    However, if there no postcode in col 601, the defence rows should be removed.

    Args:
        df (pd.DataFrame): The DataFrame containing the full responses data.
        to_impute_cols (list): list of columns to be imputed.
        config (dict): The configuration settings.

    Returns:
        pd.DataFrame: The DataFrame with updated defence rows.
    """

    df = df.copy()
    # set the imputation columns to null for defence rows
    df.loc[df["200"] == "D", to_impute_cols] = np.nan

    rows_to_save_cond = df["601"].notnull()
    non_defence_cond = (df["200"] == "C") | (df["200"].isnull())

    df = df.copy().loc[rows_to_save_cond | non_defence_cond]
    # if there are any defence rows where we need to keep postcode data, set 200 to null
    df.loc[df["200"] == "D", "200"] = np.nan
    return df


def remove_defence_for_pnp(df: pd.DataFrame, to_impute_cols: list) -> pd.DataFrame:
    """
    Remove and log any defence rows for PNP data.

    Args:
        df (pd.DataFrame): The DataFrame containing the full responses data.

    Returns:
        pd.DataFrame: The filtered DataFrame with defence removed for PNP data.
    """
    # fill nulls in question 200 (civil or defence) with "C"
    df.loc[df["instance"] > 0, "200"] = df["200"].fillna("C")

    defence_rows = df.copy().loc[df["200"] == "D"]
    if len(defence_rows) > 0:
        def_list = [int(x) for x in defence_rows["reference"].unique()]
        ImputationHelpersLogger.info(f"Defence rows found in PNP data: {def_list}")

        # update the full responses df to remove defence rows but leaving
        # rows with other important data (eg, purchase data, postcodes)
        df = update_defence_rows(df, to_impute_cols)

    else:
        ImputationHelpersLogger.info("No defence rows found in PNP data")
    return df
