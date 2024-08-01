import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any

from src.imputation import imputation_helpers as hlp
from src.imputation.impute_civ_def import impute_civil_defence
from src.imputation import expansion_imputation as ximp

formtype_long = "0001"
formtype_short = "0006"

TMILogger = logging.getLogger(__name__)


def apply_to_original(filtered_df, original_df):
    """Overwrite a dataframe with updated row values."""
    original_df.update(filtered_df)
    return original_df


def filter_by_column_content(
    df: pd.DataFrame, column: str, column_content: list
) -> pd.DataFrame:
    """Function to filter dataframe column by content."""
    return df[df[column].isin(column_content)].copy()


def apply_fill_zeros(df: pd.DataFrame, target_variables: list):
    """Applies the fill zeros function to filtered dataframes.

    A mask is created to identify clear responders, excluding instance zero rows,
    but exclude "postcode only" rows.

    Zeros are then filled for the target values based on this mask, and the equivalent
    "_imputed" columns.

    Args:
        df (pd.DataFrame): The dataframe imputation is carried out on
        target_variables (List): A list of the target variables

    Returns:
        pd.DataFrame: The same dataframe with required nulls filled with zeros.
    """
    # Condition to exclude rows conaining no data and only postcodes
    excl_postcode_only_mask = ~(df["211"].isnull() & hlp.create_notnull_mask(df, "601"))

    zerofill_mask = (
        (df["instance"] != 0)
        & (df["status"].isin(["Clear", "Clear - overridden"]))
        & excl_postcode_only_mask
    )

    for var in target_variables:
        df.loc[zerofill_mask, var] = df.loc[zerofill_mask, var].fillna(0)
        df.loc[zerofill_mask, f"{var}_imputed"] = df.loc[
            zerofill_mask, f"{var}_imputed"
        ].fillna(0)

    return df


def sort_df(target_variable: str, df: pd.DataFrame) -> pd.DataFrame:
    """Sorts a dataframe by the target variable and other variables."""
    sort_list = [
        target_variable,
        "employees",
        "reference",
    ]
    sorted_df = df.sort_values(
        by=sort_list,
        ascending=[True, False, True],
    )

    return sorted_df


def apply_trim_check(
    df: pd.DataFrame,
    variable: str,
    trim_threshold,
) -> pd.DataFrame:
    """Checks if the number of records is above or below
    the threshold required for trimming.
    """
    # tag for those classes with more than trim_threshold

    df = df.copy()

    # Exclude zero values in trim calculations
    if len(df.loc[df[variable] > 0, variable]) <= trim_threshold:
        df["trim_check"] = "below_trim_threshold"
    else:
        df["trim_check"] = "above_trim_threshold"

    # Default is dont_trim for all entries
    df[f"{variable}_trim"] = False
    return df


def trim_bounds(
    df: pd.DataFrame,
    variable: str,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """Applies a marker to specifiy whether a mean calculation is to be trimmed.

    If the 'variable' column contains more than 'trim_threshold' non-zero values,
    the largest and smallest values are flagged for trimming based on the
    percentages specified.

    Args:
        df (pd.DataFrame): Dataframe of the imputation class
        config (Dict): the configuration settings
    """
    # get the trimming parameters from the config
    # TODO: add a function to check the config settings make sense
    # TODO: as is done in outlier-detection/auto_outliers
    trim_threshold = config["imputation"]["trim_threshold"]
    lower_perc = config["imputation"]["lower_trim_perc"]
    upper_perc = config["imputation"]["upper_trim_perc"]

    df = df.copy()
    # Save the index before the sorting
    df["pre_index"] = df.index

    # Add trimming threshold marker
    df = apply_trim_check(df, variable, trim_threshold)

    # trim only if the number of non-zeros is above trim_threshold
    full_length = len(df[variable])
    if len(df.loc[df[variable] > 0, variable]) <= trim_threshold:
        df[f"{variable}_trim"] = False
    else:
        df = filter_by_column_content(df, "trim_check", ["above_trim_threshold"])
        df.reset_index(drop=True, inplace=True)

        # define the bounds for trimming
        remove_lower = np.ceil(
            len(df.loc[df[variable] > 0, variable]) * (lower_perc / 100)
        )
        remove_upper = np.ceil(
            len(df.loc[df[variable] > 0, variable]) * (upper_perc / 100)
        )

        # create trim tag (distinct from trim_check)
        # to mark which to trim for mean growth ratio

        df[f"{variable}_trim"] = True
        lower_keep_index = remove_lower - 1
        upper_keep_index = full_length - remove_upper
        df.loc[lower_keep_index:upper_keep_index, f"{variable}_trim"] = False

    return df


def calculate_mean(
    df: pd.DataFrame, imp_class: str, target_variable: str
) -> Dict[str, float]:
    """Calculate the mean of the given target variable and imputation class.

    Dictionary values are created for the mean of the given target variable for
    the given imputation class, and also for the 'count' or number of values
    used to calculate the mean.

    Args:
        df (pd.DataFrame): The dataframe of 'clear' responses for the given
            imputation class
        imp_class (str): The given imputation class
        target_variable (str): The given target variable for which the mean is
            to be evaluated.

    Returns:
        Dict[str, float]
    """

    # remove the "trim" tagged rows
    trimmed_df = filter_by_column_content(df, f"{target_variable}_trim", [False])

    # convert to floats for mean calculation
    trimmed_df[target_variable] = trimmed_df[target_variable].astype("float")

    dict_trimmed_mean = {}

    # Add mean and count to dictionary
    dict_trimmed_mean[f"{target_variable}_{imp_class}_mean"] = trimmed_df[
        f"{target_variable}"
    ].mean()
    # Count is the number of non-null items in the trimmed class
    dict_trimmed_mean[f"{target_variable}_{imp_class}_count"] = len(
        trimmed_df.loc[~trimmed_df[target_variable].isnull()]
    )

    return dict_trimmed_mean


def create_mean_dict(
    df: pd.DataFrame,
    target_variable_list: List[str],
    config: Dict[str, Any],
) -> Tuple[Dict, pd.DataFrame]:
    """Calculate trimmed mean values for each target variable and imputation class.

    Returns a dictionary of mean values and counts for each unique class and variable
    Also returns a QA dataframe containing information on how trimming was applied.

    Args:
        df (pd.DataFrame): The dataframe for imputation
        target_variable List(str): List of target variables for which the mean is
            to be evaluated.
        config: Dict[str, Any]: the pipeline configuration settings
    Returns:
        Tuple[Dict[str, float], pd.DataFrame]
    """
    TMILogger.debug("Creating mean dictionaries")
    df_list = []

    # Create an empty dict to store means
    mean_dict = dict.fromkeys(target_variable_list)

    # Filter for clear statuses
    clear_statuses = ["210", "211"]
    filtered_df = filter_by_column_content(df, "statusencoded", clear_statuses)

    # Filter out imputation classes that are missing either "200" or "201"
    filtered_df = filtered_df[~(filtered_df["imp_class"].str.contains("nan"))]

    # Group by imp_class
    grp = filtered_df.groupby("imp_class")
    class_keys = list(grp.groups.keys())

    for var in target_variable_list:
        for k in class_keys:
            # Get subgroup dataframe
            subgrp = grp.get_group(k)
            # Sort by target_variable, df['employees'], reference
            sorted_df = sort_df(var, subgrp)

            # Apply trimming
            trimmed_df = trim_bounds(sorted_df, var, config)

            tr_df = trimmed_df.set_index("pre_index")

            df_list.append(tr_df)
            # Create a dictionary with the target variable as the key
            # and a dictionary containing the
            means = calculate_mean(trimmed_df, k, var)

            # Update full dict with values
            if mean_dict[var] is None:
                mean_dict[var] = means
            else:
                mean_dict[var].update(means)

    df = pd.concat(df_list)
    df["qa_index"] = df.index
    df = df.groupby(["pre_index"], as_index=False).first()

    return mean_dict, df


def apply_tmi(df, target_variables, mean_dict):
    """Function to replace the not clear statuses with the mean value
    for each imputation class"""

    df = df.copy()

    filtered_df = filter_by_column_content(
        df, "status", ["Form sent out", "Check needed"]
    )

    # Filter out any cases where 200 or 201 are missing from the imputation class
    # This ensures that means are calculated using only valid imputation classes
    # Since imp_class is string type, any entry containing "nan" is excluded.
    filtered_df = filtered_df[~(filtered_df["imp_class"].str.contains("nan"))]

    grp = filtered_df.groupby("imp_class")
    class_keys = list(grp.groups.keys())

    for var in target_variables:
        for imp_class_key in class_keys:

            # Get grouped dataframe
            imp_class_df = grp.get_group(imp_class_key)

            imp_class_df = imp_class_df.copy()

            if f"{var}_{imp_class_key}_mean" in mean_dict[var].keys():
                # Create new column with the imputed value
                imp_class_df[f"{var}_imputed"] = float(
                    mean_dict[var][f"{var}_{imp_class_key}_mean"]
                )
                imp_class_df["imp_marker"] = "TMI"

            else:
                imp_class_df[f"{var}_imputed"] = imp_class_df[var]
                imp_class_df["imp_marker"] = "No mean found"

            # Apply changes to copy_df
            final_df = apply_to_original(imp_class_df, filtered_df)

    final_df = apply_to_original(final_df, df)

    return final_df


def calculate_totals(df):

    df["emp_total_imputed"] = (
        df["emp_researcher_imputed"]
        + df["emp_technician_imputed"]
        + df["emp_other_imputed"]
    )

    df["headcount_tot_m_imputed"] = (
        df["headcount_res_m_imputed"]
        + df["headcount_tec_m_imputed"]
        + df["headcount_oth_m_imputed"]
    )

    df["headcount_tot_f_imputed"] = (
        df["headcount_res_f_imputed"]
        + df["headcount_tec_f_imputed"]
        + df["headcount_oth_f_imputed"]
    )

    df["headcount_total_imputed"] = (
        df["headcount_tot_m_imputed"] + df["headcount_tot_f_imputed"]
    )

    return df


def run_longform_tmi(
    longform_df: pd.DataFrame,
    config: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Function to run imputation end to end and returns the final
    dataframe back to the pipeline
        dataframe back to the pipeline
    Args:
        longform_df (pd.DataFrame): the dataset filtered for long form entries
        target_variables (list): key variables
        config (Dict): the configuration settings
    Returns:
        final_df: dataframe with the imputed valued added
        and counts columns
        qa_df: qa dataframe
    """
    TMILogger.info("Starting TMI long form imputation.")
    df = longform_df.copy()
    # TMI Step 2: impute for R&D type (civil or defence)
    df = impute_civil_defence(df)

    lf_target_variables = config["imputation"]["lf_target_vars"]
    # Fill zeros for clear responders with no r&d
    df = apply_fill_zeros(df, lf_target_variables)

    TMILogger.info("Starting TMI mean calculations.")
    mean_dict, qa_df = create_mean_dict(df, lf_target_variables, config)

    qa_df.set_index("qa_index", drop=True, inplace=True)
    qa_df = qa_df.drop("trim_check", axis=1)

    # apply the imputed values to the statuses requiring imputation
    tmi_df = apply_tmi(df, lf_target_variables, mean_dict)

    tmi_df.loc[qa_df.index, "211_trim"] = qa_df["211_trim"]
    tmi_df.loc[qa_df.index, "305_trim"] = qa_df["305_trim"]

    # TMI Step 4: expansion imputation
    expanded_df = ximp.run_expansion(tmi_df, config)

    # TMI Step 5: Calculate headcount and employment totals
    final_df = calculate_totals(expanded_df)

    TMILogger.info("TMI long form imputation completed.")
    return final_df, qa_df


def run_shortform_tmi(
    shortform_df: pd.DataFrame,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """Function to run imputation end to end and returns the final
    dataframe back to the pipeline
        dataframe back to the pipeline
    Args:
        shortform_df (pd.DataFrame): the dataset filtered for long form entries
        config (Dict): the configuration settings
    Returns:
        pd.DataFrame: dataframe with the imputed valued added
    """
    TMILogger.info("Starting TMI short form imputation.")

    sf_target_variables = list(config["breakdowns"])

    # logic to identify Census rows, only these will be used for shortform TMI
    census_mask = shortform_df["selectiontype"] == "C"
    to_impute_df = shortform_df.copy().loc[census_mask]
    not_imputed_df = shortform_df.copy().loc[~census_mask]

    # Fill zeros for clear responders with no r&d
    df = apply_fill_zeros(to_impute_df, sf_target_variables)

    mean_dict, qa_df = create_mean_dict(df, sf_target_variables, config)

    qa_df.set_index("qa_index", drop=True, inplace=True)
    qa_df = qa_df.drop("trim_check", axis=1)

    # apply the imputed values to the statuses requiring imputation
    tmi_df = apply_tmi(df, sf_target_variables, mean_dict)

    tmi_df.loc[qa_df.index, "211_trim"] = qa_df["211_trim"]
    tmi_df.loc[qa_df.index, "305_trim"] = qa_df["305_trim"]

    # concatinate qa dataframes from short forms and long forms
    shortforms_updated_df = pd.concat([tmi_df, not_imputed_df])

    TMILogger.info("TMI imputation completed.")
    return shortforms_updated_df, qa_df


def run_tmi(
    full_df: pd.DataFrame,
    config: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Function to run imputation end to end and returns the final
    dataframe back to the pipeline
        dataframe back to the pipeline
    Args:
        full_df (pd.DataFrame): the full responses spp dataframe
        config (Dict): the configuration settings
    Returns:
        final_df(pd.DataFrame): dataframe with the imputed valued added and counts
            columns
        qa_df: qa dataframe
    """
    # logic to identify rows that have had MoR or CF applied,
    # these should be excluded from TMI
    mor_mask = full_df["imp_marker"].isin(["CF", "MoR"])

    # create logic to select rows for longform and shortform TMI
    long_tmi_mask = (full_df["formtype"] == formtype_long) & ~mor_mask
    short_tmi_mask = (full_df["formtype"] == formtype_short) & ~mor_mask

    # create dataframes to be used for longform TMI and short form census TMI
    longform_df = full_df.copy().loc[long_tmi_mask]
    shortform_df = full_df.copy().loc[short_tmi_mask]

    # create dataframe for all the rows excluded from TMI
    excluded_df = full_df.copy().loc[mor_mask]

    # apply TMI imputation to long forms and then short forms
    longform_tmi_df, qa_df_long = run_longform_tmi(longform_df, config)

    shortform_tmi_df, qa_df_short = run_shortform_tmi(shortform_df, config)

    # concatinate the short and long form with the excluded dataframes
    full_df = pd.concat([longform_tmi_df, shortform_tmi_df, excluded_df])

    full_df = full_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    # concatinate qa dataframes from short forms and long forms
    full_qa_df = pd.concat([qa_df_long, qa_df_short])

    full_qa_df = full_qa_df.sort_values(
        ["formtype", "imp_class"], ascending=True
    ).reset_index(drop=True)

    # add the imputed rows for reference in the trimming qa dataframe
    # Note, the buiness area weren't sure theyd need this,
    # so we might be able to take it out later
    imputed_only_df = full_df.loc[full_df.imp_marker.isin(["MoR", "CF", "TMI"])]
    imputed_only_df = imputed_only_df.sort_values(
        ["formtype", "imp_class"], ascending=True
    ).reset_index(drop=True)
    full_qa_df = pd.concat([full_qa_df, imputed_only_df]).reset_index(drop=True)

    # rearange the rows to put the manual_trim column at the end
    cols = [col for col in full_df.columns if col != "manual_trim"] + ["manual_trim"]
    full_df = full_df[cols]

    qa_cols = [col for col in full_qa_df.columns if col != "manual_trim"] + [
        "manual_trim"
    ]
    full_qa_df = full_qa_df[qa_cols]

    TMILogger.info("TMI imputation completed.")
    return full_df, full_qa_df
