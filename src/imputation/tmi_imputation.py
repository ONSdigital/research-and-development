import pandas as pd
import numpy as np
from typing import Dict, List, Tuple

from src.staging.pg_conversion import sic_to_pg_mapper
from src.imputation.impute_civ_def import impute_civil_defence

formtype_long  = "0001"
formtype_short = "0006"

def apply_to_original(filtered_df, original_df):
    """Overwrite a dataframe with updated row values."""
    original_df.update(filtered_df)
    return original_df


def filter_by_column_content(
    df: pd.DataFrame, column: str, column_content: list
) -> pd.DataFrame:
    """Function to filter dataframe column by content."""
    return df[df[column].isin(column_content)].copy()


def instance_fix(df: pd.DataFrame):
    """Set instance to 1 for status 'Form sent out.'

    References with status 'Form sent out' initially have a null in the instance
    column.
    """
    filtered_df = filter_by_column_content(df, "status", ["Form sent out"])
    filtered_df["instance"] = 1
    updated_df = apply_to_original(filtered_df, df)
    return updated_df


def duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create a duplicate of references with no R&D and set instance to 1."""
    filtered_df = filter_by_column_content(df, "604", ["No"])
    filtered_df["instance"] = 1
    updated_df = pd.concat([df, filtered_df], ignore_index=True)
    updated_df = updated_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)
    return updated_df


def impute_pg_by_sic(
    df: pd.DataFrame, 
    sic_mapper: pd.DataFrame
    ) -> pd.DataFrame:
    """Impute missing product groups for companies that do no r&d,
    where instance = 1 and status = "Form sent out"
    using the SIC number ('rusic').

    Args:
        df (pd.DataFrame): SPP dataframe from staging
        sic_mapper (pd.DataFrame): SIC to PG mapper

    Returns:
        pd.DataFrame: Returns the original dataframe with new PGs
        overwriting the missing values
    """

    df = df.copy()

    df["200"] = df["200"].astype("category")

    long_df = filter_by_column_content(df, "formtype", [formtype_long])

    # Filter for q604 = No or status = "Form sent out"
    filtered_data = long_df.loc[
        (long_df["status"] == "Form sent out") | (long_df["604"] == "No")
    ]

    filtered_data = sic_to_pg_mapper(
        filtered_data, sic_mapper, target_col="201", formtype=formtype_long
    )

    updated_df = apply_to_original(filtered_data, df)

    return updated_df


def create_imp_class_col(
    df: pd.DataFrame,
    col_first_half: str,
    col_second_half: str,
    class_name: str = "imp_class",
) -> pd.DataFrame:
    """Creates a column for the imputation class.
    
    This is done by concatenating the R&D business type, C or D from  q200 
    and the product group from  q201.

    special case for cell number 817 is added as a suffix.

    Args:
        df (pd.DataFrame): Full dataframe
        col_first_half (str): The first half of the class string
        "200" is generally used.
        col_second_half (str): The second half of the class string
        "201" is generally used.
        class_name (str): The name of the column to save the class to.
        Defaults to "imp_class"
    Returns:
        pd.DataFrame: Dataframe which contains a new column with the
        imputation classes.
    """

    df = df.copy()

    # Create class col with concatenation
    if col_second_half:
        df[class_name] = (
            df[col_first_half].astype(str) + "_" + df[col_second_half].astype(str)
        )
    else:
        df[class_name] = df[col_first_half].astype(str)

    fil_df = filter_by_column_content(df, "cellnumber", [817])
    # Create class col with concatenation + 817
    fil_df[class_name] = fil_df[class_name] + "_817"

    df = apply_to_original(fil_df, df)

    return df


def fill_zeros(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Fills null values with zeros in a given column."""
    return df[column].fillna(0).astype("float")


def apply_fill_zeros(filtered_df, df, target_variables: list):
    """Applies the fill zeros function to filtered dataframes."""
    # only fill zeros where intance is not zero
    filtered_df = filtered_df.loc[filtered_df["instance"] != 0]
    # Replace 305 nulls with zeros
    filtered_df["305"] = fill_zeros(filtered_df, "305")
    df = apply_to_original(filtered_df, df)

    # Replace Nan with zero for companies with NO R&D Q604 = "No"

    no_rd = filter_by_column_content(df, "604", ["No"])
    no_rd = no_rd.loc[no_rd["instance"] != 0]
    for i in target_variables:
        no_rd[i] = fill_zeros(no_rd, i)

    df = apply_to_original(no_rd, df)

    # Return cleaned original dataset
    return df


def tmi_pre_processing(df, target_variables_list: list) -> pd.DataFrame:
    """Function that brings together the steps needed before calculating
    the trimmed mean"""
    filtered_df = df.loc[df["instance"] != 0]

    # Filter for clear statuses
    clear_statuses = ["210", "211"]
    clear_df = filter_by_column_content(filtered_df, "statusencoded", clear_statuses)

    # Fill zeros for no r&d and apply to original
    df = apply_fill_zeros(clear_df, df, target_variables_list)

    # Calculate imputation classes for each row
    imp_df = create_imp_class_col(df, "200", "201", "imp_class")

    return imp_df


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
    check_value=10,
) -> pd.DataFrame:  # TODO add check_value to a config
    """Checks if the number of records is above or below
    the threshold required for trimming.
    """
    # tag for those classes with more than check_value (currently 10)

    df = df.copy()

    # Exclude zero values in trim calculations
    if len(df.loc[df[variable] > 0, variable]) <= check_value:
        df["trim_check"] = "below_trim_threshold"
    else:
        df["trim_check"] = "above_trim_threshold"

    # Default is dont_trim for all entries
    df[f"{variable}_trim"] = False
    return df


def trim_bounds(
    df: pd.DataFrame,
    variable: str,
    check_value: int = 10,
    lower_perc: int = 15,  # TODO add percentages to config -
    upper_perc: int = 15,
) -> pd.DataFrame:
    """Applies a marker to specifiy whether a mean calculation is to be trimmed.

    If the 'variable' column contains more than 'check_value' non-zero values,
    the largest and smallest values are flagged for trimming based on the
    percentages specified.

    Args:
        df (pd.DataFrame): Dataframe of the imputation class
    """
    df = df.copy()
    # Save the index before the sorting
    df["pre_index"] = df.index

    # trim only if the number of non-zeros is above check_value
    full_length = len(df[variable])
    if len(df.loc[df[variable] > 0, variable]) <= check_value:
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
    target_variable_list: List[str]
    ) -> Tuple[Dict, pd.DataFrame]: 
    """Calculate trimmed mean values for each target variable and imputation class.

    Returns a dictionary of mean values and counts for each unique class and variable
    Also returns a QA dataframe containing information on how trimming was applied.

    Args:
        df (pd.DataFrame): The dataframe for imputation
        target_variable List(str): List of target variables for which the mean is 
            to be evaluated.        
    Returns:
        Tuple[Dict[str, float], pd.DataFrame]
    """
    df_list = []

    # Create an empty dict to store means
    mean_dict = dict.fromkeys(target_variable_list)

    # Filter for clear statuses
    clear_statuses = ["210", "211"]
    filtered_df = filter_by_column_content(df, "statusencoded", clear_statuses)

    # Filter out imputation classes that are missing either "200" or "201"
    filtered_df = filtered_df[~(filtered_df["imp_class"].str.contains("nan"))]
    # filtered_df = filtered_df[filtered_df["formtype"] == formtype_long]

    # Group by imp_class
    grp = filtered_df.groupby("imp_class")
    class_keys = list(grp.groups.keys())

    for var in target_variable_list:
        for k in class_keys:
            # Get subgroup dataframe
            subgrp = grp.get_group(k)
            # Sort by target_variable, df['employees'], reference
            sorted_df = sort_df(var, subgrp)

            # Add trimming threshold marker
            trimcheck_df = apply_trim_check(sorted_df, var)

            # Apply trimming marker
            trimmed_df = trim_bounds(trimcheck_df, var)

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
    df["imp_marker"] = "N/A"

    for var in target_variables:
        df[f"{var}_imputed"] = df[var]

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


def run_tmi(full_df, target_variables, sic_mapper):
    """Function to run imputation end to end and returns the final
    dataframe back to the pipeline"""
    copy_df = full_df.copy()

    # Filter for long form data
    long_df = filter_by_column_content(copy_df, "formtype", [formtype_long])

    # Create an 'instance' of value 1 for non-responders and refs with 'No R&D'
    long_df = instance_fix(long_df)
    long_df = duplicate_rows(long_df)

    # TMI Step 1: impute the Product Group
    df = impute_pg_by_sic(long_df, sic_mapper)

    df = impute_civil_defence(df, target_variables)

    df = tmi_pre_processing(df, target_variables)

    mean_dict, qa_df = create_mean_dict(df, target_variables)

    qa_df.set_index("qa_index", drop=True, inplace=True)

    qa_df = qa_df.drop("trim_check", axis=1)

    final_df = apply_tmi(df, target_variables, mean_dict)

    final_df.loc[qa_df.index, "211_trim"] = qa_df["211_trim"]
    final_df.loc[qa_df.index, "305_trim"] = qa_df["305_trim"]

    return final_df, qa_df
