import pandas as pd
import math
import numpy as np
from src.staging.pg_conversion import sic_to_pg_mapper


def apply_to_original(filtered_df, original_df):
    """Overwrites a dataframe with updated row values"""
    original_df.loc[filtered_df.index] = filtered_df
    return original_df


def filter_by_column_content(
    raw_df: pd.DataFrame, column: str, column_content: list
) -> pd.DataFrame:
    """A generic Pandas filter to be re-used in this module"""

    clean_df = raw_df[raw_df[column].isin(column_content)].copy()

    return clean_df


def instance_fix(df: pd.DataFrame):
    """Changes instance to 1 for status = "Form sent out" """
    filtered_df = filter_by_column_content(df, "status", ["Form sent out"])
    filtered_df["instance"] = 1
    updated_df = apply_to_original(filtered_df, df)
    return updated_df


def duplicate_rows(df: pd.DataFrame):
    """Duplicates rows for companies that do no R&D and changes instance to 1.
    This also sorts the dataframe on reference"""
    filtered_df = filter_by_column_content(df, "604", ["No"])
    filtered_df["instance"] = 1
    updated_df = pd.concat([df, filtered_df], ignore_index=True)
    updated_df = updated_df.sort_values(["reference", "instance"], ascending=[True, True]).reset_index(drop=True)
    return updated_df


def impute_pg_by_sic(df: pd.DataFrame, sic_mapper: pd.DataFrame):
    """Impute missing product groups for companies that do no r&d,
    where instance = 1 and status = "Form sent out"
    using the SIC number ('rusic').

    Args:
        df (pd.DataFrame): initial dataframe from staging
        sic_mapper (pd.DataFrame): SIC to PG mapper

    Returns:
        pd.DataFrame: Returns the original dataframe with new PGs
        overwriting the missing values
    """

    df["200"] = df["200"].astype("category")

    long_df = filter_by_column_content(df, "formtype", ["0001"])

    # Filter for q604 = No or status = "Form sent out"
    filtered_data = long_df.loc[
        (long_df["status"] == "Form sent out")
        | (long_df["604"] == "No")
    ]

    filtered_data = sic_to_pg_mapper(
        filtered_data, sic_mapper, target_col="201", formtype="0001"
    )

    updated_df = apply_to_original(filtered_data, df)

    return updated_df


# # Impute Business R&D type TODO: STEP 2
# filtered_data["200"] = np.random.choice(
#     ["C", "D"], size=len(filtered_data), p=[0.7, 0.3]
# )
# cur_df = apply_to_original(filtered_data, cur_df)


def create_imp_class_col(
    df: pd.DataFrame,
    col_first_half: str,
    col_second_half: str,
    class_name: str = "imp_class",
) -> pd.DataFrame:
    """Creates a column for the imputation class by concatenating
    the business type "200" and product group "201" columns. The
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

    # Create class col with concatenation
    df[f"{class_name}"] = (
        df[f"{col_first_half}"].astype(str) + "_" + df[f"{col_second_half}"].astype(str)
    )

    fil_df = filter_by_column_content(df, "cellnumber", [817])
    # Create class col with concatenation + 817
    fil_df[f"{class_name}"] = fil_df[f"{class_name}"] + "_817"

    df = apply_to_original(fil_df, df)

    return df


def fill_zeros(df: pd.DataFrame, column: str):
    """Fills null values with zeros"""
    return df[column].replace({math.nan: 0}).astype("float")


def apply_fill_zeros(filtered_df, df, target_variables: list):
    """Applies the fill zeros function to the specified columns"""
    # Replace 305 nulls with zeros
    filtered_df["305"] = fill_zeros(filtered_df, "305")
    df = apply_to_original(filtered_df, df)

    # Replace Nan with zero for companies with NO R&D Q604 = "No"
    no_rd = filter_by_column_content(df, "604", ["No"])

    for i in target_variables:
        no_rd[i] = fill_zeros(no_rd, i)

    df = apply_to_original(no_rd, df)

    # Return cleaned original dataset
    return df


def tmi_pre_processing(df, target_variables_list: list) -> pd.DataFrame:
    """Function that brings together the steps needed before calculating
    the trimmed mean"""
    # Create a copy for reference
    copy_df = df.copy()

    # Filter for long form data
    long_df = filter_by_column_content(copy_df, "formtype", ["0001"])

    # Filter for instance is not 0
    filtered_df = long_df.loc[long_df["instance"] != 0]

    # Filter for clear statuses
    clear_statuses = ["210", "211"]
    clear_df = filter_by_column_content(filtered_df, "statusencoded", clear_statuses)

    # Fill zeros for no r&d and apply to original
    df = apply_fill_zeros(clear_df, df, target_variables_list)

    # Calculate imputation classes for each row
    imp_df = create_imp_class_col(df, "200", "201", "imp_class")

    return imp_df


def sort(target_variable: str, df: pd.DataFrame) -> pd.DataFrame:
    "Sorts a dataframe using the list and order provided."
    sort_list = [
        f"{target_variable}",
        "employees",
        "reference",
    ]
    sorted_df = df.sort_values(
        by=sort_list,
        ascending=[True, False, True],
    )

    return sorted_df


def trim_check(
    df: pd.DataFrame, check_value=10
) -> pd.DataFrame:  # TODO add check_value to a config
    """Checks if the number of records is above or below
    the threshold required for trimming.
    """
    # tag for those classes with more than check_value (currently 10)
    if len(df) <= check_value:  # TODO or is this just <
        df["trim_check"] = "below_trim_threshold"
    else:
        df["trim_check"] = "above_trim_threshold"

    return df


def trim_bounds(
    df: pd.DataFrame,
    variable: str,
    lower_perc=15,  # TODO add percentages to config -
    # check method inBERD_imputation_spec_V3
    upper_perc=15,
) -> pd.DataFrame:
    """Applies a marker to specifiy if a value is to be trimmed or not.
    NOTE: Trims only if more than 10 valid records
    """

    # Save the index before the sorting
    df["pre_index"] = df.index

    # Filter out zero values for trim calculations
    df = df.loc[df[variable] != 0]

    if len(df) <= 10:
        df[f"{variable}_trim"] = "dont trim"
    else:
        df = filter_by_column_content(df, "trim_check", ["above_trim_threshold"])
        df.reset_index(drop=True, inplace=True)

        # define the bounds for trimming
        remove_lower = np.ceil(len(df) * (lower_perc / 100))
        remove_upper = np.ceil(len(df) * (1 - upper_perc / 100))

        # create trim tag (distinct from trim_check)
        # to mark which to trim for mean growth ratio

        df[f"{variable}_trim"] = "do trim"
        df.loc[
            remove_lower : remove_upper - 2, f"{variable}_trim"
        ] = "dont trim"  # TODO check if needs to be inclusive of exlusive

    return df


def calculate_mean(
    df: pd.DataFrame, unique_item: str, target_variable: str
) -> pd.DataFrame:
    """Calculate the mean and count for each target and imputation class combination
    Returns a dictionary."""

    # remove the "trim" tagged rows
    trimmed_df = filter_by_column_content(df, f"{target_variable}_trim", ["dont trim"])

    # convert to floats for mean calculation
    trimmed_df[f"{target_variable}"] = trimmed_df[f"{target_variable}"].astype("float")

    dict_mean_growth_ratio = {}

    # Add mean and count to dictionary
    dict_mean_growth_ratio[f"{target_variable}_{unique_item}_mean"] = trimmed_df[f"{target_variable}"].mean()
    dict_mean_growth_ratio[f"{target_variable}_{unique_item}_count"] = len(trimmed_df[f"{target_variable}"])

    return dict_mean_growth_ratio


def calculate_means(df, target_variable_list):
    """Function to apply multiple steps to calculate the means for each target
    variable.
    Returns a dictionary of mean values and counts for each unique class and variable
    Also returns a QA dataframe containing information on how trimming was applied"""
    dfs_list = []

    # Create an empty dict to store means
    mean_dict = dict.fromkeys(target_variable_list)

    copy_df = df.copy()

    # Filter for clear statuses
    clear_statuses = ["210", "211"]
    filtered_df = filter_by_column_content(copy_df, "statusencoded", clear_statuses)

    # Filter out imputation classes that are missing either "200" or "201"
    filtered_df = filtered_df[~(filtered_df["imp_class"].str.contains("nan"))]
    filtered_df = filtered_df[filtered_df["formtype"] == "0001"]

    # Group by imp_class
    grp = filtered_df.groupby("imp_class")
    class_keys = list(grp.groups.keys())

    for var in target_variable_list:
        for k in class_keys:
            # Get subgroup dataframe
            subgrp = grp.get_group(k)
            # Sort by target_variable, df['employees'], reference
            sorted_df = sort(var, subgrp)

            # Add trimming threshold marker
            trimcheck_df = trim_check(sorted_df)

            # Apply trimming marker
            trimmed_df = trim_bounds(trimcheck_df, var)

            tr_df = trimmed_df.set_index("pre_index")

            dfs_list.append(tr_df)
            # Calculate mean and count # TODO: Rename this
            means = calculate_mean(trimmed_df, k, var)

            # Update full dict with values
            if mean_dict[var] is None:
                mean_dict[var] = means
            else:
                mean_dict[var].update(means)

    df = pd.concat(dfs_list)
    df["qa_index"] = df.index
    df = df.groupby(["pre_index"], as_index=False).first()

    return mean_dict, df


def tmi_imputation(df, target_variables, mean_dict):
    """Function to replace the not clear statuses with the mean value
    for each imputation class"""
    for var in target_variables:
        df["imp_marker"] = "N/A"
        df[f"{var}_imputed"] = df[var]

    copy_df = df.copy()

    filtered_df = filter_by_column_content(
        copy_df, "status", ["Form sent out", "Check needed"]
    )

    filtered_df = filtered_df[~(filtered_df["imp_class"].str.contains("nan"))]

    grp = filtered_df.groupby("imp_class")
    class_keys = list(grp.groups.keys())

    for var in target_variables:
        for item in class_keys:

            # Get grouped dataframe
            subgrp = grp.get_group(item)

            if f"{var}_{item}_mean" in mean_dict[var].keys():
                # Replace nulls with means
                subgrp[f"{var}_imputed"] = float(
                    mean_dict[var][f"{var}_{item}_mean"]
                )
                subgrp["imp_marker"] = "TMI"

            else:
                subgrp[f"{var}_imputed"] = subgrp[var]
                subgrp["imp_marker"] = "No mean found"

            # Apply changes to copy_df
            final_df = apply_to_original(subgrp, filtered_df)

    final_df = apply_to_original(final_df, df)

    return final_df


def run_tmi(full_df, target_variables, sic_mapper):
    """Function to run imputation end to end and returns the final
    dataframe back to the pipeline"""
    copy_df = full_df.copy()

    copy_df = instance_fix(copy_df)
    copy_df = duplicate_rows(copy_df)

    df = impute_pg_by_sic(copy_df, sic_mapper)

    df = tmi_pre_processing(df, target_variables)

    mean_dict, qa_df = calculate_means(df, target_variables)

    qa_df.set_index("qa_index", drop=True, inplace=True)

    qa_df = qa_df.drop("trim_check", axis=1)

    final_df = tmi_imputation(df, target_variables, mean_dict)

    final_df.loc[qa_df.index, "211_trim"] = qa_df["211_trim"]
    final_df.loc[qa_df.index, "305_trim"] = qa_df["305_trim"]

    return final_df, qa_df
