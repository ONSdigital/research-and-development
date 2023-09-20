import pandas as pd
import math
import numpy as np
from src.staging.pg_conversion import sic_to_pg_mapper

def apply_to_original(filtered_df, original_df):
    original_df.loc[filtered_df.index] = filtered_df
    return original_df


def filter_by_column_content(
    raw_df: pd.DataFrame, column: str, column_content: list
) -> pd.DataFrame:

    clean_df = raw_df[raw_df[column].isin(column_content)].copy()

    return clean_df


def impute_pg_by_sic(df: pd.DataFrame, sic_mapper: pd.DataFrame):
    """Impute missing product groups for companies that do no r&d
    and status = "Form sent out" using the SIC number ('rusic').

    Args:
        df (pd.DataFrame): initial dataframe from staging
        sic_mapper (pd.DataFrame): SIC to PG mapper

    Returns:
        pd.DataFrame: Returns the original dataframe with new PGs
        overwriting the missing values
    """
    
    df['200'] = df['200'].astype("category")
    
    long_df = filter_by_column_content(df, "formtype", ["0001"])

    # Filter for q604 = No or status = "Form sent out"
    filtered_data = long_df.loc[
        (long_df["status"] == "Form sent out") | (long_df["604"] == "No")
    ]

    filtered_data = sic_to_pg_mapper(filtered_data, sic_mapper)

    updated_df = apply_to_original(filtered_data, df)

    return updated_df


# # Impute Business R&D type TODO: STEP 2
# filtered_data["200"] = np.random.choice(
#     ["C", "D"], size=len(filtered_data), p=[0.7, 0.3]
# )
# cur_df = apply_to_original(filtered_data, cur_df)


def create_imp_class_col(
    clean_df: pd.DataFrame, col_first_half: str, col_second_half: str, class_name: str
) -> pd.DataFrame:

    # Create class col with concatenation
    clean_df[f"{class_name}"] = (
        clean_df[f"{col_first_half}"].astype(str)
        + "_"
        + clean_df[f"{col_second_half}"].astype(str)
    )

    fil_df = filter_by_column_content(clean_df, "cellnumber", [817])
    # Create class col with concatenation + 817
    fil_df[f"{class_name}"] = fil_df[f"{class_name}"] + "_817"

    clean_df = apply_to_original(fil_df, clean_df)

    return clean_df


def apply_filters(df):

    clean_statuses = ["Clear", "Clear - overridden", "Clear - overridden SE"]
    filtered_df = filter_by_column_content(df, "status", clean_statuses)

    return filtered_df


def fill_zeros(df: pd.DataFrame, column: str):
    return df[column].replace({math.nan: 0}).astype("float")


def clean_no_rd_data(filtered_df, og_df, target_variables: list):
    """TODO: Check if we need to do this"""
    # Replace 305 nulls with zeros
    filtered_df["305"] = fill_zeros(filtered_df, "305")
    og_df = apply_to_original(filtered_df, og_df)

    # Replace Nan with zero for companies with NO R&D Q604 = "No"
    no_rd = filter_by_column_content(og_df, "604", ["No"])

    for i in target_variables:
        no_rd[i] = fill_zeros(no_rd, i)

    og_df = apply_to_original(no_rd, og_df)

    # Return cleaned original dataset
    return og_df


def tmi_pre_processing(df, target_variables_list: list) -> pd.DataFrame:

    # Create a copy for reference
    copy_df = df.copy()

    # Filter for long form data
    long_df = filter_by_column_content(copy_df, "formtype", ["0001"])

    # Filter for good statuses
    fil_df = apply_filters(long_df)

    # Fill zeros for no r&d and apply to original

    df = clean_no_rd_data(fil_df, df, target_variables_list)

    # Calculate imputation classes for each row
    imp_df = create_imp_class_col(df, "200", "201", "imp_class")

    return imp_df


def sort(target_variable: str, df: pd.DataFrame) -> pd.DataFrame:
    sort_list = [
        f"{target_variable}",
        "employees",
        "reference",
    ]
    sorted_df = df.sort_values(
        by=sort_list,
        ascending=[True, False, True],
    )
    # sorted_df.reset_index(drop=True, inplace=True)

    return sorted_df


def trim_check(
    df: pd.DataFrame, check_value=10
) -> pd.DataFrame:  # TODO add check_value to a cofig
    """_summary_

    Args:
        df (pd.DataFrame, check_value, optional): _description_
        Defaults to 10)->pd.DataFrame(.

    Returns:
        _type_: _description_
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
    """_summary_

    Args:
        df (pd.DataFrame, lower_perc, optional): _description_.
        Defaults to 15, TODO add percentages to config

    Returns:
        _type_: _description_
    """
    # trim only if more than 10
    df["pre_index"] = df.index

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


def get_mean_growth_ratio(
    df: pd.DataFrame, unique_item: str, target_variable: str
) -> pd.DataFrame:

    # remove the "trim" tagged rows
    trimmed_df = filter_by_column_content(df, f"{target_variable}_trim", ["dont trim"])

    # convert to floats for now TODO: Should be done in staging
    trimmed_df[f"{target_variable}"] = trimmed_df[f"{target_variable}"].astype("float")

    dict_mean_growth_ratio = {}

    # Add mean and count to dictionary
    dict_mean_growth_ratio[f"{target_variable}_{unique_item}_mean and count"] = [
        trimmed_df[f"{target_variable}"].mean(),
        len(trimmed_df),
    ]

    return dict_mean_growth_ratio


def calculate_means(df, target_variable_list):

    dfs_list = []

    # Create an empty dict to store means
    mean_dict = dict.fromkeys(target_variable_list)

    copy_df = df.copy()

    filtered_df = apply_filters(copy_df)

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
            means = get_mean_growth_ratio(trimmed_df, k, var)

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
    
    for var in target_variables:
        df[f"{var}_imp_marker"] = "N/A"
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

            # Find all nulls for target variable in subgroup
            subnulls = subgrp[subgrp[var].isnull()].copy()

            if f"{var}_{item}_mean and count" in mean_dict[var].keys():
                # Replace nulls with means
                subnulls[f"{var}_imputed"] = float(mean_dict[var][f"{var}_{item}_mean and count"][0])
                subnulls[f"{var}_imp_marker"] = "TMI"

            else:
                subnulls[f"{var}_imputed"] = subnulls[var]
                subnulls[f"{var}_imp_marker"] = "No mean found"

            # Apply changes to copy_df
            final_df = apply_to_original(subnulls, filtered_df)

    final_df = apply_to_original(final_df, df)

    return final_df


def run_tmi(full_df, target_variables, sic_mapper):

    copy_df = full_df.copy()

    df = impute_pg_by_sic(copy_df, sic_mapper)

    df = tmi_pre_processing(df, target_variables)

    mean_dict, qa_df = calculate_means(df, target_variables)
    
    import csv
    with open('mycsvfile.csv', 'w') as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, mean_dict.keys())
        w.writeheader()
        w.writerow(mean_dict)

    qa_df.set_index("qa_index", drop=True, inplace=True)

    final_df = tmi_imputation(df, target_variables, mean_dict)

    final_df.loc[qa_df.index, "211_trim"] = qa_df["211_trim"]
    final_df.loc[qa_df.index, "305_trim"] = qa_df["305_trim"]

    return final_df, qa_df
