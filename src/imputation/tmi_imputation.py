import pandas as pd
import math
import numpy as np
from src.imputation import imputation as imp
from src.staging import spp_snapshot_processing as spp
from src.staging.spp_parser import parse_snap_data
import json

cur_path = "R:/BERD Results System Development 2023/DAP_emulation/survey_return_data/snapshot-202212-002-83b5bacd-7c99-45cf-b989-d43d762dd054.json"  # noqa
map_path = "R:/BERD Results System Development 2023/DAP_emulation/mappers/SIC07 to PG Conversion - From 2016 Data .csv"  # noqa

with open(cur_path, "r") as file:
    # Load JSON data from the file
    data = json.load(file)

# Join contributers and responses
contributors_df, responses_df = parse_snap_data(data)

cur_df = spp.full_responses(contributors_df, responses_df)

mapper = pd.read_csv(map_path, usecols=["2016 > Form PG", "2016 > Pub PG"]).squeeze()

sicmapper = pd.read_csv(map_path, usecols=["SIC 2007_CODE", "2016 > Pub PG"]).squeeze()

# PRE-PROCESS DATA #RDRP-207

map_dict = dict(zip(mapper["2016 > Form PG"], mapper["2016 > Pub PG"]))
map_dict = {i: j for i, j in map_dict.items()}

cur_df["201"] = pd.to_numeric(cur_df["201"], errors="coerce")
cur_df["201"] = cur_df["201"].map(map_dict)
cur_df["201"] = cur_df["201"].astype("category")

cur_df["200"] = cur_df["200"].apply(lambda v: str(v) if str(v) != "nan" else np.nan)
cur_df["200"] = cur_df["200"].astype("category")

keyvars = [
    "211",
    "305",
    "405",
    "406",
    "407",
    "408",
    "409",
    "410",
    "501",
    "502",
    "503",
    "504",
    "505",
    "506",
]


def create_imp_class_col(
    clean_df: pd.DataFrame, col_first_half: str, col_second_half: str, class_name: str
) -> pd.DataFrame:

    clean_df[f"{col_first_half}"] = clean_df[f"{col_first_half}"].astype(str)
    clean_df[f"{col_second_half}"] = clean_df[f"{col_second_half}"].astype(str)

    # Create class col with concatenation
    clean_df[f"{class_name}"] = (
        clean_df[f"{col_first_half}"] + "_" + clean_df[f"{col_second_half}"]
    )

    fil_df = filter_by_column_content(clean_df, "cellnumber", [817])
    # Create class col with concatenation + 817
    fil_df[f"{class_name}"] = fil_df[f"{class_name}"] + "_817"

    clean_df = apply_to_original(fil_df, clean_df)

    return clean_df


def filter_by_column_content(
    raw_df: pd.DataFrame, column: str, column_content: list
) -> pd.DataFrame:

    clean_df = raw_df[raw_df[column].isin(column_content)].copy()

    return clean_df


def apply_filters(df):

    clean_statuses = ["Clear", "Clear - overridden", "Clear - overridden SE"]
    filtered_df = imp.filter_by_column_content(df, "status", clean_statuses)

    return filtered_df


def fill_zeros(df: pd.DataFrame, column: str):
    return df[column].replace({math.nan: 0}).astype("float")


def apply_to_original(filtered_df, original_df):
    original_df.loc[filtered_df.index] = filtered_df
    return original_df


def clean_no_rd_data(filtered_df, og_df, target_variables: list):
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
        df["trim"] = "dont trim"
    else:
        df = filter_by_column_content(df, "trim_check", ["above_trim_threshold"])
        df.reset_index(drop=True, inplace=True)

        # define the bounds for trimming
        remove_lower = np.ceil(len(df) * (lower_perc / 100))
        remove_upper = np.ceil(len(df) * (1 - upper_perc / 100))

        # create trim tag (distinct from trim_check)
        # to mark which to trim for mean growth ratio

        df["trim"] = "do trim"
        df.loc[
            remove_lower : remove_upper - 2, "trim"
        ] = "dont trim"  # TODO check if needs to be inclusive of exlusive

    return df


def get_mean_growth_ratio(
    df: pd.DataFrame, unique_item: str, target_variable: str
) -> pd.DataFrame:

    # remove the "trim" tagged rows
    trimmed_df = filter_by_column_content(df, "trim", ["dont trim"])

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

    # Create an empty dict to store means
    mean_dict = dict.fromkeys(target_variable_list)

    copy_df = df.copy()
    filtered_df = copy_df[~(copy_df["imp_class"].str.contains("nan"))]

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
            trimmed_df = trim_bounds(trimcheck_df)

            if var == "201" or "305":
                if f"{var}_trimming" not in df:
                    df[f"{var}_trimming"] = np.nan
                df[f"{var}_trimming"].loc[trimmed_df["pre_index"]] = trimmed_df["trim"]
            else:
                continue
            # Calculate mean and count
            means = get_mean_growth_ratio(trimmed_df, k, var)

            # Update full dict with values
            if mean_dict[var] is None:
                mean_dict[var] = means
            else:
                mean_dict[var].update(means)

    return mean_dict, df


def tmi_imputation(df, target_variables, mean_dict):

    copy_df = df.copy()

    filtered_df = filter_by_column_content(
        df, "status", ["Form sent out", "Check needed"]
    )

    filtered_df = filtered_df[~(filtered_df["imp_class"].str.contains("nan"))]

    grp = filtered_df.groupby("imp_class")
    class_keys = list(grp.groups.keys())

    for item in class_keys:
        for var in target_variables:

            # Get grouped dataframe
            subgrp = grp.get_group(item)

            # Find all nulls for target variable in subgroup
            subnulls = subgrp[subgrp[var].isnull()].copy()

            # Replace nulls with means
            subnulls[var] = float(mean_dict[var][f"{var}_{item}_mean and count"][0])

            subnulls["imp_marker"] = "TMI"

            # Apply changes to copy_df
            final_df = apply_to_original(subnulls, copy_df)

    return final_df


def run_tmi(df, target_variables):

    df = tmi_pre_processing(df, target_variables)

    mean_dict, df = calculate_means(df, target_variables)

    final_df = tmi_imputation(df, target_variables, mean_dict)

    return final_df


final_df = run_tmi(cur_df, keyvars)

print("end debug")
