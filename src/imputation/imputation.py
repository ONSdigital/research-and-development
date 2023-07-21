import pandas as pd
import numpy as np

# TODO almost each could be further generalised in terms of
# variable and function names


def filter_by_column_content(
    raw_df: pd.DataFrame, column: str, column_content: str
) -> pd.DataFrame:
    """_summary_

    Args:
        raw_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # filter for rows with column_content
    clean_df = raw_df[raw_df[column] == column_content].copy()

    return clean_df


def create_imp_class_col(
    clean_df: pd.DataFrame, col_first_half: str, col_second_half: str, class_name: str
) -> pd.DataFrame:
    """_summary_

    Args:
        clean_df (_type_): _description_

    Returns:
        _type_: _description_
    """

    #change contents to string format
    clean_df[f"{col_first_half}"] = clean_df[f"{col_first_half}"].astype(str)
    clean_df[f"{col_second_half}"] = clean_df[f"{col_second_half}"].astype(str)
    
    # Create class col with concatenation
    clean_df[f"{class_name}"] = (
        clean_df[f"{col_first_half}"] + clean_df[f"{col_second_half}"]
    )

    return clean_df


def filter_same_class(
    clean_df: pd.DataFrame, current: str, previous: str
) -> pd.DataFrame:
    """_summary_

    Args:
        clean_df (_type_): _description_

    Returns:
        _type_: _description_
    """

    # Filter for cols with same contents
    clean_same_class_df = clean_df[
        clean_df[f"{current}_class"] == clean_df[f"{previous}_class"]
    ].copy()

    return clean_same_class_df


def both_notnull(
    clean_same_class_df: pd.DataFrame,
    target_variable: str,
    current: str,
    previous: str,
) -> pd.DataFrame:
    """_summary_ Checks two columns have same contents

    Args:
        clean_same_class_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    # filter where both periods have data in target col
    matched_pairs_df = clean_same_class_df[
        (clean_same_class_df[f"{target_variable}_{current}"].notnull())
        & (clean_same_class_df[f"{target_variable}_{previous}"].notnull())
    ].copy()

    return matched_pairs_df


def calc_growth_ratio(
    target_variable: str,
    df: pd.DataFrame,
    current: str, # TODO change to period
    previous: str, # TODO change to period
) -> pd.DataFrame:
    """_summary_

    Args:
        target_variable (_type_): _description_

    Returns:
        _type_: _description_
    """
    # for every target variable a growth ratio is calcualted

    df[f"{target_variable}_growth_ratio"] = (
        df[f"{target_variable}_{current}"]
        / df[f"{target_variable}_{previous}"]
    )

    return df


def sort(
    target_variable: str,
    df: pd.DataFrame,
    current: str,
    trimmed_mean=False
) -> pd.DataFrame:
    """_summary_

    Args:
        target_variable (_type_): _description_

    Returns:
        _type_: _description_
    """
    # import ipdb
    if trimmed_mean == False:
        sort_list = [
                f"200_{current}",
                f"201_{current}",
                f"{target_variable}_growth_ratio",
                "reference", # TODO replace reference with codes for "employee_count", "ru_ref",
            ]
    if trimmed_mean == True:
        sort_list = [
                f"200_{current}",
                f"201_{current}",
                f"{target_variable}_{current}",
                "reference", # TODO replace reference with codes for "employee_count", "ru_ref",
            ]
    # ipdb.set_trace()
    # sorted based on hard coded list (in arg by=)
    sorted_df = df.sort_values(
        # by=[
        #     "product_group",
        #     "civ_or_def",
        #     f"{target_variable}_growth_ratio",
        #     "employee_count",
        #     "ru_ref",
        # ],
        #TODO sort_list shouldn't be hard coded
        by=sort_list,
        ascending=[True, True, False, True], # TODO check why changed to True
    )
    sorted_df.reset_index(drop=True, inplace=True)

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
    if len(df) < 5:
        df["trim"] = "dont trim"
    else:
        df = filter_by_column_content(df, "trim_check", "above_trim_threshold")
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
    df: pd.DataFrame,
    dict_mean_growth_ratio: dict,
    unique_item: str,
    target_variable: str,
    current: str,
    trimmed_mean=False
) -> pd.DataFrame:
    """_summary_

    Args:
        dict_mean_growth_ratio (_type_): _description_

    Returns:
        _type_: _description_
    """
    """Including the count of matched pairs
for each imputed variable and imputation
class in the output would be helpful for
the RAP team and MQD to determine the
quality of the imputed value. """

    # remove the "trim" tagged rows
    trimmed_df = filter_by_column_content(df, "trim", "dont trim")
    # TODO more than two missing periods - what do we do??
    if trimmed_mean:
        dict_mean_growth_ratio[
            f"{unique_item}_{target_variable}_mean and count"
        ] = [
            trimmed_df[f"{target_variable}_{current}"].mean(),
            len(trimmed_df),
        ]
    else:
        dict_mean_growth_ratio[
            f"{unique_item}_{target_variable}_mean_growth_ratio and count"
        ] = [
            trimmed_df[f"{target_variable}_growth_ratio"].mean(),
            len(trimmed_df),
        ]  # TODO check same len(df[f'{target_variable}_growth_ratio'] and len(df)
        # Also add to a dataframe:
        # df[f'{target_variable}_mean_growth_ratio'] = \
        # df[f'{target_variable}_growth_ratio'].mean()

    return dict_mean_growth_ratio  # TODO aka "imputation links"
    # what naming is best?


def loop_unique(
    df: pd.DataFrame,
    column: str,
    target_variables_list: list,
    current: str,
    previous: str,
    dict_mean_growth_ratio={},
    trimmed_mean=False
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # will be looping over the class col
    # for subsets of class and then on target variable one at a time
    # growht ratio in calculated, data is sorted, trim check done,
    # trim bounds calculated and labelled then mean growth ratio
    # calculated and stored in a dictionary
    for unique_item in df[column].unique():
        unique_item_df = df[df[column] == unique_item].copy()
        for target_variable in target_variables_list:
            if trimmed_mean==False:
                unique_item_df = calc_growth_ratio(
                    target_variable, unique_item_df, current, previous
                )
                sorted_df = sort(target_variable, unique_item_df, current)
            else:
                unique_item_df = mean_imp_check(df, current, previous, target_variable)
                unique_item_df = filter_by_column_content(unique_item_df, "mean_imp_check", "mean imputation")
                unique_item_df.drop(columns=['mean_imp_check'])
                if len(unique_item_df):
                    continue
                sorted_df = sort(target_variable, unique_item_df, current, trimmed_mean=True)
            trim_check_df = trim_check(sorted_df)
            trimmed_df = trim_bounds(trim_check_df)

            if trimmed_mean==False:
                dict_mean_growth_ratio = get_mean_growth_ratio(
                    trimmed_df, dict_mean_growth_ratio, unique_item, target_variable, current
                )
            if trimmed_mean==True:
                dict_mean_growth_ratio = get_mean_growth_ratio(
                    trimmed_df, dict_mean_growth_ratio, unique_item, target_variable, current,
                    trimmed_mean=True
                )
            # could also store in a df?

    # growth_ratio_df = pd.concat(growth_ratio_dfs_list)
    # could also store ina dataframe

    return dict_mean_growth_ratio  # , growth_ratio_df
    # aka "imputation links" - what naming is best?


# TODO break this function into smaller functions
def forward_imputation(
    df: pd.DataFrame,
    column: str,
    target_variables_list: list,
    current: str, #TODO remove quarter
    previous: str,
    trimmed_mean=False
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    growth_ratio_df = df[~df.isin([np.nan]).any(axis=1)].copy()


    if trimmed_mean:
        
        dict_mean_growth_ratio = loop_unique(
        growth_ratio_df,
        column,
        target_variables_list,
        current,
        previous,
        trimmed_mean=True
        )
        
        dfs_list = []
        temp_df = df.copy()
        for class_name in temp_df[f"{current}_class"].unique():
            for var in target_variables_list:
                temp2_df = temp_df[ # TODO is there a better name than temp2_df?
                    temp_df[f"{current}_class"] == class_name
                ].copy()
                temp2_df = temp2_df[
                    temp2_df[f"{var}_{current}"].isnull()
                ].copy()  # TODO change the name of temp_df and temp2_df?
                temp2_df[f"forwards_imputed_{var}"] = float(
                    dict_mean_growth_ratio[
                        f"{class_name}_{var}_mean and count"
                    ][0]
                )
                dfs_list.append(temp2_df)

        df_out = pd.concat(dfs_list)

    else:
        dict_mean_growth_ratio = loop_unique(
        growth_ratio_df,
        column,
        target_variables_list,
        current,
        previous,
        )

        dfs_list = []
        temp_df = df.copy()
        for class_name in temp_df[f"{current}_class"].unique():
            for var in target_variables_list:
                temp2_df = temp_df[
                    temp_df[f"{current}_class"] == class_name
                ].copy()
                temp2_df = temp2_df[
                    temp2_df[f"{var}_{current}"].isnull()
                ].copy()  # change the name of temp_df and temp2_df
                temp2_df[f"{class_name}_{var}_growth_ratio"] = float(
                    dict_mean_growth_ratio[
                        f"{class_name}_{var}_mean_growth_ratio and count"
                    ][0]
                )  # why doesn't float work?
                temp2_df[f"forwards_imputed_{var}"] = (
                    temp2_df[f"{class_name}_{var}_growth_ratio"]
                    * temp2_df[f"{var}_{previous}"]
                )
                temp2_df = temp2_df.drop(columns=[f"{class_name}_{var}_growth_ratio"])
                dfs_list.append(temp2_df)

        df_out = pd.concat(dfs_list)

    return df_out


# TODO break this function into smaller functions
def backwards_imputation(
    df: pd.DataFrame,
    column: str,
    target_variables_list: list,
    current: str, # TODO remove quarter
    previous: str,
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    growth_ratio_df = df[~df.isin([np.nan]).any(axis=1)].copy()

    dict_mean_growth_ratio = loop_unique(
        growth_ratio_df,
        column,
        target_variables_list,
        current,
        previous,
    )

    dfs_list = []
    temp_df = df.copy()
    for class_name in temp_df[f"{current}_class"].unique():
        for var in target_variables_list:
            
            #filter by class
            temp2_df = temp_df[
                temp_df[f"{current}_class"] == class_name
            ].copy()

            #filter for missing previous values
            temp2_df = temp2_df[
                temp2_df[f"{var}_{previous}"].isnull()
            ].copy()  # TODO change the name of temp_df and temp2_df


            temp2_df[f"{class_name}_{var}_growth_ratio"] = int(
                dict_mean_growth_ratio[
                    f"{class_name}_{var}_mean_growth_ratio and count"
                ][0]
            )  # why doesn't float work?

            temp2_df[f"{var}_{current}"] = temp2_df[
                f"{var}_{current}"
            ].astype("int64")

            temp2_df[f"backwards_imputed_{var}"] = (
                temp2_df[f"{var}_{current}"]
                / temp2_df[f"{class_name}_{var}_growth_ratio"]
            )
            temp2_df = temp2_df.drop(columns=[f"{class_name}_{var}_growth_ratio"])
            dfs_list.append(temp2_df)

    df_out = pd.concat(dfs_list)

    return df_out


def run_imputation(
    # full_responses: pd.DataFrame,  # df = full_responses.copy()
    # column: str,
    df,
    target_variables_list: list,
    current: str,
    previous: str,
    trimmed_mean=False
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    # current = "2000"
    # previous = "1999"
    # target_variables_list = ["211", "305"]

    # TODO check these are right codes and remove hanrd coding
    clean_df = create_imp_class_col(
        df, f"200_{current}", f"201_{previous}", f"{current}_class"
    )
    clean_df.reset_index(drop=True, inplace=True)

    if trimmed_mean == True:
        forward_df = forward_imputation( #TODO change name 
            clean_df,
            f"{current}_class",
            target_variables_list,
            current,
            previous,
            trimmed_mean=True
        )

    if trimmed_mean == False:
        forward_df = forward_imputation(
            clean_df,
            f"{current}_class",
            target_variables_list,
            current,
            previous
        )

    backwards_df = backwards_imputation(
        clean_df,
        f"{current}_class",
        target_variables_list,
        current,
        previous
    )

    return forward_df, backwards_df


def update_imputed(
    input_full, input_imputed, target_variables_list, direction, ref_col="reference"
) -> pd.DataFrame():
    """Updates missing response data with imputed values for target variables
    Keyword Arguments:
        input_full -- DataFrame of the response data
        input_imputed -- DataFrame contining imputed values calculated in
        imputation module
        target_variables_list -- list of variable that need imputed if no
        response
        direciton -- can be either "forwards" or "backwards" depending on
        whether current or previous period has no response
    Returns:
        input_full: DataFrame with missing exchanged for imputed values
        for target variables
    """

    # add imputed tag column
    input_full["imputation_marker"] = "response"
    input_imputed["imputation_marker"] = f"{direction}_imputed"

    # exchange reference col for index
    # in preparation for update function
    input_full.index = input_full[ref_col]
    input_imputed.index = input_imputed[ref_col]

    # rename cols in preparation for update function
    for col in target_variables_list:
        input_imputed = input_imputed.rename(
            columns={f"{direction}_imputed_{col}": col}
        )

    # apply update - changes input_full inplace
    input_full.update(input_imputed)

    # change index back to normal
    input_full = input_full.reset_index(drop=True)

    return input_full


def mean_imp_check(df,
                   current,
                   previous,
                   var):
    """_summary_

    Args:
        df -- DataFrame containing response data
        col -- current period to conduct mean imputation check

    Returns:
        DataFrame: DataFrame with tag column added with "mean imputation" if 
        trimmed mean imputation is required and "not mean imputation" if not
        required
    """
    df["mean_imp_check"] = "not mean imputation"
    df.loc[(df[f"{var}_{current}"].isin([np.nan])) & (df[f"{var}_{previous}"].isin([np.nan])), "mean_imp_check"] = "mean imputation"
    return df

"""
df = pd.read_csv("R:/Vondy_created_test_data/imp_data.csv" )

filtered_df = filter_same_class(df, "2021", "2020")
# These have changed classes from last period
non_matches = df[df["2021_class"] != df["2020_class"]]
vars = [211, 305, 405, 406, 407, 408, 409, 410, 501, 502, 503, 504, 505, 506]
vars = [str(i) for i in vars]
matches = dict.fromkeys(vars)
mp_counts = dict.fromkeys(vars)
"""

# TODO Add to a function and compare runtimes:
# # Attempt to use groupby instead of nested for loop
# for i in vars:
#     # Check that both periods have data (not null) TODO: Does zero count as null?
#     matched_pairs_df = both_notnull(filtered_df, i, "2021", "2020")
#     # Save each dataset of clean matched pairs for each target variable
#     matches.update({i: matched_pairs_df})

#     # Calculate growth ratio for each class (use groupby)

#     # class_list = list(grp.groups.keys())
#     growth_df = calc_growth_ratio(i, matched_pairs_df, "2021", "2020")

#     # Trimming
#     grp = growth_df.groupby("2021_class")
#     class_keys = list(grp.groups.keys())
#     for k in class_keys:
#         subgrp = grp.get_group(k)
#         sorted_df = sort(i, subgrp, "2021")
#         trimcheck_df = trim_check(sorted_df)

#         # This only works for large classes
#         trimmed_df = trim_bounds(trimcheck_df)

#         mean_df = get_mean_growth_ratio(trimmed_df, {}, k, i, "2021")

#         if mp_counts[i]:
#             mp_counts[i].update(mean_df)
#         else:
#             mp_counts[i] = mean_df


#TODO this is where I got to 
# fwd_df = forward_imputation(df, "2021_class", vars, "2021", "2020")
# fwd_trim = forward_imputation(df, "2021_class", vars, "2021", "2020", trimmed_mean=True)

# #backwards current errors
# bwd_df = backwards_imputation(df, "2021_class", vars, "2021", "2020")
# run_imputation(df, vars, "2021", "2020")
