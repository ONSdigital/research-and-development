import pandas as pd
import numpy as np

# TODO almost each could be further generalised in terms of
# variable and function names


def filter_by_column_content(
    raw_df: pd.DataFrame, column: str, column_content: str
) -> pd.DataFrame:
    """Filter a column for specific string content.

    Args:
        raw_df (pd.DataFrame): The dataframe to be filtered.
        column (str): The name of the column to be filtered.
        column_content (str): The content to be filtered on.

    Returns:
        pd.DataFrame: The filtered dataframe.
    """
    # filter for rows with column_content
    clean_df = raw_df[raw_df[column] == column_content].copy()

    return clean_df


def rename_imp_col(clean_df: pd.DataFrame):
    """
    This function renames columns in dataframe, replacing civ_or_def with 200
    and Product_group with 201 if they are present.

    Args:
        clean_df (pd.DataFrame): Input Dataframe to rename columns.

    Returns:
        pd.Dataframe: returns dataframe with renamed columns.
    """
    if "civ_or_def" in clean_df.columns:
        clean_df = clean_df.rename(columns={"civ_or_def": "200"})

    if "Product_group" in clean_df.columns:
        clean_df = clean_df.rename(columns={"Product_group": "201"})

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

    # TODO remove when using real data
    clean_df[f"{col_second_half}"] = clean_df[f"{col_second_half}"].astype(str)

    # Create class col with concatenation
    clean_df[f"{class_name}"] = (
        clean_df[f"{col_first_half}"] + "_" + clean_df[f"{col_second_half}"]
    )

    return clean_df


def filter_same_class(
    clean_df: pd.DataFrame, current_period: str, previous_period: str
) -> pd.DataFrame:
    """_summary_
    Args:
        clean_df (_type_): _description_

    Returns:
        _type_: _description_
    """

    # Filter for cols with same contents
    clean_same_class_df = clean_df[
        clean_df[f"{current_period}_class"] == clean_df[f"{previous_period}_class"]
    ].copy()

    return clean_same_class_df


def filter_pairs(
    clean_same_class_df: pd.DataFrame,
    target_variable: str,
    current_period: str,
    previous_period: str,
) -> pd.DataFrame:
    """_summary_ Checks two columns have same contents

    Args:
        clean_same_class_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # TODO needs more tweeks but essentially same as
    # filter_same_class but for target var not class
    matched_pairs_df = clean_same_class_df[
        (clean_same_class_df[f"{current_period}_{target_variable}_status"] == "Present")
        & (
            clean_same_class_df[f"{previous_period}_{target_variable}_status"]
            == "Present"
        )
    ].copy()

    return matched_pairs_df


def flag_nulls_and_zeros(
    target_variables_list: list,
    df: pd.DataFrame,
    curr_q: str,
    prev_q: str,
):
    """Flag target variables containing nulls or zreos.

    A new column {var}_valid is created for each var in the target variables.
    This is flagged with 1 if either the current period or previous period
    contains either a null or a zero. Otherwise, the flag is 0.

    Args:
        target_variables (list of str): the target variables
        df (pd.DataFrame): dataframe with current and previous periods
        curr_q (str): the current period
        prev_q (str): the previous period

    Returns:
        pd.DataFrame - a dataframe indicating nulls and zeros in target cols.
    """
    df = df.copy()
    for var in target_variables_list:
        cond1 = (df[f"{curr_q}_{var}"].isnull()) | (df[f"{prev_q}_{var}"].isnull())
        cond2 = (df[f"{curr_q}_{var}"] == 0) | (df[f"{prev_q}_{var}"] == 0)
        df[f"{var}_valid"] = np.where(cond1 | cond2, False, True)

    return df


def calc_growth_ratio(
    target_variable: str,
    df: pd.DataFrame,
    current_period: int,
    previous_period: int,
) -> pd.DataFrame:
    """Calculate the growth ratio for imputation.

    For the current target_variable, a growth_ratio column is created.
    A growth rate is calculated for those rows where the "target_value_valid"
    is true, meaning that there are no nulls or zeros in the previous or
    current periods, TODO and the status is a 'responder' status.

    If this condition is not met, the row has a null value in this column.

    Args:
        target_variable (str): The column name of the target variable.
        df (pd.DataFrame): The dataframe containing the target variables.
        current_period

    Returns:
        pd.DataFrame
    """
    flagged_df = flag_nulls_and_zeros(
        [target_variable], df, current_period, previous_period
    )

    responder_statuses = ["Clear", "Clear - overridden", "Clear - overridden SE"]

    cond1 = flagged_df[f"{target_variable}_valid"]
    cond2 = flagged_df["status"].isin(responder_statuses)

    flagged_df[f"{target_variable}_growth_ratio"] = np.where(
        cond1 & cond2,
        (
            df[f"{current_period}_{target_variable}"]
            / df[f"{previous_period}_{target_variable}"]
        ),
        np.nan,
    )
    df = flagged_df.drop(columns=[f"{target_variable}_valid"])

    return df


def sort_df(target_variable: str, df: pd.DataFrame) -> pd.DataFrame:
    """_summary_

    Args:
        target_variable (_type_): _description_

    Returns:
        _type_: _description_
    """
    # import ipdb

    # ipdb.set_trace()
    # sorted based on hard coded list (in arg by=)
    sorted_df = df.sort_values(
        by=[
            "200",
            "201",
            f"{target_variable}_growth_ratio",
            "employees",
            "reference",
        ],
        ascending=[True, True, True, False, True],
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
    dict_mean_growth_ratio: dict,  # TODO maybe rename to more decriptive name
    unique_item: str,
    target_variable: str,
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
    df_trimmed = filter_by_column_content(df, "trim", "dont trim")

    dict_mean_growth_ratio[
        f"{unique_item}_{target_variable}_mean_growth_ratio and count"
    ] = [
        df_trimmed[f"{target_variable}_growth_ratio"].mean(),
        len(df_trimmed),
    ]  # TODO check same len(df[f'{target_variable}_growth_ratio'] and len(df)
    # Also add to a dataframe:
    # df[f'{target_variable}_mean_growth_ratio'] = \
    # df[f'{target_variable}_growth_ratio'].mean()

    return dict_mean_growth_ratio  # TODO aka "imputation links"
    # what naming is best?


def loop_unique(
    df: pd.DataFrame,  # TODO think of a better name for function
    column: str,
    target_variables_list: list,
    current_period: str,
    previous_period: str,
    dict_mean_growth_ratio={},
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # will be looping over the class col
    # dict_mean_growth_ratio = {}  # TODO change to dict at the end
    # growth_ratio_dfs_list = []
    # for subsets of class and then on target variable at a time
    # growht ratio in calculated, data is sorted, trim check done,
    # trim bounds calculated and labelled then mean growth ratio
    # calculated and stored in a dictionary
    for unique_item in df[column].unique():
        unique_item_df = df[df[column] == unique_item].copy()
        for target_variable in target_variables_list:
            growth_ratio_df = calc_growth_ratio(
                target_variable, unique_item_df, current_period, previous_period
            )
            sorted_df = sort_df(target_variable, growth_ratio_df)
            trim_check_df = trim_check(sorted_df)
            trimmed_df = trim_bounds(trim_check_df)

            dict_mean_growth_ratio = get_mean_growth_ratio(
                trimmed_df, dict_mean_growth_ratio, unique_item, target_variable
            )
            # growth_ratio_dfs_list.append(growth_ratio_df)
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
    current_period: str,
    previous_period: str,
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    df_growth_ratio = df[~df.isin([np.nan]).any(axis=1)].copy()
    # df_growth_ratio = df[
    #     df[f"{current_period}_var1"] != "missing"
    # ].copy()  # TODO add f string

    dict_mean_growth_ratio = loop_unique(
        df_growth_ratio,
        column,
        target_variables_list,
        current_period,
        previous_period,
    )

    dfs_list = []
    df_final = df.copy()
    for class_name in df_final[f"{current_period}_class"].unique():
        for var in target_variables_list:
            df_other = df_final[
                df_final[f"{current_period}_class"] == class_name
            ].copy()
            df_other = df_other[
                df_other[f"{current_period}_{var}"].isnull()
            ].copy()  # change the name of df_final and df_other

            df_other[f"{class_name}_{var}_growth_ratio"] = dict_mean_growth_ratio[
                f"{class_name}_{var}_mean_growth_ratio and count"
            ][0]
            df_other[f"forwards_imputed_{var}"] = round(
                df_other[f"{class_name}_{var}_growth_ratio"]
                * df_other[f"{previous_period}_{var}"]
            ).astype("Int64")

            df_other = df_other.drop(columns=[f"{class_name}_{var}_growth_ratio"])
            dfs_list.append(df_other)

    df_out = pd.concat(dfs_list)

    return df_out


# TODO break this function into smaller functions
def backwards_imputation(
    df: pd.DataFrame,
    column: str,
    target_variables_list: list,
    current_period: str,
    previous_period: str,
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    df_growth_ratio = df[~df.isin([np.nan]).any(axis=1)].copy()
    # df_growth_ratio = df[
    #     df[f"{previous_period}_var1"] != "missing"
    # ].copy()  # TODO add f string

    dict_mean_growth_ratio = loop_unique(
        df_growth_ratio,
        column,
        target_variables_list,
        current_period,
        previous_period,
    )

    dfs_list = []
    df_final = df.copy()
    for class_name in df_final[f"{current_period}_class"].unique():
        for var in target_variables_list:
            df_other = df_final[
                df_final[f"{current_period}_class"] == class_name
            ].copy()
            df_other = df_other[
                df_other[f"{previous_period}_{var}"].isnull()
            ].copy()  # TODO change the name of df_final and df_other
            # TODO add f string to previous_period_var1
            df_other[f"{class_name}_{var}_growth_ratio"] = dict_mean_growth_ratio[
                f"{class_name}_{var}_mean_growth_ratio and count"
            ][0]
            df_other[f"backwards_imputed_{var}"] = round(
                df_other[f"{current_period}_{var}"]
                / df_other[f"{class_name}_{var}_growth_ratio"]
            ).astype("Int64")
            df_other = df_other.drop(columns=[f"{class_name}_{var}_growth_ratio"])
            dfs_list.append(df_other)

    df_out = pd.concat(dfs_list)

    return df_out


def run_imputation(
    # full_responses: pd.DataFrame,  # df = full_responses.copy()
    # column: str,
    test_df,
    target_variables_list: list,
    current_period: str,
    previous_period: str,
) -> pd.DataFrame:
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    # replacing civ_or_def with 200 and Product_group with 201
    test_df = rename_imp_col(test_df)

    # q200 is Business or business R&D type
    # q201 is Product Group
    clean_df = create_imp_class_col(test_df, "200", "201", f"{current_period}_class")
    clean_df.reset_index(drop=True, inplace=True)

    # TODO:flag_nulls_and_zeros() could can optionally be run to output a QA csv
    # indicating where there are nulls and zeros in the target variables
    # flagged_df = flag_nulls_and_zeros(
    #     target_variables_list, clean_df, current_period, previous_period
    # )

    forward_df = forward_imputation(
        clean_df,
        f"{current_period}_class",
        target_variables_list,
        current_period,
        previous_period,
    )

    backwards_df = backwards_imputation(
        clean_df,
        f"{current_period}_class",
        target_variables_list,
        current_period,
        previous_period,
    )

    return forward_df, backwards_df


def update_imputed(
    input_full, input_imputed, target_variables_list, direction, row_col="reference"
) -> pd.DataFrame():
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # As I'm doing this cell by cell
    # I could start with rows or columns in the for loops
    input_full["imputation_marker"] = "response"
    for row in input_imputed[row_col]:
        for col in target_variables_list:
            input_full.loc[input_full[row_col] == row, col] = input_imputed.loc[
                input_imputed[row_col] == row, f"{direction}_imputed_{col}"
            ][0]
            input_full.loc[
                input_full[row_col] == row, "imputation_marker"
            ] = f"{direction}_imputed"

    return input_full
