import pandas as pd
import numpy as np

# TODO almost each could be further generalised in terms of
# variable and function names


def filter_data(
    raw_df: pd.DataFrame, column: str, column_content: str
) -> pd.DataFrame():
    """_summary_

    Args:
        raw_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # filter for rows with column_content
    clean_df = raw_df[raw_df[column] == column_content].copy()

    return clean_df


def create_class_col(
    clean_df: pd.DataFrame, col_first_half: str, col_second_half: str, class_name: str
) -> pd.DataFrame():
    """_summary_

    Args:
        clean_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Create class col with concatenation
    clean_df[f"{class_name}"] = (
        clean_df[f"{col_first_half}"] + clean_df[f"{col_second_half}"]
    )

    return clean_df


def filter_same_class(
    clean_df: pd.DataFrame, current_quarter: str, previous_quarter: str
) -> pd.DataFrame():
    """_summary_

    Args:
        clean_df (_type_): _description_

    Returns:
        _type_: _description_
    """

    # Filter for cols with same contents
    clean_same_class_df = clean_df[
        clean_df[f"{current_quarter}_class"] == clean_df[f"{previous_quarter}_class"]
    ].copy()

    return clean_same_class_df


def filter_pairs(
    clean_same_class_df: pd.DataFrame,
    target_variable: str,
    current_quarter: str,
    previous_quarter: str,
) -> pd.DataFrame():
    """_summary_ Checks two columns have same contents

    Args:
        clean_same_class_df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # TODO needs more tweeks but essentially same as
    # filter_same_class but for target var not class
    matched_pairs_df = clean_same_class_df[
        (
            clean_same_class_df[f"{current_quarter}_{target_variable}_status"]
            == "Present"
        )
        & (
            clean_same_class_df[f"{previous_quarter}_{target_variable}_status"]
            == "Present"
        )
    ].copy()

    return matched_pairs_df


def calc_growth_ratio(
    target_variable: str,
    df: pd.DataFrame,
    current_quarter: str,
    previous_quarter: str,
) -> pd.DataFrame():
    """_summary_

    Args:
        target_variable (_type_): _description_

    Returns:
        _type_: _description_
    """
    # for every target variable a growth ratio is calcualted
    df[f"{target_variable}_growth_ratio"] = (
        df[f"{current_quarter}_{target_variable}"]
        / df[f"{previous_quarter}_{target_variable}"]
    )

    return df


def sort(target_variable: str, df: pd.DataFrame) -> pd.DataFrame():
    """_summary_

    Args:
        target_variable (_type_): _description_

    Returns:
        _type_: _description_
    """
    # sorted based on hard coded list (in arg by=)
    sorted_df = df.sort_values(
        by=[
            "product_group",
            "civ_or_def",
            f"{target_variable}_growth_ratio",
            "employee_count",
            "ru_ref",
        ],
        ascending=[True, True, True, False, True],
    )
    sorted_df.reset_index(drop=True, inplace=True)

    return sorted_df


def trim_check(
    df: pd.DataFrame, check_value=10
) -> pd.DataFrame():  # TODO add check_value to a cofig
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


def trim(
    df: pd.DataFrame,
    lower_perc=15,  # TODO add percentages to config -
    # check method inBERD_imputation_spec_V3
    upper_perc=15,
) -> pd.DataFrame():
    """_summary_

    Args:
        df (pd.DataFrame, lower_perc, optional): _description_.
        Defaults to 15, TODO add percentages to config

    Returns:
        _type_: _description_
    """
    # trim only if more than 10
    df = filter_data(df, "trim_check", "above_trim_threshold")
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
) -> pd.DataFrame():
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
    df_trimmed = filter_data(df, "trim", "dont trim")

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
    current_quarter: str,
    previous_quarter: str,
) -> pd.DataFrame():
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # will be looping over the class col
    dict_mean_growth_ratio = {}  # TODO change to dict at the end
    # growth_ratio_dfs_list = []
    # for subsets of class and then on target variable at a time
    # growht ratio in calculated, data is sorted, trim check done,
    # trim bounds calculated and labelled then mean growth ratio
    # calculated and stored in a dictionary
    for unique_item in df[column].unique():
        unique_item_df = df[df[column] == unique_item].copy()
        for target_variable in target_variables_list:
            growth_ratio_df = calc_growth_ratio(
                target_variable, unique_item_df, current_quarter, previous_quarter
            )
            sorted_df = sort(target_variable, growth_ratio_df)
            trim_check_df = trim_check(sorted_df)
            trimmed_df = trim(trim_check_df)

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
    current_quarter: str,
    previous_quarter: str,
) -> pd.DataFrame():
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    df_growth_ratio = df[
        df["current_quarter_var1"] != "missing"
    ].copy()  # TODO add f string

    dict_mean_growth_ratio = loop_unique(
        df_growth_ratio,
        column,
        target_variables_list,
        current_quarter,
        previous_quarter,
    )

    # {'class1_var1_mean_growth_ratio and count':[0.5,11]}
    dfs_list = []
    df_final = df.copy()
    for class_name in df_final[f"{current_quarter}_class"].unique():
        for var in target_variables_list:
            print("dmklsd")
            df_other = df_final[
                df_final[f"{current_quarter}_class"] == class_name
            ].copy()
            df_other = df_other[
                df_other["current_quarter_var1"] == "missing"
            ].copy()  # change the name of df_final and df_other
            df_other[f"{class_name}_{var}_growth_ratio"] = int(
                dict_mean_growth_ratio[
                    f"{class_name}_{var}_mean_growth_ratio and count"
                ][0]
            )  # why doesn't float work?
            df_other[f"forwards_imputed_{var}"] = (
                df_other[f"{class_name}_{var}_growth_ratio"]
                * df_other[f"{previous_quarter}_{var}"]
            )
            df_other = df_other.drop(columns=[f"{class_name}_{var}_growth_ratio"])
            dfs_list.append(df_other)

    df_out = pd.concat(dfs_list)

    return df_out


# TODO break this function into smaller functions
def backwards_imputation(
    df: pd.DataFrame,
    column: str,
    target_variables_list: list,
    current_quarter: str,
    previous_quarter: str,
) -> pd.DataFrame():
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """

    df_growth_ratio = df[
        df["previous_quarter_var1"] != "missing"
    ].copy()  # TODO add f string

    dict_mean_growth_ratio = loop_unique(
        df_growth_ratio,
        column,
        target_variables_list,
        current_quarter,
        previous_quarter,
    )

    # {'class1_var1_mean_growth_ratio and count':[0.5,11]}
    dfs_list = []
    df_final = df.copy()
    for class_name in df_final[f"{current_quarter}_class"].unique():
        for var in target_variables_list:
            print("dmklsd")
            df_other = df_final[
                df_final[f"{current_quarter}_class"] == class_name
            ].copy()
            df_other = df_other[
                df_other["previous_quarter_var1"] == "missing"
            ].copy()  # TODO change the name of df_final and df_other
            # TODO add f string to previous_quarter_var1
            df_other[f"{class_name}_{var}_growth_ratio"] = int(
                dict_mean_growth_ratio[
                    f"{class_name}_{var}_mean_growth_ratio and count"
                ][0]
            )  # why doesn't float work?
            df_other[f"backwards_imputed_{var}"] = (
                df_other[f"{current_quarter}_{var}"]
                / df_other[f"{class_name}_{var}_growth_ratio"]
            )
            df_other = df_other.drop(columns=[f"{class_name}_{var}_growth_ratio"])
            dfs_list.append(df_other)

    df_out = pd.concat(dfs_list)

    return df_out
