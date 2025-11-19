import pandas as pd
import logging

from typing import Any


ManualImputationLogger = logging.getLogger(__name__)


def merge_manual_imputation(
    df: pd.DataFrame,
    manual_trim_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Loads a manual trimming file if it exists and adds a manual_trim column
    to the DataFrame.

    Args:
        config (dict[str, Any]): The configuration dictionary.
        df (pd.DataFrame): The dataframe to be imputed.
        isfile_func (callable): The function to use to check if the file exists.
    Returns:
        pd.DataFrame: The DataFrame with the manual_trim column added.
    """
    if manual_trim_df is not None:
        if "manual_trim" in df.columns:
            df = df.drop(columns=["manual_trim"])

        dtypes_dict = {
            "reference": df["reference"].dtype,
            "instance": df["instance"].dtype,
            "manual_trim": bool,
        }
        manual_trim_df = manual_trim_df.astype(dtypes_dict)

        df = df.merge(manual_trim_df, on=["reference", "instance"], how="left")
        df["manual_trim"] = df["manual_trim"].fillna(False).astype(bool)

        ManualImputationLogger.info(
            "manual imputation dataframe joined to responses dataframe"
        )
    else:
        if "manual_trim" not in df.columns:
            df["manual_trim"] = False
    return df


def join_manual_trim_df_for_qa(
    imputed_df: pd.DataFrame,
    qa_df: pd.DataFrame,
    links_df: pd.DataFrame,
    trimmed_df: pd.DataFrame,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Joins the manual trimming dataframe to a sweries of QA dataframes.

    Args:
        imputed_df (pd.DataFrame): The responses dataframe with imputed and unimputed
            values
        qa_df (pd.DataFrame): The QA dataframe for trimming
        links_df (pd.DataFrame): The dataframe containing imputation links
        trimmed_df (pd.DataFrame): The responses that were trimmed in manual trimming
        config (dict[str, Any]): The configuration dictionary.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: The dataframes with
            the manual_trim column added.
    """
    imputed_df = pd.concat([imputed_df, trimmed_df])
    qa_df = pd.concat([qa_df, trimmed_df]).reset_index(drop=True)

    oth_cols = [
        "imp_class",
        "reference",
        "emp_total",
        "headcount_total",
        "manual_trim",
        "formtype",
    ]  # noqa
    wanted_cols = config["imputation"]["lf_target_vars"] + oth_cols
    wanted_cols = [col for col in wanted_cols if col in trimmed_df.columns]
    links_df = pd.concat([links_df, trimmed_df[wanted_cols]]).reset_index(drop=True)

    return imputed_df, qa_df, links_df
