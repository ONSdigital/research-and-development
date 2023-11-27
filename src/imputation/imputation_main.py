"""The main file for the Imputation module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime
from itertools import chain

from src.staging.validation import load_schema
from src.imputation.imputation_helpers import split_df_on_trim
from src.imputation.apportionment import run_apportionment
from src.imputation.short_to_long import run_short_to_long
from src.imputation.MoR import run_mor
from src.imputation.sf_expansion import run_sf_expansion
from src.imputation import tmi_imputation as tmi
from src.outputs.outputs_helpers import create_output_df

ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    mapper: pd.DataFrame,
    backdata: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Run all the processes for the imputation module.
    
    These processes are, in order:
    1) Apportionment: apportion 4xx and 5xx cols to create FTE and headcount cols
    2) Short to long form conversion: create new instances with short form questions
        mapped and apportioned to longform question equivalents
    3) Mean of Ratios imputation: (forwards imputation) where back data is available, 
        with "carry forward" as fall back data exists for prev but not current period.
    4) Trimmed Mean imputation (TMI): carried out where no backdata was avaialbe to 
        allow mean of ratios or carried forward method
    5) Short form expansion imputation: imputing for questions not asked in short forms

    Args:
        df (pd.DataFrame): the full responses spp data
        mapper (pd.DataFrame): dataframe with sic to product group mapper info
        backdata (pd.DataFrame): responses data for the previous period
        config (Dict): the configuration settings

    Returns:
        pd.DataFrame: dataframe with the imputed columns updated
    """

    # Get the target values and breakdown columns from the config
    lf_target_vars = config["imputation"]["lf_target_vars"]
    sum_cols = config["imputation"]["sum_cols"]
    bd_qs_lists = list(config["breakdowns"].values())
    bd_cols = list(chain(*bd_qs_lists))

    # Apportion cols 4xx and 5xx to create FTE and headcount values
    df = run_apportionment(df)
    # Convert shortform rows to longform format
    df = run_short_to_long(df)

    # Initialise imp_marker column with a value of 'R' for clear responders
    # and a default value "no_imputation" for all other rows for now.
    clear_responders_mask = df.status.isin(["Clear", "Clear - overridden"])
    df.loc[clear_responders_mask, "imp_marker"] = "R"
    df.loc[~clear_responders_mask, "imp_marker"] = "no_imputation"

    # remove records that have had construction applied before imputation
    if "is_constructed" in df.columns:
        constructed_df = df.copy().loc[
            df["is_constructed"].isin([True]) & df["force_imputation"].isin([False])
        ]
        constructed_df["imp_marker"] = "constructed"

        df = df.copy().loc[
            ~(df["is_constructed"].isin([True]) & df["force_imputation"].isin([False]))
        ]

    if "manual_trim" in df.columns:
        trimmed_df, df = split_df_on_trim(df, "manual_trim")

    # Create new columns to hold the imputed values
    orig_cols = lf_target_vars + bd_cols + sum_cols
    for col in orig_cols:
        df[f"{col}_imputed"] = df[col]

    # Run MoR
    if backdata is not None:
        df, links_df = run_mor(df, backdata, orig_cols, lf_target_vars, config)

    # Run TMI for long forms and short forms
    imputed_df, qa_df = tmi.run_tmi(df, mapper, config)

    # Run short form expansion
    imputed_df = run_sf_expansion(imputed_df, config)

    # join constructed rows back to the imputed df
    if "is_constructed" in df.columns:
        imputed_df = pd.concat([imputed_df, constructed_df])

    # join manually trimmed columns back to the imputed df
    if "manual_trim" in df.columns:
         imputed_df = pd.concat([imputed_df, trimmed_df])       

    imputed_df = imputed_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    # Output QA files
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    imp_path = config[f"{NETWORK_OR_HDFS}_paths"]["imputation_path"]

    if config["global"]["output_imputation_qa"]:
        ImputationMainLogger.info("Outputting Imputation files.")
        tdate = datetime.now().strftime("%Y-%m-%d")
        trim_qa_filename = f"trimming_qa_{tdate}_v{run_id}.csv"
        links_filename = f"links_qa_{tdate}_v{run_id}.csv"
        full_imp_filename = f"full_responses_imputed_{tdate}_v{run_id}.csv"

        # create trimming qa dataframe with required columns from schema
        schema_path = config["schema_paths"]["manual_trimming_schema"]
        schema_dict = load_schema(schema_path)
        trimming_qa_output = create_output_df(qa_df, schema_dict)

        write_csv(f"{imp_path}/imputation_qa/{links_filename}", links_df)
        write_csv(f"{imp_path}/imputation_qa/{trim_qa_filename}", trimming_qa_output)
        write_csv(f"{imp_path}/imputation_qa/{full_imp_filename}", imputed_df)

    ImputationMainLogger.info("Finished Imputation calculation.")

    # Create names for imputed cols
    imp_cols = [f"{col}_imputed" for col in orig_cols]

    # Update the original breakdown questions and target variables with the imputed
    imputed_df[orig_cols] = imputed_df[imp_cols]

    # Drop imputed values from df
    imputed_df = imputed_df.drop(columns=imp_cols)

    return imputed_df
