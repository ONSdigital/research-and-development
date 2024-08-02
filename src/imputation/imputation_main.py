"""The main file for the Imputation module."""
import logging
import os
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime

from src.imputation import imputation_helpers as hlp
from src.imputation import tmi_imputation as tmi
from src.staging.validation import load_schema
from src.imputation.apportionment import run_apportionment
from src.imputation.short_to_long import run_short_to_long

# from src.imputation.MoR import run_mor
from src.imputation.sf_expansion import run_sf_expansion
from src.imputation import manual_imputation as mimp
from src.imputation.MoR import run_mor
from src.outputs.outputs_helpers import create_output_df


ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    manual_trimming_df: pd.DataFrame,
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

    # Apportion cols 4xx and 5xx to create FTE and headcount values
    df = run_apportionment(df)

    # Convert shortform responses to longform format
    df = run_short_to_long(df)

    # Initialise imp_marker column with a value of 'R' for clear responders
    # and a default value "no_imputation" for all other rows for now.
    clear_responders_mask = df.status.isin(["Clear", "Clear - overridden"])
    df.loc[clear_responders_mask, "imp_marker"] = "R"
    df.loc[~clear_responders_mask, "imp_marker"] = "no_imputation"

    # Create an 'instance' of value 1 for non-responders and refs with 'No R&D'
    df = hlp.instance_fix(df)
    df, wrong_604_qa_df = hlp.create_r_and_d_instance(df)

    # remove records that have had construction applied before imputation
    if "is_constructed" in df.columns:
        constructed_df = df.copy().loc[
            df["is_constructed"].isin([True]) & df["force_imputation"].isin([False])
        ]
        constructed_df["imp_marker"] = "constructed"

        df = df.copy().loc[
            ~(df["is_constructed"].isin([True]) & df["force_imputation"].isin([False]))
        ]

    # Get a list of all the target values and breakdown columns from the config
    to_impute_cols = hlp.get_imputation_cols(config)

    # Create new columns to hold the imputed values
    for col in to_impute_cols:
        df[f"{col}_imputed"] = df[col]

    # Create qa_path variable for QA output and manual imputation file
    qa_path = config["imputation_paths"]["qa_path"]

    # Load manual imputation file
    df = mimp.merge_manual_imputation(df, manual_trimming_df)
    trimmed_df, df = hlp.split_df_on_trim(df, "manual_trim")

    # Run MoR
    if backdata is not None:
        # MoR will be re-written with new backdata
        lf_target_vars = config["imputation"]["lf_target_vars"]
        df, links_df = run_mor(df, backdata, to_impute_cols, lf_target_vars, config)

    # Run TMI for long forms and short forms
    imputed_df, qa_df = tmi.run_tmi(df, config)

    # After imputation, correction to overwrite the "604" == "No" in any records with
    # Status "check needed"
    chk_mask = imputed_df["status"].str.contains("Check needed")
    imputation_mask = imputed_df["imp_marker"].isin(["TMI", "CF", "MoR"])
    # Changing all records that meet the criteria to "604" == "Yes"
    imputed_df.loc[(chk_mask & imputation_mask), "604"] = "Yes"

    # join constructed rows back to the imputed df
    # Note that constructed rows need to be included in short form expansion
    if "is_constructed" in df.columns:
        imputed_df = pd.concat([imputed_df, constructed_df])

    # Run short form expansion
    imputed_df = run_sf_expansion(imputed_df, config)

    # join manually trimmed columns back to the imputed df
    if not trimmed_df.empty:
        imputed_df = pd.concat([imputed_df, trimmed_df])
        qa_df = pd.concat([qa_df, trimmed_df]).reset_index(drop=True)

    imputed_df = imputed_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    # Output QA files
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]

    if config["global"]["output_imputation_qa"]:
        ImputationMainLogger.info("Outputting Imputation QA files.")
        trim_qa_filename = f"{survey_year}_trimming_qa_{tdate}_v{run_id}.csv"
        full_imp_filename = (
            f"{survey_year}_full_responses_imputed_{tdate}_v{run_id}.csv"
        )
        wrong_604_filename = f"{survey_year}_wrong_604_error_qa_{tdate}_v{run_id}.csv"

        # create trimming qa dataframe with required columns from schema
        schema_path = config["schema_paths"]["manual_trimming_schema"]
        schema_dict = load_schema(schema_path)
        trimming_qa_output = create_output_df(qa_df, schema_dict)

        write_csv(os.path.join(qa_path, trim_qa_filename), trimming_qa_output)
        write_csv(os.path.join(qa_path, full_imp_filename), imputed_df)
        write_csv(os.path.join(qa_path, wrong_604_filename), wrong_604_qa_df)
        if config["global"]["load_backdata"]:
            links_filename = f"{survey_year}_links_qa_{tdate}_v{run_id}.csv"
            write_csv(os.path.join(qa_path, links_filename), links_df)

    ImputationMainLogger.info("Finished Imputation calculation.")

    # remove rows and columns no longer needed from the imputed dataframe
    imputed_df = hlp.tidy_imputation_dataframe(
        imputed_df,
        config,
        ImputationMainLogger,
        to_impute_cols,
        write_csv,
        run_id,
    )

    # optionally output backdata for imputation
    if config["global"]["output_backdata"]:
        ImputationMainLogger.info("Outputting backdata for imputation.")
        backdata_path = config["imputation_paths"]["backdata_out_path"]
        backdata_filename = f"{survey_year}_backdata_{tdate}_v{run_id}.csv"
        new_backdata = hlp.create_new_backdata(imputed_df, config)
        write_csv(os.path.join(backdata_path, backdata_filename), new_backdata)

    return imputed_df
