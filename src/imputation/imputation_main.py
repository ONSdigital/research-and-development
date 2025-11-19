"""The main file for the Imputation module."""

import logging
import os
import pandas as pd
from typing import Any
from collections.abc import Callable

from src.imputation import imputation_helpers as hlp
from src.imputation import tmi_imputation as tmi
from src.staging.validation import load_schema
from src.imputation.apportionment import run_apportionment
from src.imputation.short_to_long import run_short_to_long
from src.imputation.sf_expansion import run_sf_expansion
from src.imputation import manual_imputation as mimp
from src.imputation.MoR import run_mor
from src.outputs.outputs_helpers import create_output_df
from src.utils.breakdown_validation import run_breakdown_validation
from src.utils.helpers import filename_amender


ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(  # noqa: C901
    df: pd.DataFrame,
    manual_trimming_df: pd.DataFrame,
    backdata: pd.DataFrame,
    config: dict[str, Any],
    write_csv: Callable,
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
        df (pd.DataFrame): the main dataset to run through imputation
        manual_trimming_df (pd.DataFrame): dataframe with boolean column indicating
            which references should be manually trimmed in imputation
        backdata (pd.DataFrame): previous year's data
        config (dict): the configuration settings.
        write_csv (Callable): function to write a dataframe to a csv file

    Returns:
        pd.DataFrame: dataframe with the imputed columns updated
    """
    # Apportion cols 4xx and 5xx to create FTE and headcount values
    df = run_apportionment(df)

    # Convert shortform responses to longform format
    df = run_short_to_long(df)

    # create extra columns for imputation and fix data issues
    df, wrong_604_qa_df = hlp.imputation_prep(df, config)

    # remove records that have had construction applied before imputation
    if "is_constructed" in df.columns:
        cond = df["is_constructed"].isin([True]) & df["force_imputation"].isin([False])
        constructed_df = df.copy().loc[cond]
        df = df.copy().loc[~cond]

    # Load manual imputation file
    df = mimp.merge_manual_imputation(df, manual_trimming_df)
    trimmed_df, df = hlp.split_df_on_trim(df, "manual_trim")

    # Run MoR
    if backdata is not None:
        # MoR will be re-written with new backdata
        imputed_df, links_df = run_mor(df, backdata, config)
        ImputationMainLogger.info("MoR executed: Backdata present.")
    else:
        imputed_df = df.copy()
        ImputationMainLogger.info("MoR skipped: No backdata.")

    # Run TMI for long forms and short forms
    imputed_df, qa_df, trim_counts_qa = tmi.run_tmi(imputed_df, config)

    # After TMI imputation, overwrite the "604" == "No" in any records with
    # Status "check needed" (they are now being imputed")
    tmi_mask = imputed_df["status"].isin(["Check needed", "Form sent out"])
    imputation_mask = imputed_df["imp_marker"] == "TMI"
    imputed_df.loc[(tmi_mask & imputation_mask), "604"] = "Yes"

    # Perform TMI step 5, which calculates employment and headcount totals
    imputed_df = hlp.calculate_totals(imputed_df)

    # join constructed rows back to the imputed df
    # Note that constructed rows need to be included in short form expansion
    if "is_constructed" in df.columns:
        # Check that the column is cast to bool dtype and concatenate
        imputed_df = hlp.concat_with_bool([imputed_df, constructed_df])

    # join manually trimmed columns back to the imputed df
    if not trimmed_df.empty:
        (imputed_df, qa_df, links_df) = mimp.join_manual_trim_df_for_qa(
            imputed_df, qa_df, links_df, trimmed_df, config
        )

    # Run short form expansion imputation for BERD surveys
    if config["survey"]["survey_type"] == "BERD":
        imputed_df = run_sf_expansion(imputed_df, config)

    imputed_df = imputed_df.sort_values(
        ["reference", "instance"], ascending=[True, True]
    ).reset_index(drop=True)

    ImputationMainLogger.success("Finished Imputation calculation.")

    # Output QA files
    if config["global"]["output_imputation_qa"]:
        ImputationMainLogger.info("Outputting Imputation QA files.")
        qa_path = config["imputation_paths"]["qa_path"]

        trim_qa_filename = filename_amender("trimming_qa", config)
        full_imp_filename = filename_amender("full_responses_imputed", config)
        wrong_604_filename = filename_amender("wrong_604_error_qa", config)
        links_filename = filename_amender("links_qa", config)
        trimmed_counts_filename = filename_amender("tmi_trim_count_qa", config)

        # create trimming qa dataframe with required columns from schema
        schema_path = config["schema_paths"]["manual_trimming_schema"]
        schema_dict = load_schema(schema_path)
        trimming_qa_output = create_output_df(qa_df, schema_dict)

        write_csv(os.path.join(qa_path, trim_qa_filename), trimming_qa_output)
        write_csv(os.path.join(qa_path, trimmed_counts_filename), trim_counts_qa)
        write_csv(os.path.join(qa_path, wrong_604_filename), wrong_604_qa_df)

        write_csv(os.path.join(qa_path, full_imp_filename), imputed_df)
        if backdata is not None:
            write_csv(os.path.join(qa_path, links_filename), links_df)

    # remove rows and columns no longer needed from the imputed dataframe
    imputed_df = hlp.tidy_imputation_dataframe(imputed_df, config)

    # Check the imputed values are consistent with breakdown cols summing to totals.
    run_breakdown_validation(imputed_df, config, check="imputed")

    # optionally output backdata for imputation
    if config["global"]["output_backdata"]:
        ImputationMainLogger.info("Outputting backdata for imputation.")
        backdata_path = config["imputation_paths"]["backdata_out_path"]
        backdata_filename = filename_amender("backdata", config)
        new_backdata = hlp.create_new_backdata(imputed_df, config)
        write_csv(os.path.join(backdata_path, backdata_filename), new_backdata)

    return imputed_df
