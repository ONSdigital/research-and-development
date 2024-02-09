"""The main file for the Imputation module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime

from src.imputation import imputation_helpers as hlp
from src.imputation import tmi_imputation as tmi
from src.staging.validation import load_schema
from src.imputation.pg_conversion import run_pg_conversion, pg_to_pg_mapper
from src.imputation.apportionment import run_apportionment
from src.imputation.short_to_long import run_short_to_long
from src.imputation.MoR import run_mor
from src.imputation.sf_expansion import run_sf_expansion
from src.imputation import manual_imputation as mimp
from src.outputs.outputs_helpers import create_output_df


ImputationMainLogger = logging.getLogger(__name__)


def run_imputation(
    df: pd.DataFrame,
    manual_trimming_df: pd.DataFrame,
    pg_num_alpha: pd.DataFrame,
    sic_pg_num: pd.DataFrame,
    backdata: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Run all the processes for the imputation module.

    These processes are, in order:
    1) PG conversion: convert PG column (201) from numeric to alpha-numeric
    2) Apportionment: apportion 4xx and 5xx cols to create FTE and headcount cols
    3) Short to long form conversion: create new instances with short form questions
        mapped and apportioned to longform question equivalents
    4) Mean of Ratios imputation: (forwards imputation) where back data is available,
        with "carry forward" as fall back data exists for prev but not current period.
    5) Trimmed Mean imputation (TMI): carried out where no backdata was avaialbe to
        allow mean of ratios or carried forward method
    6) Short form expansion imputation: imputing for questions not asked in short forms

    Args:
        df (pd.DataFrame): the full responses spp data
        mapper (pd.DataFrame): dataframe with sic to product group mapper info
        backdata (pd.DataFrame): responses data for the previous period
        config (Dict): the configuration settings

    Returns:
        pd.DataFrame: dataframe with the imputed columns updated
    """
    # Carry out product group conversion
    df = run_pg_conversion(
        df, pg_num_alpha, sic_pg_num, pg_column="201"
    )

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
    df, wrong_604_qa_df, wrong_604_ref_list = hlp.create_r_and_d_instance(df)

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

    # Create imp_path variable for QA output and manual imputation file
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    imp_path = config[f"{NETWORK_OR_HDFS}_paths"]["imputation_path"]

    # Load manual imputation file
    df = mimp.merge_manual_imputation(df, manual_trimming_df)
    trimmed_df, df = hlp.split_df_on_trim(df, "manual_trim")

    # Run MoR
    if backdata is not None:
        # Fix for different column names on network vs hdfs
        if NETWORK_OR_HDFS == "network":
            # Map PG numeric to alpha in column q201
            # This isn't done on HDFS as the column is already mapped
            backdata = pg_to_pg_mapper(
                backdata,
                pg_num_alpha,
                pg_column="q201",
                from_col= "pg_numeric",
                to_col="pg_alpha",
            )
            backdata = backdata.drop("pg_numeric", axis=1)

        lf_target_vars = config["imputation"]["lf_target_vars"]
        df, links_df = run_mor(df, backdata, to_impute_cols, lf_target_vars, config)

    # Run TMI for long forms and short forms
    imputed_df, qa_df = tmi.run_tmi(df, config)

    # After imputation, correction to ignore the "604" == "No" in any records with
    # Status "check needed"
    chk_mask = imputed_df["status"].str.contains("Check needed")
    imputation_mask = imputed_df["imp_marker"].isin(["TMI", "CF", "MoR"])
    # Changing all records that meet the criteria to "604" == "Yes"
    imputed_df.loc[(chk_mask & imputation_mask), "604"] = "Yes"

    # check "no R&D" records have only an instance 0 and one instance 1
    check_df = imputed_df.copy().loc[imputed_df["604"] == "No"]["reference", "instance", "604"]
    check_df["ref_count"] = check_df.groupby("reference")["instance"].sum()
    ImputationMainLogger.info("The following references are 'No R&D' ")
    ImputationMainLogger.info( "but have too many rows:")
    ImputationMainLogger.info(check_df[check_df["ref_count"]> 1])
    
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

    if config["global"]["output_imputation_qa"]:
        ImputationMainLogger.info("Outputting Imputation files.")
        tdate = datetime.now().strftime("%Y-%m-%d")
        trim_qa_filename = f"trimming_qa_{tdate}_v{run_id}.csv"
        links_filename = f"links_qa_{tdate}_v{run_id}.csv"
        full_imp_filename = f"full_responses_imputed_{tdate}_v{run_id}.csv"
        wrong_604_filename = f"wrong_604_error_qa_{tdate}_v{run_id}.csv"

        # create trimming qa dataframe with required columns from schema
        schema_path = config["schema_paths"]["manual_trimming_schema"]
        schema_dict = load_schema(schema_path)
        trimming_qa_output = create_output_df(qa_df, schema_dict)

        write_csv(f"{imp_path}/imputation_qa/{links_filename}", links_df)
        write_csv(f"{imp_path}/imputation_qa/{trim_qa_filename}", trimming_qa_output)
        write_csv(f"{imp_path}/imputation_qa/{full_imp_filename}", imputed_df)
        write_csv(f"{imp_path}/imputation_qa/{wrong_604_filename}", wrong_604_qa_df)
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
    
    return imputed_df
