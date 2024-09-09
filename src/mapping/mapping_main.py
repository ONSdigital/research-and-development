"""The main file for the mapping module."""
import logging
import os
from datetime import datetime
from typing import Callable

from src.mapping import mapping_helpers as hlp
from src.mapping.pg_conversion import run_pg_conversion
from src.mapping.ultfoc_mapping import join_fgn_ownership
from src.mapping.cellno_mapping import validate_join_cellno_mapper
from src.mapping.itl_mapping import join_itl_regions
from src.staging import staging_helpers as stage_hlp
from src.staging import validation as val

MappingMainLogger = logging.getLogger(__name__)


def run_mapping(
    full_responses,
    ni_full_responses,
    postcode_mapper,
    config: dict,
    rd_read_csv: Callable,
    rd_write_csv: Callable,
    rd_file_exists: Callable,
    run_id: int,
):
    """Perform mapping to the responses dataframes and output QA to csv.

    Args:
        full_responses (pd.DataFrame): The full responses dataframe.
        ni_full_responses (pd.DataFrame): The Northern Ireland full responses dataframe.
        postcode_mapper (pd.DataFrame): The postcode mapper dataframe.
        config (dict): The configuration settings.
        rd_read_csv (Callable): Function to read a csv file.
        rd_write_csv (Callable): Function to write a dataframe to a csv file.
        rd_file_exists (Callable): Function to check if a file exists.
        run_id (int): Unique identifier for the run.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: The BERD full responses and Northern Ireland 
            full responses dataframes with the mappers added.
    """
    # Load ultfoc (Foreign Ownership) mapper
    ultfoc_mapper = stage_hlp.load_validate_mapper(
        "ultfoc_mapper_path", config, MappingMainLogger, rd_file_exists, rd_read_csv
    )

    # Load ITL mapper
    itl_mapper = stage_hlp.load_validate_mapper(
        "itl_mapper_path",
        config,
        MappingMainLogger,
        rd_file_exists,
        rd_read_csv,
    )

    # Loading cell number coverage
    cellno_df = stage_hlp.load_validate_mapper(
        "cellno_path",
        config,
        MappingMainLogger,
        rd_file_exists,
        rd_read_csv,
    )

    # Load and validate the PG mappers
    pg_num_alpha = stage_hlp.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        config,
        MappingMainLogger,
        rd_file_exists,
        rd_read_csv,
    )
    val.validate_many_to_one(pg_num_alpha, "pg_numeric", "pg_alpha")

    # Load and validate the SIC to PG mappers, to be used to impute missing PG
    sic_pg_num = stage_hlp.load_validate_mapper(
        "sic_pg_num_mapper_path",
        config,
        MappingMainLogger,
        rd_file_exists,
        rd_read_csv,
    )
    val.validate_many_to_one(sic_pg_num, "SIC 2007_CODE", "2016 > Form PG")

    # For survey year 2022 and 2023 it's necessary to update the reference list
    # TODO: include in the config the ability to decide for future years about ref list
    if config["years"]["survey_year"] in [2022, 2023]:
        ref_list_817_mapper = stage_hlp.load_validate_mapper(
            "ref_list_817_mapper_path",
            config,
            MappingMainLogger,
            rd_file_exists,
            rd_read_csv,
        )
        full_responses = hlp.update_ref_list(full_responses, ref_list_817_mapper)

    # create a tuple for the full_responses and ni_full_responses
    responses = (full_responses, ni_full_responses)
    # Join the mappers to the full responses dataframe, with validation.
    responses = run_pg_conversion(responses, pg_num_alpha, sic_pg_num)
    responses = join_fgn_ownership(responses, ultfoc_mapper)
    responses = validate_join_cellno_mapper(responses, cellno_df, config)

    # unpack the responses
    full_responses, ni_full_responses = responses

    # Join the ITL regions mapper to the BERD full_responses dataframe
    full_responses = join_itl_regions(full_responses, postcode_mapper, itl_mapper, config)

    # Process the NI full responses if they exist
    if not ni_full_responses.empty:
        ni_full_responses = hlp.create_additional_ni_cols(ni_full_responses, config)

    # output QA files
    qa_path = config["mapping_paths"]["qa_path"]
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]

    if config["global"]["output_mapping_qa"]:
        MappingMainLogger.info("Outputting Mapping QA files.")
        full_responses_filename = (
            f"{survey_year}_full_responses_mapped_{tdate}_v{run_id}.csv"
        )
        rd_write_csv(os.path.join(qa_path, full_responses_filename), full_responses)
    MappingMainLogger.info("Finished Mapping QA calculation.")

    if config["global"]["output_mapping_ni_qa"] and not ni_full_responses.empty:
        MappingMainLogger.info("Outputting Mapping NI QA files.")
        full_responses_NI_filename = (
            f"{survey_year}_full_responses_ni_mapped_{tdate}_v{run_id}.csv"
        )
        rd_write_csv(
            os.path.join(qa_path, full_responses_NI_filename), ni_full_responses
        )
    MappingMainLogger.info("Finished Mapping NI QA calculation.")

    # return mapped_df
    return (full_responses, ni_full_responses, itl_mapper)
