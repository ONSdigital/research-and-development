"""The main file for the mapping module."""
import logging
from datetime import datetime
from typing import Callable

from src.mapping import mapping_helpers as hlp
from src.mapping.pg_conversion import run_pg_conversion
from src.mapping.ultfoc_mapping import join_fgn_ownership
from src.staging import staging_helpers as stage_hlp
from src.staging import validation as val

MappingMainLogger = logging.getLogger(__name__)


def run_mapping(
    full_responses,
    ni_full_responses,
    config: dict,
    write_csv: Callable,
    run_id: int,
):

    # Load ultfoc (Foreign Ownership) mapper
    ultfoc_mapper = stage_hlp.load_validate_mapper(
        "ultfoc_mapper_path",
        config,
        MappingMainLogger,
    )

    # Load ITL mapper
    itl_mapper = stage_hlp.load_validate_mapper(
        "itl_mapper_path",
        config,
        MappingMainLogger,
    )

    # Loading cell number coverage
    cellno_df = stage_hlp.load_validate_mapper(
        "cellno_path",
        config,
        MappingMainLogger,
    )

    # Load and validate the PG mappers
    pg_num_alpha = stage_hlp.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        config,
        MappingMainLogger,
    )
    val.validate_many_to_one(pg_num_alpha, "pg_numeric", "pg_alpha")

    # Load and validate the SIC to PG mappers, to be used to impute missing PG
    sic_pg_num = stage_hlp.load_validate_mapper(
        "sic_pg_num_mapper_path",
        config,
        MappingMainLogger,
    )
    val.validate_many_to_one(sic_pg_num, "SIC 2007_CODE", "2016 > Form PG")

    # For survey year 2022 only, it's necessary to update the reference list
    if config["years"]["survey_year"] == 2022:
        ref_list_817_mapper = stage_hlp.load_validate_mapper(
            "ref_list_817_mapper_path",
            config,
            MappingMainLogger,
        )
        full_responses = hlp.update_ref_list(full_responses, ref_list_817_mapper)

    # Join the mappers to the full responses dataframe, with validation.
    full_responses = run_pg_conversion(full_responses, pg_num_alpha, sic_pg_num)
    full_responses = join_fgn_ownership(full_responses, ultfoc_mapper)

    if ni_full_responses is not None:
        ni_full_responses = hlp.create_additional_ni_cols(ni_full_responses)
        ni_full_responses = run_pg_conversion(
            ni_full_responses, pg_num_alpha, sic_pg_num
        )
        ni_full_responses = join_fgn_ownership(
            ni_full_responses,
            ultfoc_mapper,
            is_northern_ireland=True,
        )

    # output QA files
    qa_path = config["mapping_paths"]["qa_path"]

    if config["global"]["output_mapping_qa"]:
        MappingMainLogger.info("Outputting Mapping QA files.")
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        full_responses_filename = (
            f"{survey_year}_full_responses_mapped_{tdate}_v{run_id}.csv"
        )

        write_csv(f"{qa_path}/{full_responses_filename}", full_responses)  # Changed
    MappingMainLogger.info("Finished Mapping QA calculation.")

    # return mapped_df
    return (full_responses, ni_full_responses, itl_mapper, cellno_df)
