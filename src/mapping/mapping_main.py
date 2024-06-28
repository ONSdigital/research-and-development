"""The main file for the staging and validation module."""
import logging
import pandas as pd
from typing import Callable

from src.mapping import mapping_helpers as hlp
from src.staging import staging_helpers as stage_hlp
from src.staging import validation as val

MappingMainLogger = logging.getLogger(__name__)


def run_mapping(
    full_responses,
    config: dict,
    check_file_exists: Callable,
    load_json: Callable,
    read_csv: Callable,
    write_csv: Callable,
    read_feather: Callable,
    write_feather: Callable,
    isfile: Callable,
    ):

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # Conditionally load paths
    paths = config[f"{network_or_hdfs}_paths"]

    pg_num_alpha = stage_hlp.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        val.validate_data_with_schema,
        val.validate_many_to_one,
        "pg_numeric",
        "pg_alpha",
    )

    # Load ultfoc (Foreign Ownership) mapper
    ultfoc_mapper = stage_hlp.load_validate_mapper(
        "ultfoc_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        val.validate_data_with_schema,
        hlp.validate_ultfoc_df,
    )

    # Load ITL mapper
    itl_mapper = stage_hlp.load_validate_mapper(
        "itl_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Loading cell number coverage
    cellno_df = stage_hlp.load_validate_mapper(
        "cellno_2022_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Loading SIC to PG to alpha mapper
    sic_pg_alpha_mapper = stage_hlp.load_validate_mapper(
        "sic_pg_alpha_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        val.validate_data_with_schema,
        val.validate_many_to_one,
        "sic",
        "pg_alpha",
    )

    sic_pg_utf_mapper = stage_hlp.load_validate_mapper(
        "sic_pg_utf_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        val.validate_data_with_schema,
        val.validate_many_to_one,
        "SIC 2007_CODE",
        "2016 > Form PG",
    )
    cols_needed = ["SIC 2007_CODE", "2016 > Form PG"]
    sic_pg_utf_mapper = sic_pg_utf_mapper[cols_needed]
    mapper_path = paths["mapper_path"]
    write_csv(f"{mapper_path}/sic_pg_num.csv", sic_pg_utf_mapper)

    # Loading ru_817_list mapper
    load_ref_list_mapper = config["global"]["load_reference_list"]
    if load_ref_list_mapper:
        ref_list_817_mapper = stage_hlp.load_validate_mapper(
            "ref_list_817_mapper_path",
            paths,
            check_file_exists,
            read_csv,
            MappingMainLogger,
            val.validate_data_with_schema,
            None,
        )
        # update longform references that should be on the reference list
        full_responses = hlp.update_ref_list(full_responses, ref_list_817_mapper)
    else:
        MappingMainLogger.info("Skipping loding the reference list mapper File.")
        ref_list_817_mapper = pd.DataFrame()


    # placeholder for running mapping

    # return mapped_df
