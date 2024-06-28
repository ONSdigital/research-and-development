"""The main file for the staging and validation module."""
import logging
import pandas as pd
from typing import Callable

from src.mapping import mapping_helpers as hlp
from src.mapping.pg_conversion import run_pg_conversion
from src.mapping.cellno_mapper import join_cellno_mapper

MappingMainLogger = logging.getLogger(__name__)


def run_mapping(
    full_responses,
    ni_full_responses,
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

    pg_num_alpha = hlp.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        hlp.validate_data_with_schema,
        hlp.validate_many_to_one,
        "pg_numeric",
        "pg_alpha",
    )

    # Load ultfoc (Foreign Ownership) mapper
    ultfoc_mapper = hlp.load_validate_mapper(
        "ultfoc_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        hlp.validate_data_with_schema,
        hlp.validate_ultfoc_df,
    )

    # Load ITL mapper
    itl_mapper = hlp.load_validate_mapper(
        "itl_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        hlp.validate_data_with_schema,
        None,
    )

    # Loading cell number coverage
    cellno_df = hlp.load_validate_mapper(
        "cellno_2022_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        hlp.validate_data_with_schema,
        None,
    )

    sic_pg_num = hlp.load_validate_mapper(
        "sic_pg_num_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        MappingMainLogger,
        hlp.validate_data_with_schema,
        hlp.validate_many_to_one,
        "SIC 2007_CODE",
        "2016 > Form PG",
    )

    # Loading ru_817_list mapper
    if config["global"]["survey_year"] == 2022:
        ref_list_817_mapper = hlp.load_validate_mapper(
            "ref_list_817_mapper_path",
            paths,
            check_file_exists,
            read_csv,
            MappingMainLogger,
            hlp.validate_data_with_schema,
            None,
        )
        # update longform references that should be on the reference list
        full_responses = hlp.update_ref_list(full_responses, ref_list_817_mapper)
    # Carry out product group conversion
    # Impute missing product group responses in q201 from SIC, then copy this to a new
    # column, pg_numeric. Finally, convert column 201 to alpha-numeric PG
    full_responses = run_pg_conversion(full_responses, pg_num_alpha, sic_pg_num)
    ni_full_responses = run_pg_conversion(ni_full_responses, pg_num_alpha, sic_pg_num)

    full_responses = join_cellno_mapper(full_responses, cellno_df)

    # placeholder for running mapping
    

    # return mapped_df
