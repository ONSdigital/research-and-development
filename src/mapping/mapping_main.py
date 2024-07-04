"""The main file for the mapping module."""
import logging

from src.mapping import mapping_helpers as hlp
from src.mapping.pg_conversion import run_pg_conversion

from src.staging import staging_helpers as stage_hlp

MappingMainLogger = logging.getLogger(__name__)


def run_mapping(
    full_responses,
    ni_full_responses,
    config: dict,
):

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # Conditionally load paths
    paths = config[f"{network_or_hdfs}_paths"]

    pg_num_alpha = stage_hlp.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        paths,
        MappingMainLogger,
        True,
        network_or_hdfs,
        "pg_numeric",
        "pg_alpha",
    )

    # Load ultfoc (Foreign Ownership) mapper
    ultfoc_mapper = stage_hlp.load_validate_mapper(
        "ultfoc_mapper_path",
        paths,
        MappingMainLogger,
        False,
        network_or_hdfs,
    )
    hlp.validate_ultfoc_df(ultfoc_mapper)

    # Load ITL mapper
    itl_mapper = stage_hlp.load_validate_mapper(
        "itl_mapper_path",
        paths,
        MappingMainLogger,
        False,
        network_or_hdfs,
    )

    # Loading cell number coverage
    cellno_df = stage_hlp.load_validate_mapper(
        "cellno_2022_path",
        paths,
        MappingMainLogger,
        False,
        network_or_hdfs,
    )

    sic_pg_num = stage_hlp.load_validate_mapper(
        "sic_pg_num_mapper_path",
        paths,
        MappingMainLogger,
        True,
        network_or_hdfs,
        "SIC 2007_CODE",
        "2016 > Form PG",
    )

    # Loading ru_817_list mapper
    if config["global"]["survey_year"] == 2022:
        ref_list_817_mapper = stage_hlp.load_validate_mapper(
            "ref_list_817_mapper_path",
            paths,
            MappingMainLogger,
            False,
            network_or_hdfs,
        )
        # update longform references that should be on the reference list
        full_responses = hlp.update_ref_list(full_responses, ref_list_817_mapper)

    # Carry out product group conversion
    # Impute missing product group responses in q201 from SIC, then copy this to a new
    # column, pg_numeric. Finally, convert column 201 to alpha-numeric PG
    full_responses = run_pg_conversion(full_responses, pg_num_alpha, sic_pg_num)

    # full_responses = join_cellno_mapper(full_responses, cellno_df)

    full_responses = hlp.join_fgn_ownership(full_responses, ultfoc_mapper)

    ni_full_responses = run_pg_conversion(ni_full_responses, pg_num_alpha, sic_pg_num)
    ni_full_responses = hlp.join_fgn_ownership(
        full_responses,
        ultfoc_mapper,
        is_northern_ireland=True,
    )

    # return mapped_df
    return (full_responses, ni_full_responses, itl_mapper, cellno_df)
