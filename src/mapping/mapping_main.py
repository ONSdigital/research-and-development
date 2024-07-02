"""The main file for the mapping module."""
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
):

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    if network_or_hdfs == "network":
        from src.utils import local_file_mods as mods

    elif network_or_hdfs == "hdfs":
        from src.utils import hdfs_mods as mods

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

    # Loading SIC to PG to alpha mapper
    sic_pg_alpha_mapper = stage_hlp.load_validate_mapper(
        "sic_pg_alpha_mapper_path",
        paths,
        MappingMainLogger,
        True,
        network_or_hdfs,
        "sic",
        "pg_alpha",
    )

    sic_pg_utf_mapper = stage_hlp.load_validate_mapper(
        "sic_pg_utf_mapper_path",
        paths,
        MappingMainLogger,
        True,
        network_or_hdfs,
        "SIC 2007_CODE",
        "2016 > Form PG",
    )
    cols_needed = ["SIC 2007_CODE", "2016 > Form PG"]
    sic_pg_utf_mapper = sic_pg_utf_mapper[cols_needed]
    mapper_path = paths["mapper_path"]
    mods.rd_write_csv(f"{mapper_path}/sic_pg_num.csv", sic_pg_utf_mapper)

    # Loading ru_817_list mapper
    load_ref_list_mapper = config["global"]["load_reference_list"]
    if load_ref_list_mapper:
        ref_list_817_mapper = stage_hlp.load_validate_mapper(
            "ref_list_817_mapper_path",
            paths,
            MappingMainLogger,
            False,
            network_or_hdfs,
        )
        # update longform references that should be on the reference list
        full_responses = hlp.update_ref_list(full_responses, ref_list_817_mapper)
    else:
        MappingMainLogger.info("Skipping loding the reference list mapper File.")
        ref_list_817_mapper = pd.DataFrame()

    # placeholder for running mapping

    # return mapped_df
