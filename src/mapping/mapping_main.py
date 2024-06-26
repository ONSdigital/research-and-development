"""The main file for the staging and validation module."""

from typing import Callable
import src.staging.staging_helpers as helpers


def run_mapping(
    full_responses: df,
    config: dict,
    check_file_exists: Callable,
    load_json: Callable,
    read_csv: Callable,
    write_csv: Callable,
    read_feather: Callable,
    write_feather: Callable,
    isfile: Callable,
    run_id: int,):

    pg_num_alpha = helpers.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        val.validate_many_to_one,
        "pg_numeric",
        "pg_alpha",
    )

    # Load ultfoc (Foreign Ownership) mapper
    ultfoc_mapper = helpers.load_validate_mapper(
        "ultfoc_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        val.validate_ultfoc_df,
    )

    # Load ITL mapper
    itl_mapper = helpers.load_validate_mapper(
        "itl_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Loading cell number coverage
    cellno_df = helpers.load_validate_mapper(
        "cellno_2022_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Loading SIC to PG to alpha mapper
    sic_pg_alpha_mapper = helpers.load_validate_mapper(
        "sic_pg_alpha_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        val.validate_many_to_one,
        "sic",
        "pg_alpha",
    )

    sic_pg_utf_mapper = helpers.load_validate_mapper(
        "sic_pg_utf_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        val.validate_many_to_one,
        "SIC 2007_CODE",
        "2016 > Form PG",
    )
    cols_needed = ["SIC 2007_CODE", "2016 > Form PG"]
    sic_pg_utf_mapper = sic_pg_utf_mapper[cols_needed]
    mapper_path = paths["mapper_path"]
    write_csv(f"{mapper_path}/sic_pg_num.csv", sic_pg_utf_mapper)

    pg_detailed_mapper = helpers.load_validate_mapper(
        "pg_detailed_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Loading ru_817_list mapper
    load_ref_list_mapper = config["global"]["load_reference_list"]
    if load_ref_list_mapper:
        ref_list_817_mapper = helpers.load_validate_mapper(
            "ref_list_817_mapper_path",
            paths,
            check_file_exists,
            read_csv,
            StagingMainLogger,
            val.validate_data_with_schema,
            None,
        )
        # update longform references that should be on the reference list
        full_responses = helpers.update_ref_list(full_responses, ref_list_817_mapper)
    else:
        StagingMainLogger.info("Skipping loding the reference list mapper File.")
        ref_list_817_mapper = pd.DataFrame()

    return mapped_df
