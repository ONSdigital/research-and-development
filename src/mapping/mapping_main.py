"""The main file for the mapping module."""
import logging

from src.mapping import mapping_helpers as hlp
from src.mapping.pg_conversion import run_pg_conversion
from src.mapping.ultfoc_mapping import join_fgn_ownership
from src.staging import staging_helpers as stage_hlp
from src.staging import validation as val
from src.utils import path_helpers as paths_hlp

MappingMainLogger = logging.getLogger(__name__)


def run_mapping(
    full_responses,
    ni_full_responses,
    config: dict,
    run_id: int
):

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # if network_or_hdfs == "network":
    #     from src.utils import local_file_mods as mods

    # elif network_or_hdfs == "hdfs":
    #     from src.utils import hdfs_mods as mods

    # create a config dictionary of mapper paths
    mapping_dict = paths_hlp.create_mapping_config(config)

    pg_num_alpha = stage_hlp.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        mapping_dict,
        MappingMainLogger,
        network_or_hdfs,
    )
    val.validate_many_to_one(pg_num_alpha, "pg_numeric", "pg_alpha")

    # Load ultfoc (Foreign Ownership) mapper
    ultfoc_mapper = stage_hlp.load_validate_mapper(
        "ultfoc_mapper_path",
        mapping_dict,
        MappingMainLogger,
        network_or_hdfs,
    )
    full_responses = join_fgn_ownership(full_responses, ultfoc_mapper)

    # Load ITL mapper
    itl_mapper = stage_hlp.load_validate_mapper(
        "itl_mapper_path",
        mapping_dict,
        MappingMainLogger,
        network_or_hdfs,
    )

    # Loading cell number coverage
    cellno_df = stage_hlp.load_validate_mapper(
        "cellno_path",
        mapping_dict,
        MappingMainLogger,
        network_or_hdfs,
    )

    sic_pg_num = stage_hlp.load_validate_mapper(
        "sic_pg_num_mapper_path",
        mapping_dict,
        MappingMainLogger,
        network_or_hdfs,
    )
    val.validate_many_to_one(sic_pg_num, "SIC 2007_CODE", "2016 > Form PG")

    # Loading ru_817_list mapper
    if config["years"]["survey_year"] == 2022:
        ref_list_817_mapper = stage_hlp.load_validate_mapper(
            "ref_list_817_mapper_path",
            mapping_dict,
            MappingMainLogger,
            network_or_hdfs,
        )
        # update longform references that should be on the reference list
        full_responses = hlp.update_ref_list(full_responses, ref_list_817_mapper)

    # Carry out product group conversion
    # Impute missing product group responses in q201 from SIC, then copy this to a new
    # column, pg_numeric. Finally, convert column 201 to alpha-numeric PG
    full_responses = run_pg_conversion(full_responses, pg_num_alpha, sic_pg_num)
    # if ni_full_responses is not None:
    #     ni_full_responses = run_pg_conversion(
    #         ni_full_responses, pg_num_alpha, sic_pg_num
    #     )

    # full_responses = join_cellno_mapper(full_responses, cellno_df

    # output QA files
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    mapping_path = config[f"{NETWORK_OR_HDFS}_paths"]["mapping_path"] # Changed

    if config["global"]["output_mapping_qa"]:
        ImputationMainLogger.info("Outputting Imputation files.")
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        full_responses_filename = f"{survey_year}_full_responses_qa_{tdate}_v{run_id}.csv"
        
        # create trimming qa dataframe with required columns from schema
        schema_path = config["schema_paths"]["long_form_schema"] # Changed
        schema_dict = load_schema(schema_path)
        mapping_qa_output = create_output_df(full_responses, schema_dict) # Changed

        # if backdata is not None:
        write_csv(f"{mapping_path}/imputation_qa/{full_responses_filename}", mapping_qa_output) # Changed
        # if config["global"]["load_backdata"]:
        #     write_csv(f"{mapping_path}/imputation_qa/{links_filename}", links_df)
    ImputationMainLogger.info("Finished Imputation calculation.")

    # return mapped_df
    return (full_responses, ni_full_responses, itl_mapper, cellno_df)
