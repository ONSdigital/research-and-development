"""The main pipeline"""
# Core Python modules
import time
import logging

# Our local modules
from src.utils import runlog
from src._version import __version__ as version
from src.utils.helpers import Config_settings
from src.utils.wrappers import logger_creator
from src.staging.staging_main import run_staging
from src.northern_ireland.ni_main import run_ni
from src.construction.construction import run_construction
from src.imputation.imputation_main import run_imputation  # noqa
from src.outlier_detection.outlier_main import run_outliers
from src.estimation.estimation_main import run_estimation
from src.site_apportionment.site_apportionment_main import run_site_apportionment
from src.outputs.outputs_main import run_outputs

MainLogger = logging.getLogger(__name__)


def run_pipeline(start, config_path):
    """The main pipeline.

    Args:
        start (float): The time when the pipeline is launched
        generated from the time module using time.time()
        config_path (string): The path to the config file to be
        used.
    """
    # load config
    conf_obj = Config_settings(config_path)
    config = conf_obj.config_dict

    # import yaml
    # with open(config_path, "r") as file:
    #     config = yaml.safe_load(file)

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    if network_or_hdfs == "network":

        from src.utils.local_file_mods import load_local_json as load_json
        from src.utils.local_file_mods import local_file_exists as check_file_exists
        from src.utils.local_file_mods import local_mkdir as mkdir
        from src.utils.local_file_mods import local_open as open_file
        from src.utils.local_file_mods import read_local_csv as read_csv
        from src.utils.local_file_mods import write_local_csv as write_csv
        from src.utils.local_file_mods import local_isfile as isfile

        # from src.utils.local_file_mods import local_file_exists as file_exists
        from src.utils.local_file_mods import local_write_feather as write_feather
        from src.utils.local_file_mods import local_read_feather as read_feather
    elif network_or_hdfs == "hdfs":

        from src.utils.hdfs_mods import hdfs_load_json as load_json
        from src.utils.hdfs_mods import hdfs_file_exists as check_file_exists
        from src.utils.hdfs_mods import hdfs_mkdir as mkdir
        from src.utils.hdfs_mods import hdfs_open as open_file
        from src.utils.hdfs_mods import read_hdfs_csv as read_csv
        from src.utils.hdfs_mods import write_hdfs_csv as write_csv
        from src.utils.hdfs_mods import hdfs_isfile as isfile

        # from src.utils.hdfs_mods import hdfs_file_exists as file_exists
        from src.utils.hdfs_mods import hdfs_write_feather as write_feather
        from src.utils.hdfs_mods import hdfs_read_feather as read_feather
    else:
        MainLogger.error("The network_or_hdfs configuration is wrong")
        raise ImportError

    # Set up the run logger
    global_config = config["global"]
    runlog_obj = runlog.RunLog(
        config, version, open_file, check_file_exists, mkdir, read_csv, write_csv
    )

    logger = logger_creator(global_config)
    run_id = runlog_obj.run_id
    MainLogger.info(f"Reading config from {config_path}.")

    MainLogger.info("Launching Pipeline .......................")
    logger.info("Collecting logging parameters ..........")

    # Data Ingest
    MainLogger.info("Starting Data Ingest...")

    # Staging and validatation and Data Transmutation
    MainLogger.info("Starting Staging and Validation...")

    (
        full_responses,
        secondary_full_responses,  #  may be needed later for freezing
        manual_outliers,
        ultfoc_mapper,
        itl_mapper,
        cellno_df,
        postcode_mapper,
        pg_num_alpha,
        sic_pg_alpha,
        sic_pg_num,
        backdata,
        pg_detailed,
        itl1_detailed,
        reference_list,
        civil_defence_detailed,
        sic_division_detailed,
        manual_trimming_df,
    ) = run_staging(
        config,
        check_file_exists,
        load_json,
        read_csv,
        write_csv,
        read_feather,
        write_feather,
        isfile,
        run_id,
    )
    MainLogger.info("Finished Data Ingest.")

    # Northern Ireland staging and construction
    MainLogger.info("Starting NI module...")
    ni_df = run_ni(config, check_file_exists, read_csv, write_csv, run_id)
    MainLogger.info("Finished NI Data Ingest.")

    # Construction module
    MainLogger.info("Starting Construction...")
    full_responses = run_construction(
        full_responses, config, check_file_exists, read_csv
    )
    MainLogger.info("Finished Construction...")

    # Imputation module
    MainLogger.info("Starting Imputation...")
    imputed_df = run_imputation(
        full_responses,
        manual_trimming_df,
        pg_num_alpha,
        sic_pg_num,
        backdata,
        config,
        write_csv,
        run_id,
    )
    MainLogger.info("Finished  Imputation...")

    # Outlier detection module
    MainLogger.info("Starting Outlier Detection...")
    outliered_responses_df = run_outliers(
        imputed_df, manual_outliers, config, write_csv, run_id
    )
    MainLogger.info("Finished Outlier module.")

    # Estimation module
    MainLogger.info("Starting Estimation...")
    estimated_responses_df, weighted_responses_df = run_estimation(
        outliered_responses_df, cellno_df, config, write_csv, run_id
    )
    MainLogger.info("Finished Estimation module.")

    # Data processing: Apportionment to sites
    estimated_responses_df = run_site_apportionment(
        estimated_responses_df, config, write_csv, run_id, "estimated", output_file=True
    )
    weighted_responses_df = run_site_apportionment(
        weighted_responses_df, config, write_csv, run_id, "weighted", output_file=True
    )
    MainLogger.info("Finished Site Apportionment module.")

    # Data processing: Regional Apportionment

    # Data processing: Aggregation

    # Data display: Visualisations

    # Data output: Disclosure Control

    # Data output: File Outputs
    MainLogger.info("Starting Outputs...")

    # Run short frozen form output
    run_outputs(
        estimated_responses_df,
        weighted_responses_df,
        ni_df,
        config,
        write_csv,
        run_id,
        ultfoc_mapper,
        postcode_mapper,
        itl_mapper,
        sic_pg_num,
        pg_detailed,
        itl1_detailed,
        civil_defence_detailed,
        sic_division_detailed,
        pg_num_alpha,
        sic_pg_num,
    )

    MainLogger.info("Finished All Output modules.")

    MainLogger.info("Finishing Pipeline .......................")

    runlog_obj.retrieve_pipeline_logs()

    run_time = round(time.time() - start, 5)
    runlog_obj._record_time_taken(run_time)

    runlog_obj.retrieve_configs()
    runlog_obj._create_runlog_dicts()
    runlog_obj._create_runlog_dfs()
    runlog_obj.create_runlog_files()
    runlog_obj._write_runlog()

    return run_time
