"""The main pipeline"""
# Core Python modules
import logging
import pandas as pd

# Our local modules
from src.utils import runlog
from src._version import __version__ as version
from src.utils.config import config_setup
from src.utils.wrappers import logger_creator
from src.staging.staging_main import run_staging
from src.northern_ireland.ni_main import run_ni
from src.construction.construction_main import run_construction
from src.mapping.mapping_main import run_mapping
from src.imputation.imputation_main import run_imputation  # noqa
from src.outlier_detection.outlier_main import run_outliers
from src.estimation.estimation_main import run_estimation
from src.site_apportionment.site_apportionment_main import run_site_apportionment
from src.outputs.outputs_main import run_outputs


MainLogger = logging.getLogger(__name__)


def run_pipeline(user_config_path, dev_config_path):
    """The main pipeline.

    Args:
        start (float): The time when the pipeline is launched
        generated from the time module using time.time()
        config_path (string): The path to the config file to be
        used.
    """
    # Load, validate and merge the user and developer configs
    config = config_setup(user_config_path, dev_config_path)

    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    if network_or_hdfs == "network":
        from src.utils import local_file_mods as mods

    elif network_or_hdfs == "hdfs":
        from src.utils import hdfs_mods as mods

    else:
        MainLogger.error("The network_or_hdfs configuration is wrong")
        raise ImportError

    # Set up the run logger
    global_config = config["global"]
    runlog_obj = runlog.RunLog(
        config,
        version,
        mods.rd_open,
        mods.rd_file_exists,
        mods.rd_mkdir,
        mods.rd_read_csv,
        mods.rd_write_csv,
    )
    runlog_obj.create_runlog_files()
    runlog_obj.write_config_log()
    runlog_obj.write_mainlog()
    logger = logger_creator(global_config)
    run_id = runlog_obj.run_id
    MainLogger.info(f"Reading user config from {user_config_path}.")
    MainLogger.info(f"Reading developer config from {dev_config_path}.")

    MainLogger.info("Launching Pipeline .......................")
    logger.info("Collecting logging parameters ..........")

    # Data Ingest
    MainLogger.info("Starting Data Ingest...")

    # Staging and validatation and Data Transmutation
    MainLogger.info("Starting Staging and Validation...")

    (
        full_responses,
        secondary_full_responses,  # may be needed later for freezing
        manual_outliers,
        postcode_mapper,
        backdata,
        pg_detailed,
        itl1_detailed,
        civil_defence_detailed,
        sic_division_detailed,
        manual_trimming_df,
    ) = run_staging(
        config,
        mods.rd_file_exists,
        mods.rd_load_json,
        mods.rd_read_csv,
        mods.rd_write_csv,
        mods.rd_read_feather,
        mods.rd_write_feather,
        mods.rd_isfile,
        run_id,
    )
    MainLogger.info("Finished Data Ingest.")

    # Northern Ireland staging and construction
    load_ni_data = config["global"]["load_ni_data"]
    if load_ni_data:
        MainLogger.info("Starting NI module...")
        ni_df = run_ni(
            config, mods.rd_file_exists, mods.rd_read_csv, mods.rd_write_csv, run_id
        )
        MainLogger.info("Finished NI Data Ingest.")
    else:
        # If NI data is not loaded, set ni_df to an empty dataframe
        MainLogger.info("NI data not loaded.")
        ni_df = pd.DataFrame()

    # Construction module
    MainLogger.info("Starting Construction...")
    full_responses = run_construction(
        full_responses, config, mods.rd_file_exists, mods.rd_read_csv
    )
    MainLogger.info("Finished Construction...")

    # Mapping module
    MainLogger.info("Starting Mapping...")
    (mapped_df, ni_full_responses) = run_mapping(
        full_responses,
        ni_df,
        postcode_mapper,
        config,
        mods.rd_write_csv,
        run_id,
    )
    MainLogger.info("Finished Mapping...")

    # Imputation module
    MainLogger.info("Starting Imputation...")
    imputed_df = run_imputation(
        mapped_df,
        manual_trimming_df,
        backdata,
        config,
        mods.rd_write_csv,
        run_id,
    )
    MainLogger.info("Finished  Imputation...")

    # Outlier detection module
    MainLogger.info("Starting Outlier Detection...")
    outliered_responses_df = run_outliers(
        imputed_df, manual_outliers, config, mods.rd_write_csv, run_id
    )
    MainLogger.info("Finished Outlier module.")

    # Estimation module
    MainLogger.info("Starting Estimation...")
    estimated_responses_df, weighted_responses_df = run_estimation(
        outliered_responses_df, config, mods.rd_write_csv, run_id
    )
    MainLogger.info("Finished Estimation module.")

    # Data processing: Apportionment to sites
    estimated_responses_df = run_site_apportionment(
        estimated_responses_df,
        config,
        mods.rd_write_csv,
        run_id,
        "estimated",
    )
    weighted_responses_df = run_site_apportionment(
        weighted_responses_df,
        config,
        mods.rd_write_csv,
        run_id,
        "weighted",
    )
    MainLogger.info("Finished Site Apportionment module.")

    MainLogger.info("Starting Outputs...")

    run_outputs(
        estimated_responses_df,
        weighted_responses_df,
        ni_full_responses,
        config,
        mods.rd_write_csv,
        run_id,
        postcode_mapper,
        pg_detailed,
        itl1_detailed,
        civil_defence_detailed,
        sic_division_detailed,
    )

    MainLogger.info("Finished All Output modules.")

    MainLogger.info("Finishing Pipeline .......................")

    runlog_obj.write_runlog()
    runlog_obj.mark_mainlog_passed()

    return runlog_obj.time_taken
