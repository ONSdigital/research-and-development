"""The main pipeline"""
# Core Python modules
import logging

# Our local modules
from src.utils import runlog
from src._version import __version__ as version
from src.utils.config import validate_config, merge_configs
from src.utils.wrappers import logger_creator
from src.utils.local_file_mods import safeload_yaml
from src.staging.staging_main import run_staging
from src.northern_ireland.ni_main import run_ni
from src.construction.construction import run_construction
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
    # load config
    user_config = safeload_yaml(user_config_path)
    dev_config = safeload_yaml(dev_config_path)
    # validate config
    validate_config(user_config)
    validate_config(dev_config)
    # drop validation keys
    user_config.pop("config_validation", None)
    dev_config.pop("config_validation", None)
    # combine configs
    config = merge_configs(user_config, dev_config)
    del user_config
    del dev_config

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
    MainLogger.info("Starting NI module...")
    ni_df = run_ni(
        config, mods.rd_file_exists, mods.rd_read_csv, mods.rd_write_csv, run_id
    )
    MainLogger.info("Finished NI Data Ingest.")

    # Construction module
    MainLogger.info("Starting Construction...")
    full_responses = run_construction(
        full_responses, config, mods.rd_file_exists, mods.rd_read_csv
    )
    MainLogger.info("Finished Construction...")

    # Mapping module
    MainLogger.info("Starting Mapping...")
    (mapped_df, ni_full_responses, ultfoc_mapper, itl_mapper, cellno_df,) = run_mapping(
        full_responses,
        ni_df,
        config,
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
        outliered_responses_df, cellno_df, config, mods.rd_write_csv, run_id
    )
    MainLogger.info("Finished Estimation module.")

    # Data processing: Apportionment to sites
    estimated_responses_df = run_site_apportionment(
        estimated_responses_df,
        config,
        mods.rd_write_csv,
        run_id,
        "estimated",
        output_file=True,
    )
    weighted_responses_df = run_site_apportionment(
        weighted_responses_df,
        config,
        mods.rd_write_csv,
        run_id,
        "weighted",
        output_file=True,
    )
    MainLogger.info("Finished Site Apportionment module.")

    # Data processing: Regional Apportionment

    # Data processing: Aggregation

    # Data display: Visualisations

    # Data output: Disclosure Control

    # Data output: File Outputs
    MainLogger.info("Starting Outputs...")

    run_outputs(
        estimated_responses_df,
        weighted_responses_df,
        ni_df,
        config,
        mods.rd_write_csv,
        run_id,
        ultfoc_mapper,
        postcode_mapper,
        itl_mapper,
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
