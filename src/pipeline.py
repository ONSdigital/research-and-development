"""The main pipeline"""
from src.utils import runlog
from src._version import __version__ as version

from src.utils.helpers import Config_settings
from src.utils.wrappers import logger_creator
from src.data_ingest import spp_parser
from src.data_processing import spp_snapshot_processing as processing
from src.utils.hdfs_mods import hdfs_load_json, check_file_exists
from src.data_validation import validation as val
from src.imputation.imputation import run_imputation

import time

import logging


MainLogger = logging.getLogger(__name__)


# load config
conf_obj = Config_settings()
config = conf_obj.config_dict


def run_pipeline(start):
    """The main pipeline.

    Args:
        start (float): The time when the pipeline is launched
        generated from the time module using time.time()
    """
    # Set up the run logger
    global_config = config["global"]
    runlog_obj = runlog.RunLog(config, version)

    logger = logger_creator(global_config)
    MainLogger.info("Launching Pipeline .......................")
    logger.info("Collecting logging parameters ..........")
    # Data Ingest
    MainLogger.info("Starting Data Ingest...")
    # Load SPP data from DAP
    snapshot_path = config["paths"]["snapshot_path"]

    # Check data file exists
    check_file_exists(snapshot_path)

    snapdata = hdfs_load_json(snapshot_path)
    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)
    MainLogger.info("Finished Data Ingest...")

    # Data Transmutation
    MainLogger.info("Starting Data Transmutation...")
    full_responses = processing.full_responses(contributors_df, responses_df)
    print(full_responses.sample(5))
    processing.response_rate(contributors_df, responses_df)
    MainLogger.info("Finished Data Transmutation...")

    # Data validation
    val.check_data_shape(full_responses)

    # Check the postcode column
    masterlist_path = config["paths"]["masterlist_path"]
    val.validate_post_col(contributors_df, masterlist_path)

    forward_df, backwards_df = run_imputation(
        ["201", "202"],
        "202012",
        "202009",
    )

    # Outlier detection

    # Data cleaning

    # Data processing: Imputation

    # Data processing: Estimation

    # Data processing: Regional Apportionment

    # Data processing: Aggregation

    # Data display: Visualisations

    # Data output: Disclosure Control

    # Data output: File Outputs

    MainLogger.info("Finishing Pipeline .......................")

    runlog_obj.retrieve_pipeline_logs()

    run_time = round(time.time() - start, 5)
    runlog_obj._record_time_taken(run_time)

    runlog_obj.retrieve_configs()
    runlog_obj._create_runlog_dicts()
    runlog_obj._create_runlog_dfs()
    runlog_obj.create_runlog_files()
    runlog_obj._write_runlog()
