"""The main pipeline"""

from src.utils import runlog
from src._version import __version__ as version
from src.utils.helpers import Config_settings
from src.utils.wrappers import logger_creator
from src.utils.testfunctions import Manipulate_data
import time
import logging


MainLogger = logging.getLogger(__name__)
MainLogger.setLevel(logging.INFO)


def run_pipeline(start):
    """The main pipeline.

    Args:
        start (float): The time when the pipeline is launched
        generated from the time module using time.time()
    """

    conf_obj = Config_settings()
    config = conf_obj.config_dict
    global_config = config["global"]

    runlog_obj = runlog.RunLog(config, version)

    logger = logger_creator(global_config)
    MainLogger.info("Launching Pipeline .......................")
    logger.info("Collecting logging parameters ..........")
    Manipulate_data()

    # Data Ingest
    # Load SPP data from DAP
    snapshot_path = config["paths"]["snapshot_path"]
    snapdata = hdfs_load_json(snapshot_path)
    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)
    # Data Transmutation
    full_responses = processing.full_responses(contributors_df, responses_df)
    print(full_responses.sample(5))
    logger.info(
        "The response rate is %.3%",
        processing.response_rate(contributors_df, responses_df),
    )

    # Data validation
    validation.check_file_exists(snapshot_path)

    # Check the postcode column
    validation.validate_post_col(contributors_df, masterlist_path)

    # Outlier detection

    # Data cleaning

    # Data processing: Imputation

    # Data processing: Estimation

    # Data processing: Regional Apportionment

    # Data processing: Aggregation

    # Data display: Visualisations

    # Data output: Disclosure Control

    # Data output: File Outputs

    MainLogger.info("Finshing Pipeline .......................")

    runlog_obj.retrieve_pipeline_logs()

    run_time = round(time.time() - start, 5)
    runlog_obj._record_time_taken(run_time)

    runlog_obj.retrieve_configs()
    runlog_obj._create_runlog_dicts()
    runlog_obj._create_runlog_dfs()
    runlog_obj.create_runlog_files()
    runlog_obj._write_runlog()
