"""The main pipeline"""

# Core Python modules
from datetime import datetime
import logging
import pandas as pd

# Our local modules
from src.utils import runlog
from src._version import __version__ as version
from src.utils.config import config_setup, file_validation
from src.utils.helpers import save_config_files
from src.utils.logger import logger_creator
from src.staging.staging_main import run_staging
from src.utils.helpers import validate_updated_postcodes
from src.freezing.freezing_main import run_freezing
from src.northern_ireland.ni_main import run_ni
from src.construction.construction_main import run_construction
from src.mapping.mapping_main import run_mapping
from src.imputation.imputation_main import run_imputation  # noqa
from src.outlier_detection.outlier_main import run_outliers
from src.estimation.estimation_main import run_estimation
from src.site_apportionment.site_apportionment_main import run_site_apportionment
from src.outputs.outputs_main import run_outputs

MainLogger = logging.getLogger(__name__)


def run_pipeline(user_config_path, dev_config_path):  # noqa C901
    """The main pipeline.

    Args:
        start (float): The time when the pipeline is launched
        generated from the time module using time.time()
        config_path (string): The path to the config file to be
        used.
    """
    # Load, validate and merge the user and developer configs
    config = config_setup(user_config_path, dev_config_path)

    # Set up the logger
    logger = logger_creator(config)

    # validate the filenames and survey type in the config
    config = file_validation(config)

    # Check the environment switch
    platform = config["dev_global"]["platform"]

    if platform == "s3":
        # create singletion boto3 client object & pass in bucket string
        from src.utils.singleton_boto import SingletonBoto

        boto3_client = SingletonBoto.get_client(config)  # noqa
        from src.utils import s3_mods as mods

    elif platform == "network":
        # If the platform is "network" there is no need for a client.
        # Adding a client = None for consistency.
        # config["client"] = None
        from src.utils import local_file_mods as mods
    elif platform == "hdfs":
        # config["client"] = None
        from src.utils import hdfs_mods as mods
    else:
        MainLogger.error(f"The selected platform {platform} is wrong")
        raise ImportError(f"Cannot import {platform}_mods")

    # Set up the run logger
    runlog_obj = runlog.RunLog(
        config,
        version,
        mods.rd_file_exists,
        mods.rd_mkdir,
        mods.rd_read_csv,
        mods.rd_write_csv,
    )
    runlog_obj.create_runlog_files()
    runlog_obj.write_config_log()
    runlog_obj.write_mainlog()

    run_id = runlog_obj.run_id

    # update config to include run_id and tdate for when files are written
    run_id = runlog_obj.run_id
    tdate = datetime.now().strftime("%y-%m-%d")
    config.update({"filename_items": {"run_id": run_id, "tdate": tdate}})

    # write the config yaml files to old_configs folder
    save_config_files(config, mods.rd_copy_file)

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
        manual_outliers,
        postcode_mapper,
        backdata,
        pg_detailed,
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
    )

    # Freezing module
    MainLogger.info("Starting Freezing module...")
    full_responses = run_freezing(
        full_responses,
        config,
        mods.rd_write_csv,
        mods.rd_read_csv,
        mods.rd_file_exists,
    )
    MainLogger.success("Finished Freezing module...")

    run_id = runlog_obj.run_id

    if config["global"]["load_updated_snapshot_for_comparison"]:
        MainLogger.success(
            "Updated SPP snapshot & frozen data comparison done.\n"
            f"Finishing Pipeline run id {run_id}........."
        )

        runlog_obj.write_runlog()
        runlog_obj.mark_mainlog_passed()

        return runlog_obj.time_taken

    if config["global"]["run_updates_and_freeze"]:
        MainLogger.success(
            "New frozen csv file generated.\n"
            f"Finishing Pipeline run id {run_id}........."
        )

        runlog_obj.write_runlog()
        runlog_obj.mark_mainlog_passed()

        return runlog_obj.time_taken

    MainLogger.success("Finished Data Ingest.")

    # Northern Ireland staging and construction
    load_ni_data = config["global"]["load_ni_data"]
    if load_ni_data:
        MainLogger.info("Starting NI module...")
        ni_df = run_ni(
            config,
            mods.rd_file_exists,
            mods.rd_read_csv,
            mods.rd_write_csv,
        )
        MainLogger.success("Finished NI Data Ingest.")
    else:
        # If NI data is not loaded, set ni_df to an empty dataframe
        MainLogger.info("NI data not loaded.")
        ni_df = pd.DataFrame()

    # Construction module
    MainLogger.info("Starting Construction module...")
    run_all_data_construction = config["global"]["run_all_data_construction"]
    if run_all_data_construction:
        full_responses = run_construction(
            full_responses,
            config,
            mods.rd_file_exists,
            mods.rd_read_csv,
            is_run_all_data_construction=True,
        )
    else:
        MainLogger.info("All data construction is not enabled")
    MainLogger.success("Finished Construction module...")

    # Mapping module
    MainLogger.info("Starting Mapping...")
    (mapped_df, ni_full_responses, itl_mapper) = run_mapping(
        full_responses,
        ni_df,
        postcode_mapper,
        config,
        mods.rd_read_csv,
        mods.rd_write_csv,
        mods.rd_file_exists,
    )
    MainLogger.success("Finished Mapping...")

    # Imputation module
    MainLogger.info("Starting Imputation...")
    imputed_df = run_imputation(
        mapped_df,
        manual_trimming_df,
        backdata,
        config,
        mods.rd_write_csv,
    )
    MainLogger.success("Finished Imputation...")

    # Perform postcode construction now imputation is complete
    run_postcode_construction = config["global"]["run_postcode_construction"]
    if run_postcode_construction:
        imputed_df = run_construction(
            imputed_df,
            config,
            mods.rd_file_exists,
            mods.rd_read_csv,
            is_run_postcode_construction=True,
        )

    imputed_df = validate_updated_postcodes(
        imputed_df,
        postcode_mapper,
        itl_mapper,
        config,
    )

    if config["survey"]["survey_type"] == "BERD":
        # Outlier detection module
        MainLogger.info("Starting Outlier Detection...")
        outliered_responses_df = run_outliers(
            imputed_df, manual_outliers, config, mods.rd_write_csv
        )
        MainLogger.success("Finished Outlier module.")

        # Estimation module
        MainLogger.info("Starting Estimation...")
        estimated_responses_df = run_estimation(
            outliered_responses_df, config, mods.rd_write_csv
        )
        MainLogger.success("Finished Estimation module.")

    elif config["survey"]["survey_type"] == "PNP":
        MainLogger.info("PNP is set so skipping modules Outliering and Estimation.")
        estimated_responses_df = imputed_df

    # Data processing: Apportionment to sites
    apportioned_responses_df, intram_tot_dict = run_site_apportionment(
        estimated_responses_df,
        config,
        mods.rd_write_csv,
    )

    MainLogger.success("Finished Site Apportionment module.")

    MainLogger.info("Starting Outputs...")

    run_outputs(
        apportioned_responses_df,
        ni_full_responses,
        config,
        intram_tot_dict,
        mods.rd_write_csv,
        pg_detailed,
        sic_division_detailed,
    )

    MainLogger.success(f"Finishing Pipeline run id {run_id}.........")

    runlog_obj.write_runlog()
    runlog_obj.mark_mainlog_passed()

    return runlog_obj.time_taken
