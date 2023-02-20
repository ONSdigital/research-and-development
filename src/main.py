"""The main pipeline"""

from utils import runlog
from _version import __version__ as version
from utils.helpers import Config_settings
from utils.wrappers import logger_creator
from utils.testfunctions import create_dummy_df, manipulate_df
import time


def run_pipeline(start):

    conf_obj = Config_settings()
    config = conf_obj.config_dict
    global_config = config["global"]

    runlog_obj = runlog.RunLog(config, version)

    logger = logger_creator(global_config, runlog_obj.run_id)
    logger.info("Launching Pipeline .......................")

    df = create_dummy_df()
    df = manipulate_df(df)
    time.sleep(5)
    print(df)
    logger.info("Finshing Pipeline .......................")

    runlog_obj.retrieve_pipeline_logs()

    run_time = round(time.time() - start, 5)
    runlog_obj._record_time_taken(run_time)

    runlog_obj._create_main_dict()
    runlog_obj._create_config_dict()
    runlog_obj._create_logs_dict()

    runlog_obj._create_runlog_dfs()
    runlog_obj.create_runlog_files()
    runlog_obj._write_runlog()
