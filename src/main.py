"""The main pipeline"""

from utils import runlog
from _version import __version__ as version
from utils.helpers import Config_settings
from utils.wrappers import time_logger_wrap, logger_creator
from utils.testfunctions import create_dummy_df, manipulate_df
import configparser


config = configparser.ConfigParser()
conf_file = config.read("src/utils/testconfig.ini")
global_config = config["global"]

# Get the config settings using the Config_settings class
conf_obj = Config_settings()
config = conf_obj.config_dict

# create a runlog instance
runlog_obj = runlog.RunLog(config, version)
# insert the config settings into the runlog
# runlog_obj.retrieve_pipeline_logs(logs)

x_vals = [22, 33, 61, 42, 10]
y_vals = [51, 74, 12, 0, 15]


@time_logger_wrap
def pipeline():
    logger = logger_creator(global_config, runlog_obj.run_id)  # noqa,
    # Run the pipeline
    df = create_dummy_df()
    df = manipulate_df(df)
    print(df)


if __name__ == "__main__":

    # pipeline()
    time_taken = pipeline()[1]
    runlog_obj.record_time_taken(time_taken=time_taken)
    # runlog_obj.retrieve_pipeline_logs(logs)
