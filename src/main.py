"""The main pipeline"""


from src.utils import runlog
from src._version import __version__ as version
from src.utils.helpers import Config_settings
from src.utils.wrappers import time_logger_wrap, logger_creator
from src.utils.testfunctions import dummy_function, manipulate_df
import configparser


config = configparser.ConfigParser()
conf_file = config.read("src/utils/testconfig.ini")
global_config = config["global"]

# Get the config settings using the Config_settings class
conf_obj = Config_settings()
config = conf_obj.get_config_settings()

# create a runlog instance
runlog_obj = runlog.RunLog(config, version)
# insert the config settings into the runlog
runlog_obj.get_config_settings(config)

x_vals = [22, 33, 61, 42, 10]
y_vals = [51, 74, 12, 0, 15]


@time_logger_wrap
def pipeline():
    logger = logger_creator(global_config)  # noqa
    # Run the pipeline
    df = dummy_function()
    df = manipulate_df(df)
    print(df)


if __name__ == "__main__":

    pipeline()
    time_taken = pipeline()[1]
    runlog_obj.record_time_taken(time_taken=time_taken)
