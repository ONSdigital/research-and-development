"""The main pipeline"""


from src.utils import runlog
import pandas as pd
import numpy as np
from src._version import __version__ as version
from src.utils.helpers import Config_settings
import logging
import logging.config
from src.utils.wrappers import time_logger_wrap
from src.utils.testfunctions import add, divbyzero
import time

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# create a file handler
handler = logging.FileHandler("../logs/pipeline.log")

LOGGER.info("Starting pipeline")

# Get the config settings using the Config_settings class
conf_obj = Config_settings()
config = conf_obj.get_config_settings()


def dummy_function(seed=42):
    np.random.seed(seed=seed)
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    LOGGER.info("Dummy dataframe has been created")
    return df


def manipulate_df(df):
    df = df * 2
    LOGGER.info("Dummy dataframe has been manipulated")
    return df


# create a runlog instance
runlog_obj = runlog.RunLog(config, version)

# insert the config settings into the runlog
runlog_obj.get_config_settings(config)


@time_logger_wrap
def pipeline():
    # Run the pipeline
    df = dummy_function()
    df = manipulate_df(df)
    print(df)
    for x_val, y_val in zip(x_vals, y_vals):
        x, y = x_val, y_val
        add(x, y)
    time.sleep(2)
    divbyzero(12)


if __name__ == "__main__":

    pipeline()
    time_taken = pipeline()[1]
    runlog_obj.record_time_taken(time_taken=time_taken)


# insert the logs into the runlog


logger = logging.getLogger(__name__)

x_vals = [22, 33, 61, 42, 10]
y_vals = [51, 74, 12, 0, 15]


@time_logger_wrap
def test_pipeline():
    logger.info("Started Pipeline")
    for x_val, y_val in zip(x_vals, y_vals):
        x, y = x_val, y_val
        add(x, y)
    time.sleep(2)
    logger.info("Finished Pipeline")


time_taken = test_pipeline()[1]

# Testing dataframe changes

arr_random = np.random.randint(low=2, high=10, size=(2, 3))
data = pd.DataFrame(arr_random, columns=["A", "B", "C"], index=["a", "b"])
