"""The main pipeline"""


from utils import runlog
import pandas as pd
import numpy as np 
import logging
from _version import __version__ as version
from utils.helpers import Config_settings

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# create a file handler
handler = logging.FileHandler('logs/pipeline.log')

LOGGER.info("Starting pipeline")

# Get the config settings using the Config_settings class
conf_obj = Config_settings()
config = conf_obj.get_config_settings()

def dummy_function(seed=42):
    np.random.seed(seed=seed)
    df = pd.DataFrame(np.random.randint(0, 100,size=(100, 4)), columns=list('ABCD'))
    dummy_log = LOGGER.info("Dummy dataframe has been created")
    return df

def manipulate_df(df):
    df = df * 2
    LOGGER.info("Dummy dataframe has been manipulated")
    return df

# create a runlog instance
runlog_obj = runlog.RunLog(config, version)

# insert the config settings into the runlog
runlog_obj.get_config_settings(config)

if __name__ == "__main__":
    
    # Do something
    df = dummy_function()
    df = manipulate_df(df)
    print(df)


# insert the time taken into the runlog
fake_time_taken = 10
runlog_obj.record_time_taken(time_taken=fake_time_taken)

# insert the logs into the runlog
