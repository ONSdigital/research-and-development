"""The main pipeline"""
from src.utils.configuration import ConfigReader
import logging
import time
from datetime import datetime, timedelta
import sys

from src.utils.testfunctions import add

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
# set up different logs for different modules. (i.e. data cleaning, data visualisation etc.)
file_handler = logging.FileHandler('src/utils/mylog.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)


logger.addHandler(file_handler)

#%%

def run_pipeline():
    """Run the pipeline"""
    return add(1, 2)

def add_numbers(a, b):
    return a + B

from utils import runlog
import pandas as pd
import numpy as np 


def dummy_function(seed=42):
    np.random.seed(seed=seed)
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    return df

def manipulate_df(df):
    df = df * 2
    return df

if __name__ == "__main__":
    # Do something
    df = dummy_function()
    df = manipulate_df(df)
    print(df)
    logger.info("Starting function call")

    total_time = round(time.time(), 0)
    timestamp = str(datetime.now())
    config = ConfigReader('src/utils/testconfig.ini')
    global_config = config.config_setup("global")
    
    try:
        c = a + b
        logger.info("Finished function call")
        return c
    except TypeError:
        print("error")
        logger.error("Error with input.")
        return
        
#%%
import logging
from src.utils.crime_logger_mod import logger_creator, time_logger_wrap
import configparser
config= configparser.ConfigParser()
config.read(r'src/utils/testconfig.ini')
global_config = config['global']

logger_creator(global_config)
LOGGER = logging.getLogger(__name__)
@time_logger_wrap
def add(a,b):
    c = a + b    
    return c

add(1,2)
