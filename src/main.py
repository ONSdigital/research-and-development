from src.utils.configuration import ConfigReader
import logging
import time
from datetime import datetime, timedelta
import sys


logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
# set up different logs for different modules. (i.e. data cleaning, data visualisation etc.)
file_handler = logging.FileHandler('src/utils/mylog.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)


logger.addHandler(file_handler)

#%%

def add_numbers(a, b):
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
import src.utils.crime_logger_mod
#from src.utils.crime_logger_mod import logger_creator, time_logger_wrap, exception_wrap
import configparser
config= configparser.ConfigParser()
config.read(r'src/utils/testconfig.ini')
global_config = config['global']

logger_creator(global_config)

LOGGER = logging.getLogger(__name__)
if logger.hasHandlers():
    logger.handlers = []

@time_logger_wrap
@exception_wrap
def add(a,b):
    c = a + b    
    return c

add(1,6)
