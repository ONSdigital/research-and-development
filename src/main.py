import logging
from src.utils.crime_logger_mod import logger_creator, time_logger_wrap, exception_wrap
import configparser
config= configparser.ConfigParser()
config.read(r'src/utils/testconfig.ini')
global_config = config['global']

logger_creator(global_config)

logger = logging.getLogger(__name__)

#if logger.hasHandlers():
    #logger.handlers = []

@time_logger_wrap
@exception_wrap
def add(a,b):
    c = a + b 
    return c

add(1,2)
