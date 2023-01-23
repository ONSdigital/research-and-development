import logging
from src.utils.loggingfuncs import logger_creator, time_logger_wrap, exception_wrap
import configparser
import logging.config

config = configparser.ConfigParser()
config.read(r"src/utils/testconfig.ini")
global_config = config["global"]

logger_creator(global_config)

logger = logging.getLogger(__name__)


# SEPERATOR FOR NEW METHOD OF LOGGING
logging.config.fileConfig("src/utils/testconfig.ini", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

# if logger.hasHandlers():
# logger.handlers = []

__name__


@time_logger_wrap
@exception_wrap
def add(a, b):
    c = a + b
    return c


add(1, 2)
