import logging
import logging.config
from src.utils.wrappers import time_logger_wrap
from src.utils.testfunctions import add
import time

logging.config.fileConfig("src/utils/testconfig.ini", disable_existing_loggers=False)
logger = logging.getLogger("main")

x_vals = [2, 3, 6, 4, 10]
y_vals = [5, 7, 12, 0, 1]


@time_logger_wrap
def test_pipeline():
    for x_val, y_val in zip(x_vals, y_vals):
        x, y = x_val, y_val
        add(x, y)
    time.sleep(7)


time_taken = test_pipeline()[1]
