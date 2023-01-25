import logging
import logging.config
from src.utils.wrappers import time_logger_wrap
from src.utils.testfunctions import add
import time
import numpy as np
import pandas as pd

logging.config.fileConfig("src/utils/testconfig.ini", disable_existing_loggers=False)
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
