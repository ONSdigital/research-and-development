"""The main pipeline"""


from utils import runlog
import pandas as pd
import numpy as np 
import logging
from _version import __version__ as version


config = {
    "run_id": "test",
    "version": version,
    "log_path": "data/log",
    "log_file": "runlog.hdf5",
    "log_table": "runlog",
    "log_db": "test",
    "log_format": "csv",
    "log_mode": "overwrite",
}

RUN_LOGGER = runlog.RunLog(config, version)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

LOGGER.info("Starting pipeline")


def dummy_function(seed=42):
    np.random.seed(seed=seed)
    df = pd.DataFrame(np.random.randint(0, 100,size=(100, 4)), columns=list('ABCD'))
    dummy_log = LOGGER.info("Dummy dataframe has been created")
    return df

def manipulate_df(df):
    df = df * 2
    LOGGER.info("Dummy dataframe has been manipulated")
    return df

if __name__ == "__main__":
    # Do something
    df = dummy_function()
    df = manipulate_df(df)
    print(df)
