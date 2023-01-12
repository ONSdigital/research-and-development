"""The main pipeline"""

from src.utils.testfunctions import add


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
