import time
from src.utils.wrappers import time_logger_wrap, exception_wrap
import numpy as np
import pandas as pd


@exception_wrap
def divbyzero(num):
    """Define a testing function that will throw an exception."""
    ans = num / 0
    return ans


@exception_wrap
def this_definitely_works(num):
    """Define a testing function that will not throw an exception."""
    ans = num**num
    return ans


@time_logger_wrap
def takes_a_while(num):
    """Define a testing function that takes some time to run."""
    ans = 0
    for _ in range(num):
        ans += (num**2) ** 2
        time.sleep(5.5)
    return ans


@time_logger_wrap
@exception_wrap
def addition(a, b):
    """Testing multiple wrappers for one function"""
    c = a + b
    time.sleep(0.5)
    return c


@time_logger_wrap
@exception_wrap
def create_dummy_df(seed=42):
    np.random.seed(seed=seed)
    df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD"))
    return df


@time_logger_wrap
@exception_wrap
def manipulate_df(df):
    df = df * 2
    return df


def add(a: int, b: int):
    """Testing multiple wrappers for one function"""
    # Raise error if a or b is not an integer
    if not isinstance(a, int) or not isinstance(b, int):
        raise TypeError("a and b must be integers")
    return a + b
