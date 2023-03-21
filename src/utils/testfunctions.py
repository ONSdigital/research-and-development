import time
from src.utils.wrappers import time_logger_wrap, exception_wrap, df_change_wrap
import numpy as np
import pandas as pd


@exception_wrap
def divbyzero(num: int):
    """Define a testing function that will throw an exception.

    Args:
        num (int): Integer value

    Returns:
        Error: This function returns a zero division error
    """
    ans = num / 0

    return ans


@exception_wrap
def this_definitely_works(num: int):
    """Define a testing function that will not throw an exception.

    Args:
        num (int): Integer value

    Returns:
        int: Returns the squared value of the integer input
    """
    ans = num**num

    return ans


@time_logger_wrap
def takes_a_while(num: int):
    """Define a testing function that takes some time to run.

    Args:
        num (int): Integer value

    Returns:
        int: Integer value after sleep function ends
    """
    ans = 0
    for _ in range(num):
        ans += (num**2) ** 2
        time.sleep(5.5)

    return ans


@time_logger_wrap
@exception_wrap
def addition(a: int, b: int):
    """Testing multiple wrappers for one function

    Args:
        a (int): Integer value
        b (int): Integer value

    Returns:
        int: Integer value
    """
    c = a + b
    time.sleep(0.5)

    return c


class Manipulate_data:
    def __init__(self):
        self.vf_df = self.create_dummy_df()[0]
        self.table_config = "SingleLine"
        self.df = self.manipulate_df()[0]

    @time_logger_wrap
    @exception_wrap
    def create_dummy_df(self):
        df = pd.DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
        )

        return df

    @df_change_wrap
    @time_logger_wrap
    @exception_wrap
    def manipulate_df(self):
        df1 = self.vf_df
        df2 = self.vf_df * 2
        df = df1.append(df2)

        return df


def add(a: int, b: int):
    """Simple addition function with error raising for Pytest

    Args:
        a (int): Integer value
        b (int): Integer value

    Raises:
        TypeError: Raise error if a or b is not an integer

    Returns:
        int: Sum of both inputs
    """
    # Raise error if a or b is not an integer
    if not isinstance(a, int) or not isinstance(b, int):
        raise TypeError("a and b must be integers")

    return a + b
