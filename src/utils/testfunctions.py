import time


def add(a: int, b: int) -> int:
    """Testing multiple wrappers for one function"""
    # Raise error if a or b is not an integer
    if not isinstance(a, int) or not isinstance(b, int):
        raise TypeError("a and b must be integers")
    c = a + b
    time.sleep(0.5)
    return c
