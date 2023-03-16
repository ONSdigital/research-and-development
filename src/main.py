"""The main pipeline"""

# Importing from typing allows type hinting like Callable[[x,y],z]
import utils  # noqa: F401
from typing import Callable as FunctionType
from utils.testfunctions import add


def run_pipeline() -> FunctionType[[int, int], int]:
    """Run the pipeline"""
    return add(1, 2)
