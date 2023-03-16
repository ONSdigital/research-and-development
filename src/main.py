"""The main pipeline"""

# Importing from typing allows type hinting like Callable[[x,y],z]
import sys
import typing

from testfunctions import add

# Adds folder with utility function to path for easy imports
sys.path.append("/home/cdsw/research-and-development/src/utils")


def run_pipeline() -> typing.Callable[[int, int], int]:
    """Run the pipeline"""
    return add(1, 2)
