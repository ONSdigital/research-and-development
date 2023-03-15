"""The main pipeline"""

# Adds folder with utility function to path for easy imports
import sys
sys.path.append('/home/cdsw/research-and-development/src/utils')

import helpers
from testfunctions import add

# Importing from typing allows type hinting like Callable[[x,y],z]
from typing import *

def run_pipeline() -> Callable[[int,int], int]:
    """Run the pipeline"""
    return add(1, 2)