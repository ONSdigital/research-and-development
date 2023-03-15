"""The main pipeline"""

from utils.testfunctions import add

def run_pipeline() -> callable[[int,int], int]:
    """Run the pipeline"""
    return add(1, 2)