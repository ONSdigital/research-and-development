"""Mocking the import of Pydoop"""
import sys

module = type(sys)("pydoop")
sys.modules["pydoop"] = module
