"""Mocking the import of Pydoop"""
import sys


def hdfs_fake():
    pass


pydoop_fake = type(sys)("pydoop")
pydoop_fake.hdfs = hdfs_fake
sys.modules["pydoop"] = pydoop_fake
