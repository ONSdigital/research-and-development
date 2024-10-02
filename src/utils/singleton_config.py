"""
A class that initialises a single instance of boto3 client
"""
import boto3
import raz_client


class SingletonConfig:
    _instance = None

    def __init__(self):
        raise RuntimeError("This is a Singleton, invoke get_config() instead.")


    @classmethod
    def get_config(cls, config={}):  # , bucket_str= None):
        if cls._instance is None:
            Bucket=config["s3"]["s3_bucket"]
            cls._instance = Bucket
        return cls._instance

