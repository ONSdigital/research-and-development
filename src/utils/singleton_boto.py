"""
A class that initialises a single instance of boto3 client
"""
import boto3
import raz_client


class SingletonBoto:
    _instance = None

    def __init__(self):
        raise RuntimeError("This is a Singleton, invoke get_client() instead.")


    @classmethod
    def get_client(cls, config={}):  # , bucket_str= None):
        if cls._instance is None:
            client = boto3.client("s3")
            raz_client.configure_ranger_raz(
                client,
                ssl_file=config["s3"]["ssl_file"]
             )
            Bucket=config["s3"]["s3_bucket"]
            cls._instance = client
        return cls._instance
