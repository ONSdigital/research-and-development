"""
A class that initialises a single instance of boto3 client
"""
import boto3
import raz_client


class SingletonBoto:
    _instance = None
#    _bucket_str = None

    def __init__(self):
        raise RuntimeError("This is a Singleton, invoke get_client() instead.")


    @classmethod
    def get_client(cls):  # , bucket_str= None):
        if cls._instance is None:
            client = boto3.client("s3")
            raz_client.configure_ranger_raz(
                client,
                ssl_file="/etc/pki/tls/certs/ca-bundle.crt"
             )
            cls._instance = client
#            cls._bucket_str = bucket_str
        return cls._instance
      
#    def get_bucket():
#        return cls._bucket_str
