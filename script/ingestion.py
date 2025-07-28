import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io
import os
from datetime import datetime
from .minio import MinIO

class Ingestion:
    def __init__(self, db, minio_endpoint, minio_access_key, minio_secret_key, minio_secure, bucket_name):
        """
        Initialize the Ingestion object.

        :param db: Database connection object.
        :param minio_endpoint: MinIO server endpoint.
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        :param minio_secure: Boolean indicating if MinIO connection is secure.
        :param bucket_name: Name of the MinIO bucket to use.
        """

        self.db = db
        self.minio = MinIO(
            endpoint=minio_endpoint, 
            access_key=minio_access_key, 
            secret_key=minio_secret_key, 
            secure=minio_secure
        )
        self.minio.set_bucket(bucket_name=bucket_name)
    
    
    def extract(self):
        pass
    
    
    
    def ingestion(self):
        pass                