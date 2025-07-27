from minio import Minio
from minio.error import ResponseError

class MinIO:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        
    def put_object(self, bucket_name, object_name, buffer):
        try:
            self.minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=buffer,
                length=len(buffer.getvalue()),
                content_type="application/vnd.apache.parquet"
            )
        except ResponseError as err:
            print(err)