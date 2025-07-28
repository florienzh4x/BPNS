from minio import Minio
from minio.error import ResponseError
from minio.commonconfig import CopySource
import os

class MinIO:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        """
        Initialize the MinIO client.

        :param endpoint: The endpoint URL for the MinIO server.
        :param access_key: The access key for MinIO authentication.
        :param secret_key: The secret key for MinIO authentication.
        :param secure: Boolean indicating if the connection to MinIO should use HTTPS.
        """
        
        # endpoint = os.environ["MINIO_ENDPOINT"]
        # access_key = os.environ["MINIO_ACCESS_KEY"]
        # secret_key = os.environ["MINIO_SECRET_KEY"]
        # secure = os.environ["MINIO_SECURE"]
        # bucket_name = os.environ["MINIO_BUCKET_NAME"]

        # self.bucket = bucket_name
        self.bucket = None
        
        self.minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        
    def set_bucket(self, bucket_name):
        """
        Set the bucket to be used for operations.

        :param bucket_name: The name of the bucket to use.
        """
        self.bucket = bucket_name
    
    def list_buckets(self):
        """
        List the buckets in the MinIO server.

        :return: A list of bucket names.
        """
        
        buckets = self.minio_client.list_buckets()        
        bucket_list = []
        for bucket in buckets:
            bucket_list.append(bucket.name)
        
        return bucket_list
    
    def get_object(self, bucket_name, object_name):
        """
        Check if an object exists in the specified bucket in MinIO server.

        :param bucket_name: The name of the bucket to check in.
        :param object_name: The name of the object to check for.
        :return: True if the object exists, False if it does not.
        """
        
        try:
            self.minio_client.get_object(bucket_name, object_name)
            return True
        except ResponseError as err:
            if err.code == "NoSuchKey":
                print("Object not found")
                return False
            else:
                print(err)
        
    def put_object(self, bucket_name, object_name, buffer):
        """
        Put an object to the specified bucket in MinIO server. Default content type is `application/vnd.apache.parquet`.

        :param bucket_name: The name of the bucket to put the object in.
        :param object_name: The name of the object to put.
        :param buffer: A BytesIO or StringIO buffer containing the object data.
        """
        
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
            
    def copy_object(self, src_bucket, src_object, dst_bucket, dst_object):
        """
        Copy an object from one bucket to another/same bucket in the MinIO server.

        :param src_bucket: The name of the source bucket.
        :param src_object: The name of the source object.
        :param dst_bucket: The name of the destination bucket.
        :param dst_object: The name of the destination object.
        """
        
        try:
            self.minio_client.copy_object(src_bucket, src_object, dst_bucket, dst_object)
            
            source = CopySource(src_bucket, src_object)
            
            self.minio_client.copy_object(
                bucket_name=dst_bucket,
                object_name=dst_object,
                source=source
            )
        except ResponseError as err:
            print(err)
            
    def delete_object(self, bucket_name, object_name):
        """
        Delete an object from the specified bucket in the MinIO server.

        :param bucket_name: The name of the bucket from which the object will be deleted.
        :param object_name: The name of the object to delete.
        """

        try:
            self.minio_client.remove_object(bucket_name, object_name)
        except ResponseError as err:
            print(err)