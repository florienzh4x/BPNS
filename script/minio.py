from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
import os

class MinIO:
    def __init__(self):
        """
        Initialize MinIO client.

        The following environment variables are required to be set to initialize the MinIO client:

        - MINIO_ENDPOINT
        - MINIO_ACCESS_KEY
        - MINIO_SECRET_KEY
        - MINIO_BUCKET_NAME
        - MINIO_SECURE (default False)

        If MINIO_BUCKET_NAME is not set, the first bucket in the list of buckets will be used.

        :return: None
        """
        
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "admin_minio")
        secret_key = os.getenv("MINIO_SECRET_KEY", "admin_minio")
        secure = self.str_to_bool(os.getenv("MINIO_SECURE", "false"))
        bucket_name = os.getenv("MINIO_BUCKET_NAME", "landing-zones")

        self.minio_client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        
        if bucket_name is None:
            self.bucket = bucket_name
        else:
            self.bucket = self.list_buckets()[0]
        
    
    def str_to_bool(self, val):
        return val.lower() in ("true", "yes", "1", "on")
        
    def set_bucket(self, bucket_name):
        """
        Set the bucket from parameter to be used for operations.

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
    
    def list_objects(self, object_path):
        """
        List objects in the specified bucket in MinIO server with the given prefix.

        :param object_path: The prefix of the objects to list.
        :return: A list of object names.
        """
        
        object_list = []
        for object in self.minio_client.list_objects(bucket_name=self.bucket, prefix=object_path):
            object_list.append(object.object_name)
            
        return object_list
    
    def get_object(self, object_name):
        """
        Check if an object exists in the specified bucket in MinIO server.

        :param object_name: The name of the object to check for.
        :return: True if the object exists, False if it does not.
        """
        
        try:
            self.minio_client.get_object(self.bucket, object_name)
            return True
        except S3Error as err:
            if err.code == "NoSuchKey":
                print("Object not found")
                return False
            else:
                print(err)
        
    def put_object(self, buffer, object_name):
        """
        Put an object to the specified bucket in MinIO server. Default content type is `application/vnd.apache.parquet`.

        :param bucket_name: The name of the bucket to put the object in.
        :param object_name: The name of the object to put.
        :param buffer: A BytesIO or StringIO buffer containing the object data.
        """
        
        try:
            self.minio_client.put_object(
                bucket_name=self.bucket,
                object_name=object_name,
                data=buffer,
                length=len(buffer.getvalue()),
                content_type="application/vnd.apache.parquet"
            )
        except S3Error as err:
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
        except S3Error as err:
            print(err)
            
    def delete_object(self, object_name):
        """
        Delete an object from the specified bucket in the MinIO server.

        :param object_name: The name of the object to delete.
        """

        try:
            self.minio_client.remove_object(self.bucket, object_name)
        except S3Error as err:
            print(err)