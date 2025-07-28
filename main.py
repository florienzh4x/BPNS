import os
from utils.database import Database
from script.ingestion import Ingestion

db = Database("postgresql", "localhost", 5432, "postgres", "postgres", "dvdrental")
ingestion = Ingestion(
                db, 
                os.environ["MINIO_ENDPOINT"], 
                os.environ["MINIO_ACCESS_KEY"], 
                os.environ["MINIO_SECRET_KEY"], 
                os.environ["MINIO_SECURE"], 
                os.environ["MINIO_BUCKET_NAME"]
            )