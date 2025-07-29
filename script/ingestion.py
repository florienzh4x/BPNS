import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import io
from datetime import datetime
from .minio import MinIO

class Ingestion:
    def __init__(self, db):
        """
        Initialize Ingestion object.

        :param db: An instance of utils.database.Database to handle database operations.
        :return: None
        """
        
        self.db = db
        self.minio = MinIO()
        
    def convert_df_to_parquet(self, df):
        """
        Convert given pandas DataFrame to a parquet-formatted bytes object.

        :param df: A pandas DataFrame object to be converted to parquet.
        :return: A bytes object containing parquet-formatted data from given DataFrame.
        """
        
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buffer, compression="snappy")
        
        buffer.seek(0)
        
        return buffer
    
    def ingest_to_minio(self, buffer, object_name):
        """
        Ingest given bytes object to MinIO server at the specified object name.

        :param buffer: A bytes object containing data to be ingested.
        :param object_name: The name of the object to ingest the data to.
        :return: None
        """
        
        self.minio.put_object(
                buffer=buffer,
                object_name=object_name
        )
        
    def parsing_schema_obj(self, obj_list):
        """
        Parse given list of dictionaries into a list of dictionaries containing object path and table name query
        for incremental load.

        The given list of dictionaries must be in the following format:

        {
            "database_name_schema": [
                {
                    "table": "table_name",
                    "incremental": True|False
                },
                ...
            ],
            ...
        }

        The output list of dictionaries will be in the following format:

        [
            {
                "object_path": "database_name/schema_name/table_name/latest/",
                "table_name_query": "schema_name.table_name",
                "incremental": True|False
            },
            ...
        ]

        :param obj_list: A list of dictionaries containing database schema and list of tables with their incremental
        load status.
        :return: A list of dictionaries containing object path and table name query for incremental load.
        """
        
        output = []
        
        for db_schema, table_list in obj_list.items():
            
            for table in table_list:
                
                table_obj = {}
                
                if type(db_schema) == tuple:
                    database_name, schema = db_schema
                    table_obj["object_path"] = f"{database_name}/{schema}/{table['table']}/latest/"
                else:
                    schema = db_schema
                    table_obj["object_path"] = f"{schema}/{table['table']}/latest/"
                    
                if table["incremental"]:
                    table_obj["incremental"] = True
                else:
                    table_obj["incremental"] = False
                
                table_obj["table_name_query"] = f"{schema}.{table['table']}"
                    
                output.append(table_obj)
                    
        
        return output
    
    def incremental_load(self, object_path, table_name_query):
        """
        Perform an incremental load of the given table to MinIO server at the specified object path.
        
        If no data has been ingested to MinIO server for the given table, perform a full load. Otherwise, perform an
        incremental load by selecting data from the given table where the "updated_at" column's value is greater than or
        equal to the current date.
        
        :param object_path: The path of the object in MinIO server to ingest the data to.
        :param table_name_query: The name of the table in the database to load from.
        :return: None
        """
        
        get_obj_exists = self.minio.list_objects(object_path)
        
        if not get_obj_exists:
            self.load_all(object_path, table_name_query)
        else:
            df = pd.read_sql(f"SELECT * FROM {table_name_query} where updated_at >= '{datetime.now().strftime('%Y-%m-%d')}'", self.db.impl.engine)
            
            if not len(df):
                pass
            else:
                parquet_buffer = self.convert_df_to_parquet(df)
                
                object_name = f"{object_path}{datetime.now().strftime('%Y%m%d')}.parquet"
                
                self.ingest_to_minio(parquet_buffer, object_name)
    
    def load_all(self, object_path, table_name_query):
        """
        Perform a full load of the given table to MinIO server at the specified object path.

        :param object_path: The path of the object in MinIO server where the data will be ingested to.
        :param table_name_query: The name of the table to load data from.
        :return: None
        """
        
        df_list = pd.read_sql(f"SELECT * FROM {table_name_query}", self.db.impl.engine, chunksize=10000)
        
        count = 0
        
        for df in df_list:
            
            parquet_buffer = self.convert_df_to_parquet(df)
            
            object_name = f"{object_path}{datetime.now().strftime('%Y%m%d')}_{count}.parquet"
            
            self.ingest_to_minio(parquet_buffer, object_name)
            count += 1
        
        
    def extract(self):
        """
        Start the ELT process by getting the schema and table names from the database
        and then decide whether to do an incremental load or a full load based on the
        existence of a parquet file in the object storage. If the file exists, do an
        incremental load, otherwise do a full load.

        :return: None
        """
        
        table_stats_list = self.db.impl.get_load_status()
        
        schema_obj_list = self.parsing_schema_obj(table_stats_list)
        
        for schema_obj in schema_obj_list:
            
            if schema_obj["incremental"]:
                self.incremental_load(schema_obj["object_path"], schema_obj["table_name_query"])
            else:
                self.load_all(schema_obj["object_path"], schema_obj["table_name_query"])