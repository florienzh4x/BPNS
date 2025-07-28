import pandas as pd
from sqlalchemy import create_engine, inspect
import os

class Postgresql:
    def __init__(self, host: str, port: int, user: str, password: str, database: str, schema: str = ""):
        """
        Initialize Postgresql object.

        :param host: host of database
        :param port: port of database
        :param user: username of database
        :param password: password of database
        :param database: name of database
        :param schema: name of database schema, default is empty string
        """
        
        # host = os.getenv("DB_HOST", host)
        # port = os.getenv("DB_PORT", port)
        # user = os.getenv("DB_USER", user)
        # password = os.getenv("DB_PASSWORD", password)
        # database = os.getenv("DB_NAME", database)
        # schema = os.getenv("DB_SCHEMA", schema)
        
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        
        if schema:
            self.schema = schema
        else:
            self.schema = "all"
    
    def get_schemas(self):
        """
        Get all schemas in database if self.schema is "all", otherwise return self.schema in list.

        :return: list of schemas
        """
        
        if self.schema == "all":
        
            inspector = inspect(self.engine)
            schemas = inspector.get_schema_names()
            schemas.remove("information_schema")
            
            return schemas
        else:
            return [self.schema]
        
    def get_tables(self):
        """
        Retrieve the names of all tables within each schema in the database.

        This function inspects the database engine to gather a list of table names
        for every schema retrieved by the `get_schemas` method. The table names
        are organized in a dictionary where each key is a schema name and the
        corresponding value is a list of tables within that schema.

        :return: Dictionary where keys are schema names and values are lists of table names.
        """

        schema_list = self.get_schemas()
        
        table_name_dict = {}
        
        for schema in schema_list:
        
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema)
            
            table_name_dict[schema] = tables
            
        return table_name_dict
    
    def get_load_status(self):
        """
        Retrieve a dictionary of schema names and their associated tables with incremental load status.

        The dictionary returned by this function will have schema names as keys and lists of dictionaries as values.
        Each dictionary within the list will contain the name of a table and a boolean indicating whether that table
        has an incremental load implemented. The boolean value is derived from the presence of columns named
        "updated_at", "created_at", and "deleted_at" in the table. If all three columns are present, the "incremental"
        value in the dictionary will be True, otherwise it will be False.

        :return: Dictionary where keys are schema names and values are lists of dictionaries containing the name of a
        table and its associated incremental load status.
        """
        
        schema_table_dict = self.get_tables()
        
        column_dict = {}
        
        for schema, table_list in schema_table_dict.items():
            
            column_dict[schema] = []
            
            for table in table_list:
                
                inspector = inspect(self.engine)
                columns = inspector.get_columns(table, schema)
                
                incremental_status = 0
                
                for column in columns:
                    if column["name"] in ["updated_at", "created_at", "deleted_at"]:
                        incremental_status += 1
                    else:
                        pass
                    
                if incremental_status == 3:
                    column_dict[schema].append({"table": table, "incremental": True})
                else:
                    column_dict[schema].append({"table": table, "incremental": False})
                
        return column_dict
    
    def select_data(self, query: str):
        """
        Execute a SQL query and return the result as a pandas DataFrame.

        :param query: SQL query to be executed
        :return: pandas DataFrame containing the result of the query
        """

        df = pd.read_sql(query, self.engine)
        
        return df