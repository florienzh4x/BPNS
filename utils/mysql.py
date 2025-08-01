import pandas as pd
from sqlalchemy import create_engine, inspect
import os

class MySQL:
    def __init__(self):
        """
        Initialize the MySQL class.

        Set up the database connection parameters using environment variables:
        - DB_HOST: Host address (default: "localhost")
        - DB_PORT: Port number (default: "3306")
        - DB_USER: Database user (default: "mysql_admin")
        - DB_PASSWORD: User password (default: "mysql_admin")
        - DB_NAME: Database name (default: "mysql")

        Create a SQLAlchemy engine object to connect to the MySQL database using the provided parameters.

        Initialize the schema attribute with the database name.
        """

        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "3306")
        user = os.getenv("DB_USER", "mysql_admin")
        password = os.getenv("DB_PASSWORD", "mysql_admin")
        database = os.getenv("DB_NAME", "mysql")
        
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
        
        self.schema = database
    
    def get_tables(self):
        """
        Retrieve the names of all tables within each schema in the database.

        This function inspects the database engine to gather a list of table names
        for every schema retrieved by the `get_schemas` method. The table names
        are organized in a dictionary where each key is a schema name and the
        corresponding value is a list of tables within that schema.

        :return: Dictionary where keys are schema names and values are lists of table names.
        """
        
        table_name_dict = {}
        
        inspector = inspect(self.engine)
        tables = inspector.get_table_names(self.schema)
        
        if tables:
            table_name_dict[self.schema] = tables
        
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
        
        tables_dict = {}
        
        for schema_key, table_list in schema_table_dict.items():
            
            tables_dict[schema_key] = []
            
            for table in table_list:
                
                inspector = inspect(self.engine)
                columns = inspector.get_columns(table, schema_key)
                
                if "updated_at" in columns and "created_at" in columns and "deleted_at" in columns:
                    tables_dict[schema_key].append({"table": table, "incremental": True})
                else:
                    tables_dict[schema_key].append({"table": table, "incremental": False})
                    
        return tables_dict
        