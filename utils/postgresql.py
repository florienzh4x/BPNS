import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect

class Postgresql:
    def __init__(self, host: str, port: int, user: str, password: str, database: str, schema: str = ""):
        
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
        
        if schema:
            self.schema = schema
        else:
            self.schema = "all"
    
    def get_schemas(self):
        
        if self.schema == "all":
        
            inspector = inspect(self.engine)
            schemas = inspector.get_schema_names()
            schemas.remove("information_schema")
            
            return schemas
        else:
            return [self.schema]
        
    def get_tables(self):
        
        schema_list = self.get_schemas()
        
        table_name_dict = {}
        
        for schema in schema_list:
        
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema)
            
            table_name_dict[schema] = tables
            
        return table_name_dict
    
    def get_load_status(self):
        
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
        
        df = pd.read_sql(query, self.engine)
        
        return df