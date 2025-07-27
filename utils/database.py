import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from .postgresql import Postgresql


class Database:
    def __init__(
        self, 
        db_type: str, 
        host: str, 
        port: int, 
        user: str, 
        password: str, 
        database: str, 
        schema: str = ""
    ):
        if db_type == "postgresql":
            self.impl = Postgresql(host=host, port=port, user=user, password=password, database=database, schema=schema)
        elif db_type == "mysql":
            pass
        
    def clean_connection(self):
        self.engine.dispose()    