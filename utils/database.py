from .postgresql import Postgresql
import os

class Database:
    def __init__(self):
        """
        Initialize the Database object.

        Determine the database type from the environment variable DB_TYPE and
        create an instance of the corresponding class to handle the database
        operations.

        :raises ValueError: If DB_TYPE is not set or not supported.
        """
        
        db_type = os.getenv("DB_TYPE", "postgresql")
        
        if db_type == "postgresql":
            self.impl = Postgresql()
        elif db_type == "mysql":
            pass
        
    def clean_connection(self):
        """
        Clean database connection.

        Call this method after finishing with the database connection
        to avoid connection pool issues.
        """
        self.engine.dispose()    