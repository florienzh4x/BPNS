from .postgresql import Postgresql

class Database:
    def __init__(self,db_type: str, host: str, port: int, user: str, password: str, database: str, schema: str = ""):
        """
        Initialize Database object.

        :param db_type: type of database, can be "postgresql" or "mysql"
        :param host: host of database
        :param port: port of database
        :param user: username of database
        :param password: password of database
        :param database: name of database
        :param schema: name of database schema, default is empty string
        """
        if db_type == "postgresql":
            self.impl = Postgresql(host=host, port=port, user=user, password=password, database=database, schema=schema)
        elif db_type == "mysql":
            pass
        
    def clean_connection(self):
        """
        Clean database connection.

        Call this method after finishing with the database connection
        to avoid connection pool issues.
        """
        self.engine.dispose()    