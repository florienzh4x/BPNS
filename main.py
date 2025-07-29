from utils.database import Database
from script.ingestion import Ingestion

db = Database()
ingestion = Ingestion(db)
ingestion.extract()