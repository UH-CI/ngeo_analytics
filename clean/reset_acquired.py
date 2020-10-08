from db_connect import DBConnector
from sqlalchemy import text

with DBConnector() as connector:
    query = text("UPDATE gpl_processed SET acquired = -1;")
    connector.engine_exec(query, None, 5)


