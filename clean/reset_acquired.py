import db_connect
from sqlalchemy import text

db_connect.create_db_engine()
try:
    query = text("UPDATE gpl_processed SET acquired = -1")
    db_connect.engine_exec(query, None, 5)
    db_connect.cleanup_db_engine()
except Exception as e:
    db_connect.cleanup_db_engine()
    raise e

