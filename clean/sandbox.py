import db_connect
from sqlalchemy import text


def mark_gpl_processed(engine, gpl):
    query = text("""
        UPDATE gpl_processed
        SET processed = false
        WHERE gpl = :gpl;
    """)
    with engine.begin() as con:
        con.execute(query, gpl = gpl)

engine = db_connect.get_db_engine()
try:
   mark_gpl_processed(engine, "GPL9999")
   db_connect.cleanup_db_engine()
except Exception as e:
    db_connect.cleanup_db_engine()
    raise e




