from sqlalchemy import text, String, Boolean, Column
from sqlalchemy.ext.declarative import declarative_base
import sqlite3
import db_connect
from time import sleep
from sys import stderr

Base = declarative_base()

class GPLProc(Base):
    __tablename__ = "gpl_processed"
    gpl = Column(String, primary_key=True)
    processed = Column(Boolean)

def handle_batch(batch, retry):
    if len(batch) > 0:
        query = text("REPLACE INTO gpl_processed (gpl, processed) VALUES (:gpl, :processed)")
        db_connect.engine_exec(query, batch, retry)


def create_db():
    query = text("""CREATE TABLE IF NOT EXISTS gpl_processed (
        gpl TEXT NOT NULL,
        processed BOOLEAN NOT NULL,
        PRIMARY KEY (gpl(255))
    );""")
    db_connect.engine_exec(query, None, 0)

dbf = "C:/GEOmetadb.sqlite"
db_connect.create_db_engine()
try:
    create_db()
    con = sqlite3.connect(dbf)
    cur = con.cursor()
    #get all gpls
    query = """
        SELECT gpl
        FROM gpl;
    """
    cur.execute(query)
    res = cur.fetchone()
    batch = []
    batch_size = 10000
    while res is not None:
        gpl = res[0]
        row = {
            "gpl": gpl,
            "processed": False
        }
        batch.append(row)
        if len(batch) % batch_size == 0:
            handle_batch(batch, 5)
            batch = []
        res = cur.fetchone()
    handle_batch(batch, 5)
    print("Complete!")
    
except Exception as e:
    print(e, file = stderr)
finally:
    db_connect.cleanup_db_engine()