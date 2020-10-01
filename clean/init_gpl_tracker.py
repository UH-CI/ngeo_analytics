from sqlalchemy import text, exc, String, Boolean, Column
from sqlalchemy.ext.declarative import declarative_base
import sqlite3
import db_connect
from time import sleep

Base = declarative_base()

class GPLProc(Base):
    __tablename__ = "gpl_processed"
    gpl = Column(String, primary_key=True)
    processed = Column(Boolean)

def handle_batch(engine, batch, retry, delay = 0):
    if retry < 0:
        error = Exception("Retry limit exceeded")
    #do nothing if empty list (note batch size limiting handled in caller in this case)
    elif len(batch) > 0:
        sleep(delay)
        try:
            with engine.begin() as con:
                con.execute(text("REPLACE INTO gpl_processed (gpl, processed) VALUES (:gpl, :processed)"), batch)

        except exc.OperationalError as e:
            #check if deadlock error (code 1213)
            if e.orig.args[0] == 1213:
                backoff = 0
                #if first failure backoff of 0.25-0.5 seconds
                if delay == 0:
                    backoff = 0.25 + random.uniform(0, 0.25)
                #otherwise 2-3x current backoff
                else:
                    backoff = delay * 2 + random.uniform(0, delay)
                #retry with one less retry remaining and current backoff
                error = submit_db_batch(engine, batch, retry - 1, backoff)
            #something else went wrong, log exception and add to failures
            else:
                raise e

def create_db(engine):
    query = text("""CREATE TABLE IF NOT EXISTS gpl_processed (
        gpl TEXT NOT NULL,
        processed BOOLEAN NOT NULL,
        PRIMARY KEY (gpl(255))
    );""")

    with engine.begin() as con:
        con.execute(query)

dbf = "C:/GEOmetadb.sqlite"
engine = db_connect.get_db_engine()
try:
    create_db(engine)
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
            handle_batch(engine, batch, 5)
            batch = []
        res = cur.fetchone()
    handle_batch(engine, batch, 5)
    
except Exception as e:
    print(e)
finally:
    db_connect.cleanup_db_engine()