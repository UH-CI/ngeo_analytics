import db_connect
from sqlalchemy import text


# def mark_gpl_processed(engine, gpl):
#     query = text("""
#         UPDATE gpl_processed
#         SET processed = false
#         WHERE gpl = :gpl;
#     """)
#     with engine.begin() as con:
#         con.execute(query, gpl = gpl)

# engine = db_connect.create_db_engine()
# try:
#    mark_gpl_processed(engine, "GPL9999")
#    db_connect.cleanup_db_engine()
# except Exception as e:
#     db_connect.cleanup_db_engine()
#     raise e

def reset_acquire():
    query = text("UPDATE gpl_processed SET acquired = -1")
    db_connect.engine_exec(query, None, 5)

def add_acquired_col():
    query = text("ALTER TABLE gpl_processed ADD acquired int NOT NULL DEFAULT(-1)")    
    db_connect.engine_exec(query, None, 5)

def pack_gpls_from_query(res):
    rows = res.fetchall()
    params = [{
        "gpl": row[0]
    } for row in rows]
    return params

#acquires batch of gpls to process, if length of return set is less than the specified batch size then this is the last set of platforms to process
def acquire_batch(exec_id, batch_size):
    gpls = []
    while len(gpls) < batch_size:
        remaining = batch_size - len(gpls)
        #get set to try acquire
        query = text("SELECT gpl FROM gpl_processed WHERE processed = false AND acquired = -1 LIMIT %d" % remaining)
        res = db_connect.engine_exec(query, None, 0)
        #attempt to acquire set (only acquire if acquired is -1 (not currently acquired))
        params = pack_gpls_from_query(res)
        #if params is 0 length then no results from first query (no more unaquired and unprocessed entries), break and return whatever currently acquired
        if len(params) == 0:
            break
        query = text("UPDATE gpl_processed SET acquired = %d WHERE gpl = :gpl AND acquired = -1" % exec_id)
        db_connect.engine_exec(query, params, 5)
        #get acquired set
        query = text("SELECT gpl FROM gpl_processed WHERE processed = false AND acquired = %d" % exec_id)
        res = db_connect.engine_exec(query, None, 0)
        rows = res.fetchall()
        gpls += [row[0] for row in rows]
    return gpls


def test_acquire_batch(exec_id, batch_size):
    gpls = []
    while len(gpls) < batch_size:
        remaining = batch_size - len(gpls)
        print(remaining)
        #get set to try acquire
        query = text("SELECT gpl FROM gpl_processed WHERE processed = false AND acquired = -1 LIMIT %d" % remaining)
        res = db_connect.engine_exec(query, None, 0)
        if exec_id == 1 and len(gpls) == 0:
            #attempt to mess it up, should only return 60 now since another acquired after check
            test_acquire_batch(2, 40)
        #attempt to acquire set (only acquire if scquired is -1 (not currently acquired))
        params = pack_gpls_from_query(res)
        query = text("UPDATE gpl_processed SET acquired = %d WHERE gpl = :gpl AND acquired = -1" % exec_id)
        db_connect.engine_exec(query, params, 0)
        #get acquired set
        query = text("SELECT gpl FROM gpl_processed WHERE processed = false AND acquired = %d" % exec_id)
        res = db_connect.engine_exec(query, None, 0)
        rows = res.fetchall()
        #final query will also include all preceding acquires, so just use new list
        gpls = [row[0] for row in rows]
    return gpls


def main():
    db_connect.create_db_engine()
    try:
        gpls = test_acquire_batch(1, 100)
        print(gpls)
        print(len(gpls))
        reset_acquire()
        db_connect.cleanup_db_engine()
    except Exception as e:
        db_connect.cleanup_db_engine()

if __name__ == "__main__":
    main()

