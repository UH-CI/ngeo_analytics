from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager
from sys import stderr, argv, exit
import platform_processor
from ftp_handler import FTPHandler
import db_connect
import json
from os import cpu_count
from sqlalchemy import text

config_file = "config.json"
if len(argv) < 3:
    raise RuntimeError("Invalid command line args. Must provide config file and execution id")
config_file = argv[1]
exec_id = int(argv[2])
config = None
with open(config_file) as f:
    config = json.load(f)

def create_table():
    query = text("""CREATE TABLE IF NOT EXISTS gene_gpl_ref_new (
        gpl TEXT NOT NULL,
        ref_id TEXT NOT NULL,
        gene_id TEXT NOT NULL,
        PRIMARY KEY (gpl(255), ref_id(255))
    );""")

    db_connect.engine_exec(query, None, 0)
    


# def nt_handler(lock):
#     fname = config["out_files"]["nt_log"]
#     def _nt_handler(gpl, ref_id = None):
#         if ref_id is None:
#             ref_id = ""
#         with lock:
#             with open(fname, "a") as f:
#                 f.write("%s,%s\n" % (gpl, ref_id))
#     return _nt_handler

def get_error_handler(lock):
    fname = config["out_files"]["error_log"]
    def _error_handler(e):
        with lock:
            with open(fname, "a") as f:
                f.write("%s\n" % str(e))
    return _error_handler

# def get_failure_handler(lock):
#     fname = config["out_files"]["failure_log"]
#     def _failure_handler(entries):
#         with lock:
#             with open(fname, "a") as f:
#                 for entry in entries:
#                     f.write("%s,%s,%s\n" % (entry["gpl"], entry["ref_id"], entry["gene_id"]))
#     return _failure_handler

def init_logs():
    fname = config["out_files"]["error_log"]
    with open(fname, "w") as f:
        f.write("")
    # fname = config["out_files"]["failure_log"]
    # with open(fname, "w") as f:
    #     f.write("gpl,ref_id,gene_id\n")

def get_processes():
    processes = config["p_limit"]
    if processes is None:
        processes = cpu_count()
    return processes

def get_threads():
    threads = config["t_limit"]
    if threads is None:
        threads = cpu_count()
    return threads

def mark_gpl_processed(gpl, retry):
    query = text("""
        UPDATE gpl_processed
        SET processed = true
        WHERE gpl = :gpl;
    """)
    params = {
        "gpl": gpl
    }
    db_connect.engine_exec(query, params, retry)
   


def process_batch(batch, error_lock):
    threads = get_threads()
    ftp_opts = config["ftp_opts"]
    ftp_base = config["ftp_base"]
    ftp_pool_size = config["ftp_pool_size"]
    g2a_db = config["gene2accession_file"]
    #want one handler (one ftp manager) for all threads, should be threadsafe
    ftp_handler = FTPHandler(ftp_base, ftp_pool_size, ftp_opts)
    #also one engine for all threads
    #just use engine exec for everything instead of passing around engine
    db_connect.create_db_engine(config["extern_db_config"])
    insert_batch_size = config["insert_batch_size"]
    db_retry = config["db_retry"]
    ftp_retry = config["ftp_retry"]
    #failures now tracked only by whole gpl by gpl_processed table for easy retry
    # failure_handler = get_failure_handler(failure_lock)
    error_handler = get_error_handler(error_lock)
    err = None
    try:
        with ThreadPoolExecutor(threads) as t_exec:
            for gpl in batch:
                #gpl, g2a_db, ftp_handler, db_retry, ftp_retry, batch_size
                f = t_exec.submit(platform_processor.handle_gpl, gpl, g2a_db, ftp_handler, db_retry, ftp_retry, insert_batch_size)
                def cb(gpl):
                    def _cb(f):
                        e = f.exception()
                        if e is not None:
                            e = "Error in gpl %s handler: %s" % (gpl, str(e))
                            error_handler(e)
                        #only mark as processed if an error wasn't thrown
                        else:
                            try:
                                mark_gpl_processed(gpl, db_retry)
                            #if an exception occured while trying to mark as processed then can always just reprocess, log error and ignore
                            except Exception as e:
                                e = "Error while updating gpl %s processed entry: %s" % (gpl, str(e))
                                error_handler(e)
                    return _cb
                f.add_done_callback(cb(gpl))
    except Exception as e:
        err = e
    finally:
        db_connect.cleanup_db_engine()
        ftp_handler.dispose()
    if err is not None:
        raise err

batches_complete = 0
def handle_batch(batch, p_exec, error_lock):
    if len(batch) == 0:
        return
    error_handler = get_error_handler(error_lock)
    f = p_exec.submit(process_batch, batch, error_lock)
    def cb(f):
        global batches_complete
        batches_complete += 1
        e = f.exception()
        if e is not None:
            e = "Uncaught error in batch %d: %s" % (batches_complete, str(e))
            error_handler(e) 
        print("Completed %d batches" % batches_complete)
    f.add_done_callback(cb)

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

def main():
    processes = get_processes()
    batch_size = config["gpl_per_batch"]
    init_logs()
    #need output files to be coordinated between processes, use manager locks
    #note this creates an extra process
    manager = Manager()
    error_lock = manager.Lock()

    res = None
    db_connect.create_db_engine(config["extern_db_config"])
    try:
        create_table()
        with ProcessPoolExecutor(processes) as p_exec:
            batch = acquire_batch(exec_id, batch_size)
            while len(batch) == batch_size:
                handle_batch(batch, p_exec, error_lock)
                batch = acquire_batch(exec_id, batch_size)
            handle_batch(batch, p_exec, error_lock)
            
            db_connect.cleanup_db_engine()
        print("Complete!")
    except Exception as e:
        db_connect.cleanup_db_engine()
        raise e
        
    



if __name__ == "__main__":
    main()