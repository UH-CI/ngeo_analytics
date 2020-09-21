import sqlite3
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
if len(argv) > 1:
    config_file = argv[1]
config = None
with open(config_file) as f:
    config = json.load(f)

def create_table():
    engine = db_connect.get_db_engine(config["extern_db_config"])
    try:
        query = text("""CREATE TABLE IF NOT EXISTS gene_gpl_ref_new (
            gpl TEXT NOT NULL,
            ref_id TEXT NOT NULL,
            gene_id TEXT NOT NULL,
            PRIMARY KEY (gpl(255), ref_id(255))
        );""")

        with engine.begin() as con:
            con.execute(query)
    
    finally:
        db_connect.cleanup_db_engine()


def nt_handler(lock):
    fname = config["out_files"]["nt_log"]
    def _nt_handler(gpl, ref_id = None):
        if ref_id is None:
            ref_id = ""
        with lock:
            with open(fname, "a") as f:
                f.write("%s,%s\n" % (gpl, ref_id))
    return _nt_handler

def error_handler(lock):
    fname = config["out_files"]["error_log"]
    def _error_handler(e):
        with lock:
            with open(fname, "a") as f:
                f.write("%s\n" % str(e))
    return _error_handler

def failure_handler(lock):
    fname = config["out_files"]["failure_log"]
    def _failure_handler(entries):
        with lock:
            with open(fname, "a") as f:
                for entry in entries:
                    f.write("%s,%s,%s\n" % (entry["gpl"], entry["ref_id"], entry["gene_id"]))
    return _failure_handler

def init_logs():
    fname = config["out_files"]["error_log"]
    with open(fname, "w") as f:
        f.write("")
    fname = config["out_files"]["failure_log"]
    with open(fname, "w") as f:
        f.write("gpl,ref_id,gene_id\n")

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


#HOW MANY THREADS AND PROCESSES
#gpl, ftp_handler, engine, batch_size, failure_lock, error_lock, nt_lock
def process_batch(batch, failure_lock, error_lock, nt_lock):
    threads = get_threads()
    #2 heartbeat threads should be ok (config?)
    ftp_opts = config["ftp_opts"]
    ftp_base = config["ftp_base"]
    g2a_db = config["gene2accession_file"]
    #want one handler (one ftp manager) for all threads, should be threadsafe
    ftp_handler = FTPHandler(ftp_base, ftp_opts)
    #also one engine for all threads
    engine = db_connect.get_db_engine(config["extern_db_config"])
    insert_batch_size = config["insert_batch_size"]
    db_retry = config["db_retry"]
    ftp_retry = config["ftp_retry"]
    try:
        with ThreadPoolExecutor(threads) as t_exec:
            for gpl in batch:
                #gpl, g2a_db, ftp_handler, db_retry, ftp_retry, engine, batch_size, failure_handler, error_handler, nt_handler
                f = t_exec.submit(platform_processor.handle_gpl, gpl, g2a_db, ftp_handler, db_retry, ftp_retry, engine, insert_batch_size, failure_handler(failure_lock), error_handler(error_lock), nt_handler(nt_lock))
                def cb(f):
                    e = f.exception()
                    #shouldn't throw exceptions, this is bad, print to console and log (note unknown what was inserted if this triggered so can't log failures)
                    if e is not None:
                        print(e, file=stderr)

    
                f.add_done_callback(cb)
    except Exception as e:
        print()
    finally:
        db_connect.cleanup_db_engine()

batches_complete = 0
def handle_batch(batch, p_exec, failure_lock, error_lock, nt_lock):
    if len(batch) == 0:
        return
    
    f = p_exec.submit(process_batch, batch, failure_lock, error_lock, nt_lock)
    def cb(f):
        global batches_complete
        e = f.exception()
        if e is not None:
            print(e, file = stderr)
            exit()
        batches_complete += 1
        print("Completed %d batches" % batches_complete)
    f.add_done_callback(cb)


def main():
    processes = get_processes()
    dbf = config["meta_db_file"]
    batch_size = config["gpl_per_batch"]
    init_logs()
    #need output files to be coordinated between processes, use manager locks
    #note this creates an extra process
    manager = Manager()
    failure_lock = manager.Lock()
    error_lock = manager.Lock()
    nt_lock = manager.Lock()

    create_table()

    con = sqlite3.connect(dbf)
    cur = con.cursor()
    #get all gpls
    query = """
    SELECT gpl
    FROM gpl
    """
    cur.execute(query)
    with ProcessPoolExecutor(processes) as p_exec:
        res = cur.fetchone()
        batch = []
        while res is not None:
            gpl = res[0]
            batch.append(gpl)
            if len(batch) % batch_size == 0:
                handle_batch(batch, p_exec, failure_lock, error_lock, nt_lock)
                batch = []
            res = cur.fetchone()
        #handle remaining items
        handle_batch(batch, p_exec, failure_lock, error_lock, nt_lock)



if __name__ == "__main__":
    main()