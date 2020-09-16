import sqlite3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager
from sys import stderr
import platform_processor
from ftp_handler import FTPHandler
import db_connect

config_file = "config.json"
config = None
with open(config_file) as f:
    config = json.load(f)["extern_db_config"]

def nt_handler(lock):
    fname = config["out_files"]["nt_log"]
    def _nt_handler(gpl, row_id = None):
        if row_id is None:
            row_id = ""
        with lock:
            with open(fname, "a") as f:
                f.write("%s,%s\n" % (gpl, row_id))

def error_handler(lock):
    fname = config["out_files"]["error_log"]
    def _error_handler(e):
        with lock:
            with open(fname, "a") as f:
                f.write("%s\n" % str(e))

def failure_handler(lock):
    fname = config["out_files"]["failure_log"]
    def _failure_handler(entries):
        with lock:
            with open(fname, "a") as f:
                for entry of entries:
                    f.write("%s,%s,%s\n" % (entry["gpl"], entry["ref_id"], entry["gene_id"]))


#HOW MANY THREADS AND PROCESSES
#gpl, ftp_handler, engine, batch_size, failure_lock, error_lock, nt_lock
def process_batch(batch, failure_lock, error_lock, nt_lock):
    #2 heartbeat threads should be ok (config?)
    ftp_opts = {
        "heartbeat_threads": 2
    }
    ftp_base = "ftp.ncbi.nlm.nih.gov"
    #want one handler (one ftp manager) for all threads, should be threadsafe
    ftp_handler = FTPHandler(ftp_base, ftp_opts)
    #also one engine for all threads
    engine = db_connect.get_db_engine()
    insert_batch_size = 10000
    try:
        with ThreadPoolExecutor() as t_exec:
            for gpl in batch:
                #gpl, ftp_handler, engine, batch_size, failure_lock, error_lock, nt_lock
                f = t_exec.submit(platform_processor.handle_gpl, gpl, ftp_handler, engine, insert_batch_size, failure_handler(failure_lock), error_handler(error_lock), nt_handler(nt_lock))
                def cb(f):
                    e = f.exception()
                    #shouldn't throw exceptions, this is bad, print to console and log (note unknown what was inserted if this triggered so can't log failures)
                    if e is not None:
                        print(e, stderr)

    
                f.add_done_callback(cb)
    finally:
        db_connect.cleanup_db_engine()

batches_complete = 0
def handle_batch(batch, p_exec, failure_lock, error_lock, nt_lock):
    global batches_complete
    if len(batch) == 0:
        return
    
    f = p_exec.submit(process_batch, batch, failure_lock, error_lock, nt_lock)
    def cb(f):
        e = f.exception()
        if e is not None:
            print()
        batches_complete += 1
        print("Completed %d batches" % batches_complete)
    f.add_done_callback(cb)


def main():
    #config?
    dbf = "E:/ncbigeo/GEOmetadb.sqlite"
    batch_size = 1000
    #need output files to be coordinated between processes, use manager locks
    #note this creates an extra process
    manager = Manager()
    failure_lock = manager.Lock()
    error_lock = manager.Lock()
    nt_lock = manager.Lock()

    con = sqlite3.connect(dbf)
    cur = con.cursor()
    #get all gpls
    query = """
    SELECT gpl
    FROM gpl
    """
    cur.execute(query)
    with ProcessPoolExecutor() as p_exec:
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