import sqlite3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager
from sys import stderr
import platform_processor



#HOW MANY THREADS AND PROCESSES

def process_batch(batch):
    with ThreadPoolExecutor() as t_exec:
        for gpl in batch:
            f = t_exec.submit(platform_processor.handle_gpl, gpl)
            def cb(f):
                pass
            f.add_done_callback(cb)

batches_complete = 0
def handle_batch(batch, p_exec, failure_lock):
    global batches_complete
    if len(batch) == 0:
        return
    
    f = p_exec.submit(process_batch, batch, failure_lock)
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
    batch_size = 10000

    manager = Manager()
    failure_lock = manager.Lock()

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
                handle_batch(batch, p_exec, failure_lock)
                batch = []
            res = cur.fetchone()
        #handle remaining items
        handle_batch(batch, p_exec, failure_lock)



if __name__ == "__main__":
    main()