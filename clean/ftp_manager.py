import time
import threading
import ftplib
from concurrent.futures import ThreadPoolExecutor
from os import cpu_count
import inspect



#global id to use for easy referencing ftp connections
ftp_con_id = 0
id_lock = threading.Lock()


class FTPConnection():
    
    def __init__(self, uri):
        global ftp_con_id
        global id_lock
        with id_lock:
            self.id = ftp_con_id
            ftp_con_id += 1
        self.disposed = False
        self.lock = threading.Lock()
        #don't lock so can acquire before finished initializing, but don't use until initialized
        self.initialized = threading.Event()
        #initialize connection in thread, should provide speedup since network bound
        self.threaded_initialize_connection(uri)
        self.uri = uri
        self.heartbeat_idle = threading.Event()
        #initially heartbeat is idle
        self.heartbeat_idle.set()

    def threaded_initialize_connection(self, uri):
        t = threading.Thread(target = self.create_connection, args = (uri,))
        t.start()

    def create_connection(self, uri, cb = None):
        self.ftp = ftplib.FTP(uri)
        self.ftp.login()
        self.initialized.set()
        

    #replace ftp with new connection, note should be locked before calling
    def reconnect(self, threaded = True):
        #connection not initialized while reconnecting
        self.initialized.clear()
        #try to quit the connection in case still active
        self.disconnect()
        if threaded:
            self.threaded_initialize_connection(self.uri)
        else:
            self.create_connection(self.uri)

    def locked(self):
        return self.lock.locked()

    def disconnect(self):
        try:
            self.ftp.quit()
        except ftplib.all_errors:
            pass

    def dispose(self):
        self.disposed = True
        self.disconnect()

    def acquire(self, blocking = True):
        # print(inspect.stack()[1][3])
        # print("lock called")
        # print(self.lock.locked())
        # success = 
        # print(success)
        return self.lock.acquire(blocking)
    
    def release(self):
        return self.lock.release()

    def __enter__(self):
        self.acquire()
    
    def __exit__(self, type, value, tb):
        self.release()

    #use id as hash
    def __hash__(self):
        return self.id

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return self.id != other.id



class FTPManager:

    def __init__(self, uri, init_cons = 10, heartrate = 2, heartbeat_threads = None):
        #accessing static variables from constructor is weird because pythons a wonderful language, so just define these here
        MIN_CONS = 10
        MAX_CONS = 1000
        #if only 5 connections left prepare some more
        BUFFER_CONS = 5
        #if more than 10 connections to spare start removing some
        PRUNE_BUFFER = 10
        MIN_HEARTRATE = 1
        CPUS = cpu_count()
        #default to 2 helpers and a main
        if CPUS is None:
            CPUS = 3

        if init_cons < MIN_CONS:
            init_cons = MIN_CONS
        elif init_cons > MAX_CONS:
            init_cons = MAX_CONS
        if heartrate <= MIN_HEARTRATE:
            heartrate = MIN_HEARTRATE

        self.prune_buffer = PRUNE_BUFFER
        self.buffer_cons = BUFFER_CONS
        self.min_cons = MIN_CONS
        self.max_cons = MAX_CONS

        if heartbeat_threads is None:
            self.heartbeat_threads = CPUS
        #need at least 2 (pulse check and main event cycle)
        elif heartbeat_threads < 2:
            self.heartbeat_threads = 2
        else:
            self.heartbeat_threads = heartbeat_threads

        self.heartrate = heartrate

        self.uri = uri
        self.in_use = 0

        #should be sufficient to use single resource lock (multiples shouldn't be more efficient due to GIL)
        #wonder what the overhead is for locking/unlocking something in python, might actually be less efficient
        self.resource_lock = threading.Lock()
        self.connections = {FTPConnection(uri) for i in range(init_cons)}
        def staggered_cons(num, interval):
            for i in range(init_cons):
                time.sleep(interval)
                with self.resource_lock:
                    self.add_connection()
        #stagger at 5 second intervals
        startup_t = threading.Thread(target = staggered_cons, args = (init_cons - 1, 5,), daemon = True)

        self.ended = threading.Event()
        self.all_busy = threading.Semaphore(MAX_CONS)
        
        #heartbeat thread to ensure connections stay alive
        heartbeat = threading.Thread(target = self.__heartbeat, daemon = True)
        #start heartbeat thread
        heartbeat.start()


    #(ftp, lock)
    def __try_check_pulse(self, con):
        # print("attempting pulse check")
        #if connection in use, just pass heartbeat check
        #also check if disposed and make sure manager wasn't ended
        # acquired = con.acquire(False)
        con.heartbeat_idle.clear()
        if con.locked() or con.disposed or self.ended.is_set():
            con.heartbeat_idle.set()
            # #if the lock was acquired but one of the other conditions went through then unlock
            # if acquired:
            #     con.release()
            return
        #connection acquired (locked)
        if not self.__check_pulse(con):
            con.reconnect()
        con.heartbeat_idle.set()
            
        # #release connection (unlock)
        # con.release()

    def __check_pulse(self, con):
        success = True
        try:
            # print("checking")
            con.ftp.voidcmd("NOOP")
            # print("badum\n\n")
        except ftplib.all_errors:
            # print("-----\n\n")
            success = False
        return success

    def __heartbeat(self):
        #-1 for main heartbeat thread
        pulse_exec = ThreadPoolExecutor(self.heartbeat_threads - 1)
        #should not submit new heartbeat check for a given connection if old one not complete
        #so maintain list of the connections and their heartbeat futures
        heartbeat_refs = {}
        #maintain own copy of connection list and check if need to update to maintain integrity without having to lock resources
        while not self.ended.is_set():
            #check pulses every heartrate seconds
            time.sleep(self.heartrate)

            #check/clean heartbeat refs that are already stored
            disposed = []
            for connection in heartbeat_refs:
                #first check if the connection has been disposed
                if connection.disposed:
                    #append to list and delete after to maintain dict integrity while iterating (might behave oddly otherwise)
                    disposed.append(connection)
                else:
                    future = heartbeat_refs[connection]
                    #if not done do nothing, already queued for pulse check
                    if future.done():
                        new_future = pulse_exec.submit(self.__try_check_pulse, (connection))
                        heartbeat_refs[connection] = new_future
            #remove the disposed connections from ref dict
            for connection in disposed:
                del heartbeat_refs[connection]

            new_cons = []
            #acquire resource lock so can iterate over connections list
            with self.resource_lock:
                #just checking if any new connections not tracked by ref dict have been added
                for connection in self.connections:
                    if heartbeat_refs.get(connection) is None:
                        #want to hold resource lock for as little time as possible, not sure what submit overhead is like, so just add to list for now and submit after releasing resource lock
                        new_cons.append(connection)
            #submit pulse check for new connections
            for connection in new_cons:
                future = pulse_exec.submit(self.__try_check_pulse, (connection))
                heartbeat_refs[connection] = future



    def end_all(self):
        if ended.is_set:
            return
        #no more operations
        self.ended.set()
        for con in self.connections:
            #need to wait until not busy
            with con:
                con.dispose()
        
    def get_available(self):
        #return none if ended
        if self.ended.is_set():
            return None

        #block if maximum number of connections are busy
        self.all_busy.acquire()
        self.resource_lock.acquire()

        available = None
        backup = None
        for con in self.connections:
            #if not initialized or pulse check running can be used, but keep looking for one that is already available and isn't in use
            if (not con.initialized.is_set() or not con.heartbeat_idle.is_set()) and available is None:
                #still need to acquire
                if con.acquire(False):
                    available = con
            #try to acquire the connection, move on if already locked
            elif con.acquire(False):
                #already locked on an available (uninitilized) connection, need to unlock
                if available is not None:
                    available.release()
                #connection acquired, ready for use by caller
                available = con
                break
            else:
                #just store item as backup in case none available for some reason (should probably never happen)
                backup = con
        #done with resource lock
        self.resource_lock.release()
        #all connections were busy (shouldn't happen very often, only if pulse check taking all available or starting up, just wait and retry)
        if available is None:
        
            #this should never happen
            if backup is None:
                raise RuntimeError("No connections available. Could not get an FTP connection")

            #acquire backup connection (block until can acquire)
            backup.acquire()
            available = backup
        
        return available


    def get_con(self):
        con = self.get_available()
        with self.resource_lock:
            self.in_use += 1
            self.check_add_connection()
        #wait until pulse check finished if running
        con.heartbeat_idle.wait()
        #wait until initialized if it isn't
        con.initialized.wait()
        return con
            


    def release_con(self, con, problem = False):
        
        self.resource_lock.acquire()

        self.in_use -= 1

        pruned = self.check_prune_connection(con)

        self.resource_lock.release()

        #caller indicated a possible problem with the connection
        #don't bother fixing potentially broken connections if pruned
        if not pruned and problem:
            #check if problem and reconnect if there is
            if self.__check_pulse(con):
                con.reconnect()
        con.release()

        self.all_busy.release()


    #resource lock should be acquired when calling
    def check_add_connection(self):
        #check if idle connections is less than the number of buffer connections and total connections less than max
        if len(self.connections) - self.in_use < self.buffer_cons and len(self.connections) < self.max_cons:
            self.add_connection()

    def add_connection(self):
        con = FTPConnection(self.uri)
        self.connections.add(con)


    #resource lock should be acquired when calling
    #no need to manage a ton of connections if most of them are idle
    def check_prune_connection(self, con):
        pruned = False
        #don't need to do this
        if self.ended.is_set():
            return

        #check if idle connections exceeds prune buffer and make sure not to prune past minimum number of connections
        if len(self.connections) - self.in_use > self.prune_buffer and len(self.connections) > self.min_cons:
            self.prune_connection(con)
            pruned = True

        return pruned


    def prune_connection(self, con):
        con.dispose()
        self.connections.remove(con)




    








    