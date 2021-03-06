import requests
import io
import gzip
import ftplib
import threading
import zlib
import csv
# import soft_parser
import time
import GEOparse
import sys


#threadsafe
#consumes data as it goes to save memory
class CircularRWBuffer():
    def __init__(self, init_buffer_size):
        self.buf = bytearray(init_buffer_size)
        self.max_size = init_buffer_size
        self.size = 0
        self.head = 0
        self.tail = 0
        self.access_lock = threading.Lock()
        self.complete = False
        self.data_block = threading.Event()

    def write(self, data):
        write_size = len(data)
        # print(write_size)
        #a lot of things mess with state consistency, so just lock everything
        self.access_lock.acquire()
        #overflow, resize buffer and try again
        if self.size + write_size > self.max_size:
            self.__resize()
            #need to release lock before recursing
            self.access_lock.release()
            #try to write again with new larger buffer
            return self.write(data)

        #wrap around (note if head already wrapped cannot overlap tail without triggering overflow condition)
        elif self.head + write_size > self.max_size:
            len_to_end = self.max_size - self.head
            remainder = write_size - len_to_end

            self.buf[self.head : self.max_size] = data[0 : len_to_end]
            self.buf[0 : remainder] = data[len_to_end : write_size]
            self.head = remainder
        else:
            self.buf[self.head : self.head + write_size] = data
            #update head
            self.head = self.head + write_size

        self.size += write_size
        #indicate data is present
        self.data_block.set()

        self.access_lock.release()


        #return bytes written to buffer
        return write_size

    def end_of_data(self):
        self.access_lock.acquire()
        self.complete = True
        #set data block signal to make sure no read is stuck on completion
        self.data_block.set()
        self.access_lock.release()


    def read(self, read_size = None, block = True, timeout = None):

        #a lot of things mess with state consistency, so just lock everything
        self.access_lock.acquire()
        #block and wait for data if none present, block is true, and data not finished
        if block and not self.complete:
            #data block might be set if read waiting for more data, let me through if enough data for me
            #could lead to resource starving if one reader waiting on a lot of data, not an issue for this though (and can use timeout if need)
            if self.size == 0 or (read_size is not None and read_size > self.size):
                self.access_lock.release()
                if not self.data_block.wait(timeout):
                    raise TimeoutError("Read request timed out")
                self.access_lock.acquire()


        #data is finished and all read, return empty
        if self.complete and self.size == 0:
            self.access_lock.release()
            return bytes()
        
        #need to block until proper number of bytes ready!!! (issue is probably that read expects n bytes and providing less because it's ready)
        #only provide < asked for bytes if eof (data stream complete)

        #if not blocking then just read what's there
        if read_size is None or (read_size > self.size and (self.complete or not block)):
            read_size = self.size
        #set data block and retry read, should go through after more data written (note only can trigger if block is true, otherwise caught by if condition)
        elif read_size > self.size:
            self.data_block.clear()
            self.access_lock.release()
            return self.read(read_size, block, timeout)
        
        
        data = bytearray(read_size)
        #wrap around
        if self.tail + read_size > self.max_size:
            # print("read_wrapped")
            p1_len = self.max_size - self.tail
            remainder = read_size - p1_len
            data[0 : p1_len] = self.buf[self.tail : self.max_size]
            data[p1_len : read_size] = self.buf[0 : remainder]
            self.tail = remainder
        #not wrapped, can grab directly
        else:
            data[0 : read_size] = self.buf[self.tail : self.tail + read_size]
            self.tail = self.tail + read_size
        self.size -= read_size

        if self.size == 0:
            self.data_block.clear()

        self.access_lock.release()

        # print(self.tail)

        return bytes(data)


    def __resize(self):
        temp = self.buf
        temp_size = self.max_size
        self.max_size *= 2
        self.buf = bytearray(self.max_size)
        self.__transfer(temp, temp_size, self.buf)
        temp = None

    def __transfer(self, old, old_size, new):
        #wrapped around
        if self.head < self.tail:
            p1_len = old_size - self.tail
            new[0 : p1_len] = old[self.tail : old_size]
            new[p1_len : self.size] = old[0 : self.head]
        #not wrapped, can transfer directly
        else:
            new[0 : self.size] = old[self.tail : self.head]
        self.tail = 0
        self.head = self.size

    # #close the stream so 
    # def close():



ftp_base = "ftp.ncbi.nlm.nih.gov"

type_details = {
    "platform": {
        "acc_prefix": "GPL",
        "resource_base": "/geo/platforms/",
        "resource_suffix": "soft/",
        "file_suffix": "_family.soft.gz"
    },
    "series": {
        "acc_prefix": "GSE",
        "resource_base": "/geo/series/",
        "resource_suffix": "matrix/",
        "file_suffix": "_series_matrix.txt.gz"
    }
}


ftp = ftplib.FTP(ftp_base)
ftp.login()


def get_ftp_files(dir):
    #return empty list if exception thrown
    files = []
    try:
        files = ftp.nlst(dir)
    except Exception:
        pass
    return files
    
def get_resource_dir(accession, resource_details):
    acc_prefix = resource_details["acc_prefix"]
    resource_base = resource_details["resource_base"]
    resource_suffix = resource_details["resource_suffix"]

    resource_id = accession[3:]
    id_len = len(resource_id)
    resource_prefix = ""
    if id_len > 3:
        resource_prefix = resource_id[:-3]
    resource_cat = "%s%snnn/%s/" % (acc_prefix, resource_prefix, accession)
    
    resource_dir = "%s%s%s" % (resource_base, resource_cat, resource_suffix)

    return resource_dir


def get_gpl_data_stream(gpl, data_processor):
    resource_details = {
        "acc_prefix": "GPL",
        "resource_base": "/geo/platforms/",
        "resource_suffix": "soft/",
    }

    file_suffix = "_family.soft.gz"
    fname = "%s%s" % (gpl, file_suffix)

    resource_dir = get_resource_dir(gpl, resource_details)
    #verify resource exists and thow exception if it doesn't
    files = get_ftp_files(resource_dir)
    if fname not in files:
        raise Exception("Resource not found")
    
    resource = "%s%s" % (resource_dir, fname)

    get_data_stream_from_resource(resource, data_processor)


def get_gse_data_stream(gse, gpl, data_processor):
    resource_details = {
        "acc_prefix": "GSE",
        "resource_base": "/geo/series/",
        "resource_suffix": "matrix/",
    }
    #file name can be one or the other depending if associated with single platform or multiples
    file_suffix = "_series_matrix.txt.gz"

    resource = None
    resource_dir = get_resource_dir(gse, resource_details)
    
    file_single = "%s%s" % (gse, file_suffix)
    file_multiple = "%s-%s%s" % (gse, gpl, file_suffix)
    
    resource_single = "%s%s" % (resource_dir, file_single)
    resource_multiple = "%s%s" % (resource_dir, file_multiple)
    
    files = get_ftp_files(resource_dir)
    # print(files)
    if resource_single in files:
        resource = resource_single
    elif resource_multiple in files:
        resource = resource_multiple
    else:
        raise Exception("Resource not found in dir %s" % resource_dir)
    print(resource)
    get_data_stream_from_resource(resource, data_processor)

#series are <gse>_series_matrix.txt.gz if only one platform associated
#otherwise <gse>-<gpl>_series_matrix.txt.gz







def retr_data(resource, stream, blocksize, term_flag):

    def data_cb(data):
        #check if should terminate
        if(term_flag.is_set()):
            #end data stream
            stream.end_of_data()
            #terminate thread
            sys.exit()
        stream.write(data)

    ftp.retrbinary("RETR %s" % resource, data_cb, blocksize = blocksize)
    stream.end_of_data()


def get_data_stream_from_resource(resource, data_processor):

    

    # uri = "%s%s" % (ftp_base, resource)

    # size = ftp.size(resource)
    # print(size)
    # print(resource)

    # stream = io.BytesIO()

    # dc = zlib.decompressobj()

    
    # raw = io.BytesIO()
    
    # writer = raw
    # reader = io.BytesIO(writer.getbuffer())

    #256KB starting buffer
    stream = CircularRWBuffer(262144)
    # stream = io.BytesIO()


    # def decompress_and_stream(chunk):
    #     decompressed = dc.decompress(chunk)
    #     stream.write(decompressed)

    #perform in thread since network io
    
    # ftp.retrbinary("RETR %s" % resource, stream.write, blocksize = 4096)

    term_flag = threading.Event()
    
    #daemon thread stops extra data after table from tying up process
    #better way to do this? do we need resource cleanup?
    t = threading.Thread(target = retr_data, args = (resource, stream, 4096, term_flag), daemon = True)
    t.start()

    # retr_data(resource, stream, 4096)


    # with gzip.open(stream, mode='rt') as f:
    #     for row in csv.reader(f, delimiter = "\t"):
    #         temp = row
    #         print(row)
    #         # continue
    start = time.time()
    data_processor(stream)
    term_flag.set()
    end = time.time()
    print(end - start)

    

    #might have data after finished reading table, should end process and add some sort of stream cleanup
    

    # for chunk in stream:
    #     print(chunk)

    # reader = io.BufferedReader(stream)
    # print(reader.read(100))

    
    

    # gz = gzip.GzipFile(fileobj = reader, mode = "rb")
    # for line in gz:
    #     print(line)

    
    # stream.seek(0)
    # writer.tell()
    #gz = gzip.GzipFile(fileobj = stream, mode = "rb")
    

    # stream.seek(0)
    
    # gz = gzip.GzipFile(fileobj = stream, mode = "rb")
    # stream.seek(0)
    # for line in gz:
    #     print(line)
        
def comp_test(accession):
    start = time.time()
    gpl_data = GEOparse.get_GEO(geo = accession, destdir = "./cache", silent = True)
    table = gpl_data.table
    for line in table:
        #just for minimal processing overhead so not optimized away (can python even do that?), should never trigger
        if(len(line) == 10000000000):
            print("loooooong")
    end = time.time()
    print(end - start)
    
def test_driver():
    s = b"the quick brown fox jumps over the lazy dog"

    stream = CircularRWBuffer(10)

    stream.write(s[0:30])
    # stream.write(s[10:20])
    print(stream.read(2))
    print(stream.read())
    # stream.write(s[20:30])
    #deadlock if block true and no timeout
    print(stream.read(500, True, 10))
    stream.write(s[30:])
    stream.end_of_data()
    print(stream.read(500))
    
    # print(stream.read(100))
    # stream.write(s[0:4])
    # print(stream.read())
    # stream.write(s[4:10])
    # stream.write(s[10:12])
    # print(stream.read(2))
    # stream.write(s[12:15])
    # print(stream.read(2))
    # stream.write(s[15:])
    # print(stream.read())
    # stream.write(s[0:7])
    # print(stream.read())
    # stream.write(s[7:15])
    # stream.write(s[15:])
    # print(stream.read())



#test_driver()
# acc = "GPL5275"
# process_acc(acc, "platform")
# comp_test(acc)




# class RWstream:

#     def __init__(self, buffer_size = ):
#         writer = io.BytesIO()
#         reader = io.BufferedReader(writer)


#     def read(self):



#     def write(self, chunk):








# file = ""

# with gzip.open(file, "r") as gz:
#     f = io.BufferedReader(gz)
#     for line in f:
#         line = line.strip()
#         if line == "!platform_table_begin":
#             break
#     reader = csv.reader(f, delimiter='\t')
#     for row in reader:
#         if row[0] == "!platform_table_end":
#             break
#         print(row)