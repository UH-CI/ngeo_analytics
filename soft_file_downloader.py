import requests
import io
import gzip
import ftplib
import threading
import zlib
import csv
import soft_parser
import time


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


    def read(self, read_size = None, block = True):

        #block and wait for data if none present, block is true, and data not finished
        if block and not self.complete:
            self.data_block.wait()

        #a lot of things mess with state consistency, so just lock everything
        self.access_lock.acquire()

        #data is finished and all read, return empty
        if self.complete and self.size == 0:
            self.access_lock.release()
            return bytes()
        
        if read_size is None or read_size > self.size:
            read_size = self.size
        
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
    }
}


ftp = ftplib.FTP(ftp_base)
ftp.login()


def retr_data(resource, stream, blocksize):
    ftp.retrbinary("RETR %s" % resource, stream.write, blocksize = blocksize)
    stream.end_of_data()

def process_platform(accession, acc_type):

    acc_prefix = type_details[acc_type]["acc_prefix"]
    resource_base = type_details[acc_type]["resource_base"]
    resource_suffix = type_details[acc_type]["resource_suffix"]
    file_suffix = type_details[acc_type]["file_suffix"]

    resource_id = accession[3:]
    id_len = len(resource_id)
    resource_prefix = ""
    if id_len > 3:
        resource_prefix = resource_id[:-3]
    resource_dir = "%s%snnn/%s/" % (acc_prefix, resource_prefix, accession)
    fname = "%s%s" % (accession, file_suffix)

    resource = "%s%s%s%s" % (resource_base, resource_dir, resource_suffix, fname)

    # uri = "%s%s" % (ftp_base, resource)

    size = ftp.size(resource)
    print(size)
    print(resource)

    # stream = io.BytesIO()

    # dc = zlib.decompressobj()

    
    # raw = io.BytesIO()
    
    # writer = raw
    # reader = io.BytesIO(writer.getbuffer())

    #edge case issue? looks like problem if buffer init size equal to chunk size or small multiple
    #probably an issue if buffer exactly full
    stream = CircularRWBuffer(1024)
    # stream = io.BytesIO()


    # def decompress_and_stream(chunk):
    #     decompressed = dc.decompress(chunk)
    #     stream.write(decompressed)

    #perform in thread since network io
    
    # ftp.retrbinary("RETR %s" % resource, stream.write, blocksize = 4096)
    
    #daemon thread stops extra data after table from tying up process
    #better way to do this? do we need resource cleanup?
    t = threading.Thread(target = retr_data, args = (resource, stream, 2048), daemon = True)
    t.start()

    # retr_data(resource, stream, 4096)


    # with gzip.open(stream, mode='rt') as f:
    #     for row in csv.reader(f, delimiter = "\t"):
    #         temp = row
    #         print(row)
    #         # continue
    start = time.time()
    soft_parser.parse_soft_gz(stream)
    end = time.time()
    print(end - start)

    #might have data after finished reading table, should end process and add some sort of stream cleanup
    # t.join()
    # print("joined")
    

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
        

    
def test_driver():
    s = b"the quick brown fox jumps over the lazy dog"

    stream = CircularRWBuffer(10)

    stream.write(s[0:4])
    print(stream.read())
    stream.write(s[4:10])
    stream.write(s[10:12])
    print(stream.read(2))
    stream.write(s[12:15])
    print(stream.read(2))
    stream.write(s[15:])
    print(stream.read())
    # stream.write(s[0:7])
    # print(stream.read())
    # stream.write(s[7:15])
    # stream.write(s[15:])
    # print(stream.read())



#test_driver()
process_platform("GPL7", "platform")




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