import requests
import io
import gzip
import ftplib
import threading
import zlib

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


    

    stream = io.BufferedRandom(io.BytesIO())



    # def decompress_and_stream(chunk):
    #     decompressed = dc.decompress(chunk)
    #     stream.write(decompressed)

    #perform in thread since network io
    t = threading.Thread(target = ftp.retrbinary, args = ("RETR %s" % resource, stream.write,), kwargs = {"blocksize": 4096})
    # ftp.retrbinary("RETR %s" % resource, stream.write, blocksize = 4096)
    t.start()
    print("a")

    for chunk in stream:
        print(chunk)

    # reader = io.BufferedReader(stream)
    # print(reader.read(100))

    
    

    # gz = gzip.GzipFile(fileobj = reader, mode = "rb")
    # for line in gz:
    #     print(line)

    t.join()

    print(stream.read())

    # stream.seek(0)
    
    # gz = gzip.GzipFile(fileobj = stream, mode = "rb")
    # stream.seek(0)
    # for line in gz:
    #     print(line)
        

    





process_platform("GPL6314", "platform")


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