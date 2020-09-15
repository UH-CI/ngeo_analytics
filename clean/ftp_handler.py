import csv
import io
import gzip
import sqlite3
import math
import ftplib
import ftp_manager
import ftp_downloader
# file = "./cache/GPL5.txt"

# with open(file, "r") as f:
#     for line in f:
#         line = line.strip()
#         if line == "!platform_table_begin":
#             break
#     reader = csv.reader(f, delimiter='\t')
#     for row in reader:
#         if row[0] == "!platform_table_end":
#             break
#         print(row)



#retry times if something wrong with ftp connection
RETRY = 5

ftp_base = "ftp.ncbi.nlm.nih.gov"
#should set this
heartbeat_threads = 3
manager = ftp_manager.FTPManager(ftp_base, heartbeat_threads = heartbeat_threads)



def parse_data_table_gz(file, table_start, table_end, row_processor):
    with gzip.open(file, "rt", encoding = "utf8") as f:
        #read past header data to table
        for line in f:
            line = line.strip()
            if line == table_start:
                break
        reader = csv.reader(f, delimiter='\t')
        header = None
        for row in reader:
            #apparently some might have extra lines? just ignore
            if len(row) == 0:
                continue
            if row[0] == table_end:
                break
            #haven't gotten header yet, this row should be the header
            if header is None:
                header = row
            else:
                #row processor should signal if done with data
                if not row_processor(header, row):
                    break



# #id_refs might be none, handle that
# def gpl_row_handler(data_bank, id_refs):
#     id_ref_index = None
#     #make a deep copy of id_refs so dont destroy the integrity of the original list
#     id_refs_c = [ref for ref in id_refs]

#     def _row_handler(header, row):
#         nonlocal id_ref_index
#         #only need to get the index once
#         if id_ref_index is None:
#             #should throw error if not found, should always be found
#             id_ref_index = header.index("ID_REF")
#         if row[id_ref_index] in id_refs_c:
#             del id_refs_c[id_ref_index]
#             for i in range(len(header)):
#                 #ignore id_ref col, rest should be samples
#                 if i != id_ref_index:
#                     gsm = header[i]
#                     value = row[i]
#                     #values should be numeric, check if value invalid or parses to nan and ignore if it does
#                     try:
#                         value = float(value)
#                         if math.isnan(value):
#                             raise ValueError()
#                     except ValueError:
#                         continue
#                     values = data_bank.get(gsm)
#                     #check if already created gsm entry and make new one if not
#                     if values is None:
#                         data_bank[gsm] = [value]
#                     else:
#                         values.append(value)

#         return True
#     return _row_handler
    

# def get_gses_from_gpl(gpl):
#     query = "SELECT gse FROM gse_gpl WHERE gpl == '%s'" % gpl

#     def get_single(cursor, row):
#         return row[0]

#     con = sqlite3.connect(dbf)
#     con.row_factory = get_single
#     cur = con.cursor()
#     cur.execute(query)

#     res = cur.fetchall()

#     return res


def data_processor(table_start, table_end, row_handler):
    def _data_processor(file):
        parse_data_table_gz(file, table_start, table_end, row_handler)
    return _data_processor

#IF HAVING ISSUES WITH EXCEEDING RETRY LIMIT SHOULD JUST ADD METHOD TO RECONNECT CONNECTION BEING USED AND USE SAME ONE RATHER THAN GETTING ANOTHER FROM THE POOL
#OR CAN TRY INCREASE HEARTRATE

#shouldn't need a delay on retry, just getting another connection from the pool
def process_gpl_data(gpl, row_handler, retry):
    if retry < 0:
        raise RuntimeError("A connection error has occured. Could not get FTP data")
    ftp_con = manager.get_con()
    ftp = ftp_con.ftp
    problem = False
    table_start = "!platform_table_begin"
    table_end = "!platform_table_end"
    try:
        ftp_downloader.get_gpl_data_stream(ftp, gpl, data_processor(table_start, table_end, row_handler))
    except ftplib.all_errors as e:
        problem = True
    except Exception as e:
        manager.release_con(ftp_con, problem)
        #issue with resource info (or some edge case in NCBI, but let's pretend that won't happen)
        raise ValueError(e)
    manager.release_con(ftp_con, problem)
    #check if there was a problem and retry with a new connection if there was
    if problem:
        process_gpl_data(gpl, retry - 1)


