import csv
import io
import gzip
import sqlite3
import math
import ftplib
from ftp_manager import FTPManager
import ftp_downloader
from ftp_handler import FTPHandler



class FTPHandler:
    def __init__(self, ftp_base, ftp_opts):
        self.manager = FTPManager(ftp_base, **ftp_opts)


    def parse_data_table_gz(self, file, table_start, table_end, row_processor):
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




    def data_processor(self, table_start, table_end, row_handler):
        def _data_processor(file):
            self.parse_data_table_gz(file, table_start, table_end, row_handler)
        return _data_processor

    #IF HAVING ISSUES WITH EXCEEDING RETRY LIMIT SHOULD JUST ADD METHOD TO RECONNECT CONNECTION BEING USED AND USE SAME ONE RATHER THAN GETTING ANOTHER FROM THE POOL
    #OR CAN TRY INCREASE HEARTRATE

    #shouldn't need a delay on retry, just getting another connection from the pool
    def process_gpl_data(self, gpl, row_handler, retry):
        if retry < 0:
            raise RuntimeError("A connection error has occured. Could not get FTP data")
        ftp_con = self.manager.get_con()
        ftp = ftp_con.ftp
        problem = False
        table_start = "!platform_table_begin"
        table_end = "!platform_table_end"
        try:
            ftp_downloader.get_gpl_data_stream(ftp, gpl, self.data_processor(table_start, table_end, row_handler))
        except ftplib.all_errors as e:
            problem = True
        except Exception as e:
            #issue with resource info (or some edge case in NCBI, but let's pretend that won't happen)
            #shouldn't actually be a problem with the connection
            self.manager.release_con(ftp_con, problem)
            raise ValueError(e)
        self.manager.release_con(ftp_con, problem)
        #check if there was a problem and retry with a new connection if there was
        if problem:
            self.process_gpl_data(gpl, retry - 1)


