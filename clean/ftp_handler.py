import csv
import io
import gzip
import sqlite3
import math
import ftplib
from ftp_manager import FTPManager
import ftp_downloader



class FTPHandler:
    def __init__(self, ftp_base, ftp_pool_size, ftp_opts):
        self.manager = FTPManager(ftp_base, ftp_pool_size, **ftp_opts)


    def parse_data_table_gz(self, file, table_start, table_end, row_processor):
        with gzip.open(file, "rt", encoding = "utf8", errors = "ignore") as f:
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

    #shouldn't need a delay on retry, just getting another connection from the pool
    def process_gpl_data(self, gpl, row_handler, retry):
        self.__process_gpl_data_r(gpl, row_handler, retry)
        

    
    def __process_gpl_data_r(self, gpl, row_handler, retry, ftp_con = None):
        if retry < 0:
            if ftp_con is not None:
                #release connection, there may be an issue with it, but it's not my problem anymore (should be picked up by heartbeat or something)
                self.manager.return_con(ftp_con)
            #raise retry limit exceeded error
            raise RuntimeError("A connection error has occured. Could not get FTP data")

        #if no connection provided get a new one
        if ftp_con is None:
            ftp_con = self.manager.get_con()
        #otherwise reconnect provided connection (failed in last iter)
        else:
            ftp_con = self.manager.reconnect(ftp_con) 

        ftp = ftp_con.ftp
        table_start = "!platform_table_begin"
        table_end = "!platform_table_end"
        try:
            ftp_downloader.get_gpl_data_stream(ftp, gpl, self.data_processor(table_start, table_end, row_handler))
            self.manager.return_con(ftp_con)
        #problem with connection
        except ftplib.all_errors as e:
            #retry
            self.__process_gpl_data_r(gpl, row_handler, retry - 1, ftp_con)
        #probably an issue with resource info or gpl/resource does not exist
        #shouldn't actually be a problem with the connection, assumes error was not in return_con call
        except Exception as e:
            self.manager.return_con(ftp_con)
            raise e

    
    def dispose(self):
        self.manager.dispose()


