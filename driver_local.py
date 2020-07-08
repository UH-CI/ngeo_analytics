

import sqlite3 as sql
from sqlite3 import Error

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import sys, os

# #has to be before rpy2 imports
# os.environ["R_HOME"] = "E:/R/R-3.6.3" #path to R installation
# os.environ["R_USER"] = "C:/Users/Jard/AppData/Local/Programs/Python/Python35/Lib/site-packages/rpy2" #python rpy2 package path

# from rpy2.robjects import r as R
# #note, requires numpy <= 1.16.4 to work with newest windows rpy2 version
# from rpy2.robjects import pandas2ri

# from rpy2.robjects.packages import importr

import platform_processor_local
import GEOparse
import json
import id_translator
import requests
import math
import multiprocessing
# from multiprocessing import Manager
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from throttle_manager import CoordManager
from enum import IntEnum
import traceback


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#two files, gpl based, and row based, gpl based list number of errors

class ErrorCodes(IntEnum):
    no_res = 0
    multi_res = 1
    post_err = 2

#a lot of platforms have empty data tables, just ignore those

        

#pipe faster than queue, pipe send, recv, recv blocks until something available
#with pipe need to make one for each process, otherwise can use queue to consume from any process

#get all platform and sample ids
#create two thread pools, one that gets files, one that writes out sql
#listening to queues? add to shared queue when available

#multiprocessing queues should be thread and process safe, and have blocking methods to wait for items

#set up locks


#use event object to signal done (check if queue empty and this is set)

#keep in mind that python multiproces

# global main_stdout
# global nullout
# nullout = None
# main_stdout = sys.stdout
# def supress_stdout(suppress):
#     if(suppress):
#         print("suppressed")
#         nullout = open(os.devnull, "w")
#         sys.stdout = nullout
#         print("?")
#     else:
#         if(nullout != None):
#             nullout.close()
#             nullout = None
#         sys.stdout = main_stdout




def create_connection(db_file):
    con = None
    try:
        con = sql.connect(db_file)
    except Error as e:
        print(e)
    return con



def submit_to_api(data):
    url = "https://ci.its.hawaii.edu/ngeo/api/v1/gene_gpl_ref"
    try:
        headers = {'Content-type': 'application/json'}
        res = requests.post(url, json = data, headers = headers, verify = False)
    except Exception as e:
        return {
            "success": False,
            "message": e,
            "status": None
        }

    if res.status_code == 201:
        return {
            "success": True,
            "message": res.content.decode("utf8"),
            "status": 201
        }

    else:
        return {
            "success": False,
            "message": res.content.decode("utf8"),
            "status": res.status_code
        }


def iterable2csvstr(iterable):
    csvstr = ",".join(iterable)
    # for item in iterable:
    #     csvstr += "%s," % item
    # #guard to make sure string not empty (if list empty)
    # if len(csvstr) > 0:
    #     #remove trailing ,
    #     csvstr = csvstr[:-1]
    return csvstr

def barstr2csvstr(barstr):
    csvstr = barstr.replace('|', ',')
    return csvstr


def submission_handler(retry, data):
    res = None
    #+1 for initial try
    for i in range(retry + 1):
        #need gene_name, alt_names, description, platform, id
        res = submit_to_api(data)
        if res["success"]:
            break

    return res



def write_log(failed, gpl, gene_id_cols, out_file_gpl, out_file_row, success_count, error_count, row_errors, gpl_lock, row_lock):
    with gpl_lock:
        id_cols_str = iterable2csvstr(gene_id_cols)
        with open(out_file_gpl, "a") as f:
            f.write("%s,true,\"%s\",%d,%d,%s\n" % (gpl, id_cols_str, success_count, error_count, failed))

    with row_lock:
        with open(out_file_row, "a") as f:
            for error in row_errors:
                f.write("%s,%d,%d,%s\n" % (gpl, *error))


#"gpl,table_valid,id_cols,successes,errors,ingestion_failed"
#"gpl,row,error_type,message"
def getData(gpl, cache, retry, out_file_gpl, out_file_row, gpl_lock, row_lock, t_max):

    success_count = 0
    error_count = 0
    row_errors = []
    row_futures = []
    gene_id_cols = []

    try:
        gpl_data = GEOparse.get_GEO(geo = gpl, destdir = cache, silent = True)
        
        table = gpl_data.table

        ref_id_col = "ID"
        gene_id_cols = platform_processor_local.get_id_cols_and_validate(table)
        if gene_id_cols is None:
            with gpl_lock:
                with open(out_file_gpl, "a") as f:
                    f.write("%s,false,,,,false\n" % gpl)
            return
        if len(gene_id_cols) == 0:
            with gpl_lock:
                with open(out_file_gpl, "a") as f:
                    f.write("%s,true,\"\",,,false\n" % gpl)
            return

        t_executor = ThreadPoolExecutor(t_max)

        for index, row in table.iterrows():
            
            gene_info = platform_processor_local.get_gene_info_from_row(row, gene_id_cols)
            ref_id = row.get(ref_id_col)
            
            if gene_info is None:
                row_errors.append((index, ErrorCodes.no_res, ""))
                error_count += 1
                continue

            data = {
                "gene_symbol": gene_info.get("gene_symbol"),
                "gene_synonyms": gene_info.get("gene_synonyms"),
                "gene_description": gene_info.get("gene_description"),
                "gpl": gpl,
                "ref_id": ref_id
            }

            #db file contains bar separated style list, replaces with csv style
            if data["gene_synonyms"] is not None:
                data["gene_synonyms"] = barstr2csvstr(data["gene_synonyms"])
            
            future = t_executor.submit(submission_handler, retry, data)
            row_futures.append(future)
            

        #synchronize and get results
        for future in as_completed(row_futures):
            e = future.exception()
            if e is not None:
                #just throw the error, something odd happened since request errors should be caught
                raise e
            res = future.result()

            if not res["success"]:
                ecode = res["status"]
                if ecode is None:
                    ecode = ErrorCodes.post_err
                row_errors.append((index, ecode, res["message"]))
                error_count += 1
            else:
                success_count += 1

        t_executor.shutdown(True)

        write_log("false", gpl, gene_id_cols, out_file_gpl, out_file_row, success_count, error_count, row_errors, gpl_lock, row_lock)
            
    except Exception as e:
        print(traceback.format_exc(), file = sys.stderr)
        write_log("true", gpl, gene_id_cols, out_file_gpl, out_file_row, success_count, error_count, row_errors, gpl_lock, row_lock)
        exit(1)








#key table, provide platform
#table:
#gene_name, alt_names, description, platform, id


#don't need to store samples here, can get from gsm table by matching gpl column
#if ids match can combine, list of gpls with gene to id mapping


#redundant samples possible with this
#from gene type can get platforms -> series -> samples
#create table mapping ids to gene type

#https://warwick.ac.uk/fac/sci/moac/people/students/peter_cock/python/genbank/

def main():

    config_file = "config.json"

    config = None
    with open(config_file) as f:
        config = json.load(f)

    dbf = config.get("dbf")
    cache = config.get("cache")
    p_max = config.get("p_max")
    t_max = config.get("t_max")
    #main process basically idle waiting for children so no need to subtract one for it
    if p_max < 1:
        #make sure at least one in case of issue with count
        p_max = max(multiprocessing.cpu_count(), 1)
    if t_max < 1:
        #subtract 1 because main thread still processing
        #make sure at least one though
        t_max = max(multiprocessing.cpu_count() - 1, 1)
    retry = config.get("retry")
    out_file_gpl = config.get("out_file_gpl")
    out_file_row = config.get("out_file_row")

    with open(out_file_gpl, "w") as f:
        #header for output csv
        f.write("gpl,table_valid,id_cols,successes,errors,ingestion_failed\n")
    with open(out_file_row, "w") as f:
        #header for output csv
        f.write("gpl,row,error_type,message\n")

    con = create_connection(dbf)

    if con:
        con.text_factory = lambda b: b.decode(errors = 'ignore')
        cur = con.cursor()

        #gds only has a curated subset, do w
        # cur.execute("""
        #         SELECT gds.gpl, gds_subset.sample_id
        #         FROM gds JOIN gds_subset ON gds.gds = gds_subset.gds
        #     """)

        #filter out platforms with 0 data rows
        cur.execute("""
                SELECT gpl
                FROM gpl
                WHERE data_row_count > 0
            """)


        res = cur.fetchall()

        #protein_accession
        #protein_gi
        #rna_nucleotide_accession
        #rna_nucleotide_gi
        #note accessions use acc.version, so have to use "LIKE <acc>._%"
        #can add genomic_nucleotide_accession, matching GENOME_ACC, might be included with some orfs
        #doesn't seem to be a standard field for genomic_nucleotide_id
        #what is mature_peptide_accession?



        #start process pool with id queue
        p_executor = ProcessPoolExecutor(p_max)

        with CoordManager() as manager:
            gpl_lock = manager.Lock()
            row_lock = manager.Lock()

            # req_handler = id_translator.EntrezRequestHandler(entrez_config.get("email"), entrez_config.get("tool_name"), entrez_config.get("api_token"))
            # translator = id_translator.PlatformFieldTranslator(req_handler)

            for ids in res:
                gpl = ids[0]
                p_executor.submit(getData, gpl, cache, retry, out_file_gpl, out_file_row, gpl_lock, row_lock, t_max)

            p_executor.shutdown(True)


if __name__ == "__main__":
    main()

