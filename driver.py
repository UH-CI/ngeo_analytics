

import sqlite3 as sql
from sqlite3 import Error

from concurrent.futures import ProcessPoolExecutor

import sys, os

# #has to be before rpy2 imports
# os.environ["R_HOME"] = "E:/R/R-3.6.3" #path to R installation
# os.environ["R_USER"] = "C:/Users/Jard/AppData/Local/Programs/Python/Python35/Lib/site-packages/rpy2" #python rpy2 package path

# from rpy2.robjects import r as R
# #note, requires numpy <= 1.16.4 to work with newest windows rpy2 version
# from rpy2.robjects import pandas2ri

# from rpy2.robjects.packages import importr

import platform_processor

import GEOparse
import json

import id_translator
import requests

import math

from multiprocessing import Manager
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from enum import IntEnum

#two files, gpl based, and row based, gpl based list number of errors

class ErrorCodes(IntEnum):
    no_res = 0
    multi_res = 1

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
        print(e)
        return {
            "success": False,
            "message": e,
            "status": None
        }
    print(res.content)
    if res.status_code == 201:
        print(res)
        return {
            "success": True,
            "message": res.json(),
            "status": 201
        }

    else:
        print(res.json())
        return {
            "success": False,
            "message": res.json()["message"],
            "status": res.status_code
        }

#"gpl,table_valid,id_cols,successes,errors,ingestion_failed"
#"gpl,row,error_type"
def getData(gpl, entrez_config, cache, retry, out_file_gpl, out_file_row, gpl_lock, row_lock):
    try:
        # raise Exception("test")
        # print("test", file = sys.stderr)


        gpl_data = GEOparse.get_GEO(geo = gpl, destdir = cache, silent = True)
        
        table = gpl_data.table

        ref_id_col = "ID"
        gene_id_cols = platform_processor.get_id_cols_and_validate(table)
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

        req_handler = id_translator.EntrezRequestHandler(entrez_config.get("email"), entrez_config.get("tool_name"), entrez_config.get("api_token"))
        translator = id_translator.PlatformFieldTranslator(req_handler)

        def write_out(ingestion_failed):
            with gpl_lock:
                id_cols_str = ""
                for col in gene_id_cols:
                    id_cols_str += "%s," % col
                id_cols_str = id_cols_str[:-1]
                with open(out_file_gpl, "a") as f:
                    f.write("%s,true,\"%s\",%d,%d,%s\n" % (gpl, id_cols_str, success_count, error_count, ingestion_failed))
        
            with row_lock:
                with open(out_file_row, "a") as f:
                    for error in row_errors:
                        f.write("%s,%d,%d\n" % (gpl, *error))

        success_count = 0
        error_count = 0
        row_errors = []

        for index, row in table.iterrows():
            gene_id_col = None
            value = None
            #get first id col with a value
            for gene_id_col in gene_id_cols:
                if row.get(gene_id_col) is not None:
                    value = row[gene_id_col]
                    break
            #apparently empty fields come out as NaN, use string conversion to prevent type errors, and keep None check just in case
            if str(value) == "nan" or value is None:
                continue

            ref_id = row[ref_id_col]
            value = row[gene_id_col]
            parsed = platform_processor.parse_id_col(value, gene_id_col)
            gb_acc = platform_processor.translate_to_acc(parsed, gene_id_col, translator)
            
            gene_info = translator.translate_acc(gb_acc)

            #add gpl
            if len(gene_info) > 1:
                row_errors.append((index, ErrorCodes.multi_res))
                error_count += 1
                continue

            if len(gene_info) < 1:
                row_errors.append((index, ErrorCodes.no_res))
                error_count += 1
                continue

            gene_info = gene_info[0]

            data = {
                "gene_symbol": gene_info.get("gene_symbol"),
                "gene_synonyms": gene_info.get("gene_synonyms"),
                "gene_description": gene_info.get("gene_description"),
                "gpl": gpl,
                "ref_id": ref_id
            }

            res = None
            #+1 for initial try
            for i in range(retry + 1):
                print(i)
                #need gene_name, alt_names, description, platform, id
                res = submit_to_api(data)
                print(res)
                if res["success"]:
                    break

            if not res["success"]:
                write_out("true")
                raise Exception("Error: Failed to post data to api:\ngene_symbol: %s, gene_synonyms: %s, gene_description: %s, gpl: %s, ref_id: %s\n%s" % (data["gene_symbol"], data["gene_synonyms"], data["gene_description"], data["gpl"], data["ref_id"], res["message"]))

            success_count += 1

        write_out("false")
            
    except Exception as e:
        print(e, file = sys.stderr)
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
    retry = config.get("retry")
    out_file_gpl = config.get("out_file_gpl")
    out_file_row = config.get("out_file_row")
    entrez_config = config.get("entrez")

    with open(out_file_gpl, "w") as f:
        #header for output csv
        f.write("gpl,table_valid,id_cols,successes,errors,ingestion_failed\n")
    with open(out_file_row, "w") as f:
        #header for output csv
        f.write("gpl,row,error_type\n")

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


        res = cur.fetchall()[0:10]

        #start process pool with id queue
        p_executor = ProcessPoolExecutor(p_max)

        lock_manager = Manager()
        gpl_lock = lock_manager.Lock()
        row_lock = lock_manager.Lock()

        for ids in res:
            gpl = ids[0]
            p_executor.submit(getData, gpl, entrez_config, cache, retry, out_file_gpl, out_file_row, gpl_lock, row_lock)


        p_executor.shutdown(True)


if __name__ == "__main__":
    main()

