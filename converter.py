

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

import subprocess







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
    url = "http://ngeo.cts.ci.its.hawaii.edu:8080/api/v1/gene_gpl_ref"
    try:
        res = requests.post(url, data)
    except Exception as e:
        return {
            "success": False,
            "message": e,
            "status": None
        }
    if res.status_code == 201:
        return {
            "success": True,
            "message": res.json(),
            "status": 201
        }

    else:
        return {
            "success": False,
            "message": res.json()["message"],
            "status": res.status_code
        }

def getData(gpl, config_file, cache, retry):
    # raise Exception("test")
    # print("test", file = sys.stderr)
    config = None  
    with open(config_file) as f:
        config = json.load(f)


    gpl_data = GEOparse.get_GEO(geo = gpl, destdir = cache, silent = True)
    
    table = gpl_data.table

    ref_id_col = "ID"
    gene_id_col = platform_processor.get_id_col_and_validate(table)
    if gene_id_col is None:
        print("Warning: No usable ID column found for gpl %s. Skipping..." % gpl)
        return

    for index, row in table.iterrows():
        ref_id = row[ref_id_col]
        gene_id = row[gene_id_col]

        req_handler = id_translator.EntrezRequestHandler(config.get("email"), config.get("tool_name"), config.get("api_token"))
        translator = id_translator.AccessionTranslator(req_handler)
        gene_info = translator.translate_ids([gene_id])[0]

        #add gpl
        if len(gene_info) > 1:
            print("Too many res: %s, %s" % (gpl, gene_id_col))
            exit()
            print("Warning: Returned multiple results for search on gpl %s, gene id %s of type %s. Skipping..." % (gpl, gene_id, gene_id_col))
            continue

        if len(gene_info) < 1:
            print("No res: %s, %s" % (gpl, gene_id_col))
            exit()
            print("Warning: Returned no results for search on gpl %s, gene id %s of type %s. Skipping..." % (gpl, gene_id, gene_id_col))
            continue

        gene_info = gene_info[0]
        print("%s, %s" % (gpl, gene_id_col))
        exit()

        data = {
            "gene_symbol": gene_info.get("gene_symbol"),
            "gene_synonyms": gene_info.get("gene_synonyms"),
            "gene_description": gene_info.get("gene_description"),
            "gpl": gpl,
            "ref_id": ref_id
        }

        print(data)

        res = None
        #+1 for initial try
        for i in range(retry + 1):
            #need gene_name, alt_names, description, platform, id
            res = submit_to_api(data)
            if res["success"]:
                break

        if not res["success"]:
            print("""
                Error: Failed to post data to api:
                gene_symbol: %s, gene_synonyms: %s, gene_description: %s, gpl: %s, ref_id: %s
                %s
            """ % (data["gene_symbol"], data["gene_synonyms"], data["gene_description"], data["gpl"], data["ref_id"], res["message"]), file = sys.stderr)
        
        








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

    config_file = "entrez_config.json" 

    dbf = "E:/ncbigeo/GEOmetadb.sqlite"
    cache = "E:/ncbigeo/"
    p_max = 5
    retry = 3

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


        for ids in res:
            gpl = ids[0]
            p_executor.submit(getData, gpl, config_file, cache, retry)


        p_executor.shutdown(True)


if __name__ == "__main__":
    main()

