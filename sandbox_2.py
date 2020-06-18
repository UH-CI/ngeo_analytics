
import sqlite3 as sql
from sqlite3 import Error

import os
os.environ["R_HOME"] = "E:/R/R-3.6.3" #path to your R installation
os.environ["R_USER"] = "C:/Users/Jard/AppData/Local/Programs/Python/Python35/Lib/site-packages/rpy2"

from rpy2.robjects import r as R
#note, requires numpy <= 1.16.4 to work with newest windows rpy2 version
from rpy2.robjects import pandas2ri




dbf = "E:/ncbigeo/GEOmetadb.sqlite"
cache = "E:/ncbigeo/"



def create_connection(db_file):
    con = None
    try:
        con = sql.connect(db_file)
    except Error as e:
        print(e)
    return con


def get_data_table_by_id(id, cache):
    id_type = id[0:3].lower()
    R.assign("id", id)
    R.assign("id_type", id_type)
    R.assign("cache", cache)
    R("""
        library(GEOquery)
        data = getGEO(id, destdir=cache)
    """)
    data = R("Table(data)")
    data = pandas2ri.ri2py(data)
    return data


con = create_connection(dbf)

if con:
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    cur = con.cursor()

    cur.execute("""
                SELECT gpl, value_type
                FROM gds
                WHERE sample_type == "RNA"
            """)

    res = cur.fetchall()
    base = res[200]
    gpl = base[0]
    value_type = base[1]
    print(gpl)

    data = get_data_table_by_id(gpl, cache)

    print("\n\n\n\n")
    print(value_type)
    print(data.columns)
    #print(data["Gene Info"])

    #how many genes? get gene id, etc and list platforms that use them
    #does all information for similar genes match between platform (platform have other info?)