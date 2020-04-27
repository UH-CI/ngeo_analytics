import sqlite3 as sql
from sqlite3 import Error
import pandas
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
        SELECT gds.gds, gds_subset.sample_id
        FROM gds JOIN gds_subset ON gds.gds = gds_subset.gds
        WHERE sample_organism = "Homo sapiens"
        GROUP BY gds.gds
        LIMIT 1
    """)
    res = cur.fetchall()
    #use first sample
    gsm_id = res[0][1].split(",")[0]

    #get platform from sample
    cur.execute("""
        SELECT gpl
        FROM gsm
        WHERE gsm == "%s"
    """ % gsm_id)
    res = cur.fetchall()
    #don't need database anymore
    con.close()

    gpl_id = res[0][0]

    #get sample data
    gsm_data = get_data_table_by_id(gsm_id, cache)

    #get platform data
    gpl_data = get_data_table_by_id(gpl_id, cache)

    #join the platform and sample data on the probe id (ID in platform, ID_REF in sample)
    joined_data = pandas.merge(gpl_data, gsm_data, how = "inner", left_on = "ID", right_on = "ID_REF")

    # print(joined_data.columns)
    # print(joined_data.iloc[0])
    # exit()

    gene_id_tag = "Gene Symbol"
    value_tag = "VALUE"

    #limit to gene ids and values
    data_map = joined_data[[gene_id_tag, value_tag]]

    #explode transcript id lists into multiple columns, separated by " /// "
    split_transcript_ids = data_map[gene_id_tag].str.split(" /// ").tolist()

    #create new data frame with each gene id as a separate column with the values as an index
    data_map_expanded = pandas.DataFrame(split_transcript_ids, index = data_map[value_tag])
    #stack all of the gene ids into one column
    data_map_expanded = data_map_expanded.stack()
    #remove value index to expand out values
    data_map_expanded = data_map_expanded.reset_index(0)
    #remove and drop extra index added by stack
    data_map_expanded = data_map_expanded.reset_index(0, drop = True)
    #rename columns
    data_map_expanded.columns = [value_tag, gene_id_tag]
    #order columns
    data_map_expanded = data_map_expanded.reindex(columns = [gene_id_tag, value_tag])

    print(data_map_expanded)