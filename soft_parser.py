import csv
import io
import gzip
import sqlite3
import soft_file_downloader
import math

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



dbf = "E:/ncbigeo/GEOmetadb.sqlite"




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
                #row processor should signal if sone with data
                if not row_processor(header, row):
                    break




def row_handler(data_bank, id_refs):
    id_ref_index = None
    #make a deep copy of id_refs so dont destroy the integrity of the original list
    id_refs_c = [ref for ref in id_refs]

    def _row_handler(header, row):
        nonlocal id_ref_index
        #only need to get the index once
        if id_ref_index is None:
            #should throw error if not found, should always be found
            id_ref_index = header.index("ID_REF")
        if row[id_ref_index] in id_refs_c:
            del id_refs_c[id_ref_index]
            for i in range(len(header)):
                #ignore id_ref col, rest should be samples
                if i != id_ref_index:
                    gsm = header[i]
                    value = row[i]
                    #values should be numeric, check if value invalid or parses to nan and ignore if it does
                    try:
                        value = float(value)
                        if math.isnan(value):
                            raise ValueError()
                    except ValueError:
                        continue
                    values = data_bank.get(gsm)
                    #check if already created gsm entry and make new one if not
                    if values is None:
                        data_bank[gsm] = [value]
                    else:
                        values.append(value)

        return True
    return _row_handler
    

def get_gses_from_gpl(gpl):
    query = "SELECT gse FROM gse_gpl WHERE gpl == '%s'" % gpl

    def get_single(cursor, row):
        return row[0]

    con = sqlite3.connect(dbf)
    con.row_factory = get_single
    cur = con.cursor()
    cur.execute(query)

    res = cur.fetchall()

    return res


def data_processor(data_bank, id_refs):
    def _data_processor(file):
        parse_data_table_gz(file, "!series_matrix_table_begin", "!series_matrix_table_end", row_handler(data_bank, id_refs))
    return _data_processor

data = {}
gpl = "GPL96"
id_refs = ["90265_at"]
gpl_data = {}
data[gpl] = gpl_data
gses = get_gses_from_gpl(gpl)
# print(len(gses))
# exit()
# gse = gses[0]
for gse in gses:
    gse_data = {}
    gpl_data[gse] = gse_data
    soft_file_downloader.get_gse_data_stream(gse, gpl, data_processor(gse_data, id_refs))
    # break
print(data)

#"!series_matrix_table_begin", "!series_matrix_table_end"
#"!platform_table_begin", "!platform_table_end"


#group by gpl for value ref queries
#possible to have multiple records for gene symbol for gpl (different gene ids)
#multiple values per series (one for each id_ref)
#returned data hierarchy gpl,gse,gsm,[values]

#note, can use streaming content
#https://flask.palletsprojects.com/en/1.1.x/patterns/streaming/
