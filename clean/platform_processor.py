from sqlalchemy import text
from time import sleep
import re
import sqlite3
from ftp_downloader import ResourceNotFoundError

def submit_db_batch(connector, batch, retry):
    if len(batch) > 0:
        query = text("REPLACE INTO gene_gpl_ref_new (gpl, ref_id, gene_id) VALUES (:gpl, :ref_id, :gene_id)")
        connector.engine_exec(query, batch, retry)

                

#log gpl failures, errors, and no translations
def handle_gpl(connector, gpl, g2a_db, ftp_handler, db_retry, ftp_retry, batch_size):
    batch = [] 
    
    #(id_col, [translation_cols])
    header_info = None
    header = None
    #super fast check, return (boolean include, boolean continue)
    def check_include_continue(row):
        nonlocal header_info
        nonlocal header
        if header is None:
            header = row
            header_info = get_id_cols_and_validate(header)
            #no id column or translatable columns were found, return continue false to terminate reading
            if header_info[0] is None or len(header_info[1]) == 0:
                return (False, False)
            else:
                return (False, True)
        else:
            return (True, True)

    def handle_row(row):
        nonlocal batch
        nonlocal header_info
        nonlocal header

        gene_id = get_gene_id_from_row(header, row, header_info[1], g2a_db)
        ref_id = row[header_info[0]]
        #check if a gene_id was found
        if gene_id is not None:
            #process row into field ref dict
            fields = {
                "gpl": gpl,
                "ref_id": ref_id,
                "gene_id": gene_id
            }
            batch.append(fields)
            if len(batch) % batch_size == 0:
                #just let errors be handled in thread executor callback, if a single batch fails just redo the whole platform for simplicity sake
                submit_db_batch(connector, batch, db_retry)
                batch = []


    #let errors be handled by callee (log error and don't mark gpl as processed)
    #note that some of the unprocessed gpls may just not exist, can check those after
    try:
        ftp_handler.process_gpl_data(gpl, check_include_continue, handle_row, ftp_retry)
    #if a resource not found error was raised then the resource doesn't exist on the ftp server, just skip this one
    except ResourceNotFoundError:
        pass
    #submit anything leftover in the last batch
    submit_db_batch(connector, batch, db_retry)

    


    
















#might have to be accession
#main_ids = ["GB_ACC", "GI", "CLONE_ID", "ORF", "GENOME_ACC", "SNP_ID", "miRNA_ID", "PT_ACC", "PT_GI", "SP_ACC"]

#will swiss protein work the same as the normal protein ids?
#RANGE_GB uses aux fields to show range, id should just be accession with version
single_ids = {"GB_ACC", "PT_ACC", "RANGE_GB", "GI", "PT_GI", "GENOME_ACC", "GENE_ID"}

range_ids = {"GB_RANGE", "GI_RANGE"}

list_ids = {"GB_LIST", "PT_LIST", "GI_LIST", "PT_GI_LIST"}

accessions = {"GB_ACC", "PT_ACC", "RANGE_GB", "GENOME_ACC", "GB_LIST", "PT_LIST", "GB_RANGE"}
ids = {"GI", "PT_GI", "GI_RANGE", "GI_LIST", "PT_GI_LIST", "GENE_ID"}


acceptable_fields = {"GENE_ID", "GI", "PT_GI", "GI_RANGE", "GI_LIST", "PT_GI_LIST", "GB_ACC", "PT_ACC", "RANGE_GB", "GB_RANGE", "GB_LIST", "PT_LIST", "GENOME_ACC"}

# nuc_trans = ["GI", "GI_RANGE", "GI_LIST"]
# pt_trans = ["PT_GI", "PT_GI_LIST"]

#map field to pipeline


#other_standard_fields = []

required = "ID"





def get_single_from_list(acc_list):
    #lists are split on commas or spaces
    first = re.split(",| ", acc_list)[0]
    return strip_version(first)

#if no version should still work
def strip_version(acc):
    return acc.split(".")[0]

def strip_range(acc):
    stripped = acc.split("[")[0]
    #make sure no version number attached
    return strip_version(stripped)





def generate_parse_map():
    parse_map = {}
    for item in single_ids:
        parse_map[item] = strip_version
    for item in list_ids:
        parse_map[item] = get_single_from_list
    for item in range_ids:
        parse_map[item] = strip_range
    return parse_map


parse_map = generate_parse_map()

#looks like orf can map to locus_tag in gene_info possibly improperly formatted or gene symbol/alternatives (and maybe more!)

#assumed mappings really hope these are correct
#GENE_ID not a standard field but probably more or less consistent

#looks like
# mature_peptide_accession protein refseq
# protein_accession protein genbank || refseq
# rna_nucleotide_accession nuc genbank
# genomic_nucleotide_accession nuc refseq || genbank
#protein and nucleotide gis should be unique between refseq and genbank types, so shouldn't have an issue with conflicting mappings



def generate_col_map():
    nuc_id = ["genomic_nucleotide_gi", "rna_nucleotide_gi"]
    nuc_acc = ["genomic_nucleotide_accession", "rna_nucleotide_accession"]
    prot_id = ["protein_gi", "mature_peptide_gi"]
    prot_acc = ["protein_accession", "mature_peptide_accession"]
    gene_id = ["gene_id"]

    col_map = {
        "GI": nuc_id,
        "PT_GI": prot_id,
        "GI_RANGE": nuc_id,
        "GI_LIST": nuc_id,
        "PT_GI_LIST": prot_id,
        "GB_ACC": nuc_acc,
        "PT_ACC": prot_acc,
        "RANGE_GB": nuc_acc,
        "GB_RANGE": nuc_acc,
        "GB_LIST": nuc_acc,
        "PT_LIST": prot_acc,
        #documentation says can be genbank or refseq, example and gene database field name seem to suggest nucleotide, so allow nucleotide accession types
        "GENOME_ACC": nuc_acc,
        #non-standard field, but appears to be fairly common and should be standard format
        "GENE_ID": gene_id
    }
    return col_map


col_map = generate_col_map()



# def __init__(self, table):
#     self.table = table

#return None if invalid or no standard field in list of usable ids found
#use field indices for efficiency
def get_id_cols_and_validate(header):
    gene_id_cols = []
    id_col = None
    for i in range(len(header)):
        field = header[i]
        #convert to upper case just in case case convention not followed
        field = field.upper()
        if field in acceptable_fields:
            gene_id_cols.append(i)
        elif field == "ID":
            id_col = i

    return (id_col, gene_id_cols)






def get_gene_id_from_row(header, row, id_col_indices, g2a_db):
    col = None
    value = None
    #get first id col with a value
    #note this assumes if one id col maps to nothing they all do
    #should be reasonable, they should all have the same entry in the gene2accession table
    for i in id_col_indices:
        candidate = row[i]
        #are empty values "" or "-"? Just catch either
        if candidate != "" and candidate != "-":
            value = candidate
            col = header[i]
            #convert to upper case just in case case convention not followed
            col = col.upper()
            break
    #didn't find any mappable values in the row
    if value is None:
        return None
    #parse the value according to the rules set for the column type
    parsed_value = parse_id_col(value, col)
    #map value to gene id
    gene_id = get_gene_id_from_gpl(col, parsed_value, g2a_db)
    return gene_id



#python doesn't have switch statements, what a beautiful beautiful language
def parse_id_col(value, col):
    parser = parse_map.get(col)
    if parser is None:
        return None

    return parser(value)


    
#remember to clean value first
def get_gene_id_from_gpl(gpl_col, value, g2a_db):
    con = sqlite3.connect(g2a_db)
    cur = con.cursor()
    #already have gene id
    if gpl_col == "GENE_ID":
        return value
    db_cols = col_map.get(gpl_col)
    if db_cols is None:
        return None

    def get_where_clause(db_col):
        if gpl_col in accessions:
            return "%s GLOB '%s[.]*'" % (db_col, value)
        else:
            return "%s = '%s'" % (db_col, value)

    where_clauses = []
    for db_col in db_cols:
        where_clauses.append(get_where_clause(db_col))

    query = "SELECT gene_id FROM gene2accession WHERE %s" % " OR ".join(where_clauses)
    cur.execute(query)
    res = cur.fetchone()
    if res is not None:
        #should be a one length list (the gene_id col result)
        res = res[0]
    return res
