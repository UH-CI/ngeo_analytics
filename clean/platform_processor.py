# class PlatformTableHandler:

#     #groupings
#     #

#     #lets start with GB_ACC only then add others, GB_ACC seems the most common


#     #organism on sample, what does it look like if multi-organism?

#     #only using single id fields

#     #will all of these work?
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, exc
from time import sleep
import random
import ftp_downloader
import ftp_manager

Base = declarative_base()

#primary key should be gpl ref_id
class GPLRef(Base):
    __tablename__="gene_gpl_ref"
    gene_id = Column(String)
    ref_id = Column(String, primary_key=True)
    gpl = Column(String, primary_key=True)

ftp_base = "ftp.ncbi.nlm.nih.gov"
#how many heartbeat threads?
heartbeat_threads = 2
#global manager
manager = ftp_manager.FTPManager(ftp_base, heartbeat_threads = heartbeat_threads)


def submit_db_batch(gpl, engine, batch, retry, delay = 0):
    error = None
    if retry < 0:
        error = Exception("Retry limit exceeded")
    #do nothing if empty list (note batch size limiting handled in caller in this case)
    elif len(batch) > 0:
        sleep(delay)
        try:
            with engine.begin() as con:
                con.execute(GPLRef.__table__.insert(), batch)

        #note duplicated primary key is unacceptable in this case, should never happen (means that gpl table has duplicate ref ids)
        #just group in with default exception handling

        except exc.OperationalError as e:
            #check if deadlock error (code 1213)
            if e.orig.args[0] == 1213:
                backoff = 0
                #if first failure backoff of 0.25-0.5 seconds
                if delay == 0:
                    backoff = 0.25 + random.uniform(0, 0.25)
                #otherwise 2-3x current backoff
                else:
                    backoff = delay * 2 + random.uniform(0, delay)
                #retry with one less retry remaining and current backoff
                error = submit_db_batch(gpl, engine, batch, retry - 1, backoff)
            #something else went wrong, log exception and add to failures
            else:
                error = e
        #catch anything else and return error
        except Exception as e:
            error = e
        return error
        


#log gpl failures, errors, and no translations
def handle_gpl(gpl, engine, batch_size, failure_lock, error_lock, nt_lock):
    global manager
    retry = 5
    batch = []
    #row and header handlers
    def handle_header(header):
        pass
    def handle_row(row):
        nonlocal batch
        #process row into field ref dict
        fields = {}
        batch.append(fields)
        if len(batch) % batch_size == 0:
            error = submit_db_batch(gpl, engine, batch, retry)
            if error is not None:
                with error_lock:
                    #write out items in batch (no need to redo translations etc)
                    pass
            batch = []

    ftp_downloader.get_gpl_data_stream(manager, gpl, )


    







import re
import sqlite3







db_file = "genedb.sqlite"

con = sqlite3.connect(db_file)
cur = con.cursor()


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
    found_id = False
    for i in range(len(header)):
        field = header[i]
        if field in acceptable_fields:
            gene_id_cols.append(i)
        elif field == "ID":
            found_id = True

    return None if not found_id else gene_id_cols






def get_gene_id_from_row(row, id_col_indices):
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
            break
    #didn't find any mappable values in the row
    if value is None:
        return None
    #parse the value according to the rules set for the column type
    parsed_value = parse_id_col(value, col)
    #map value to gene id
    gene_id = get_gene_id_from_gpl(col, parsed_value)
    return gene_id



#python doesn't have switch statements, what a beautiful beautiful language
def parse_id_col(value, col):
    parser = parse_map.get(col)
    if parser is None:
        return None

    return parser(value)


    
#remember to clean value first
def get_gene_id_from_gpl(gpl_col, value):
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
            return "%s == '%s'" % (db_col, value)

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
