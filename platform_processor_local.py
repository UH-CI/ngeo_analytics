# class PlatformTableHandler:

#     #groupings
#     #

#     #lets start with GB_ACC only then add others, GB_ACC seems the most common


#     #organism on sample, what does it look like if multi-organism?

#     #only using single id fields

#     #will all of these work?


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
def get_id_cols_and_validate(table):
    gene_id_cols = []
    found_id = False

    for field in table.columns:
        if field in acceptable_fields:
            gene_id_cols.append(field)
        elif field == "ID":
            found_id = True

    return None if not found_id else gene_id_cols


def get_gene_info_from_row(row, id_cols):
    col = None
    value = None
    #get first id col with a value
    for col in id_cols:
        value = row.get(col)
        #apparently empty fields come out as NaN, use string conversion to prevent type errors, and keep None check just in case
        #note sql empty fields come out as None, this is for the GEOparse package stuff
        if str(value) != "nan" and value is not None:
            break
    #didn't find any mappable values in the row
    if str(value) == "nan" or value is None:
        return None

    parsed_value = parse_id_col(value, col)
    gene_id = get_gene_id_from_gpl(col, parsed_value)
    #no mapping
    if gene_id is None:
        return None
    gene_info = get_gene_info_from_id(gene_id)

    return gene_info



#python doesn't have switch statements, what a beautiful beautiful language
def parse_id_col(value, col):
    parser = parse_map.get(col)
    if parser is None:
        return None

    return parser(value)


def get_gene_info_from_id(gene_id):
    query = "SELECT gene_symbol, synonyms, description FROM gene_info WHERE gene_id == %s" % gene_id
    cur.execute(query)
    #transform to dict
    res = cur.fetchone()
    if res is None:
        return None
    res_dict = {
        "gene_symbol": res[0],
        "gene_synonyms": res[1],
        "gene_description": res[2]
    }
    return res_dict
    
#remember to clean value first
def get_gene_id_from_gpl(gpl_col, value):
    #already have gene id
    if gpl_col is "GENE_ID":
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
        print(query)
        #should be a one length list (the gene_id col result)
        res = res[0]
    return res


















# platform_standard_fields = {
#     "ID": {
#         "required": True,
#         "associated_fields": []
#     },
#     "SEQUENCE": {
#         "required": False,
#         "associated_fields": []
#     },
#     "GB_ACC": {
#         "required": False,
#         "associated_fields": []
#     },
#     "GB_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "GB_RANGE": {
#         "required": False,
#         "associated_fields": []
#     },
#     "RANGE_GB": {
#         "required": False,
#         "associated_fields": []
#     },
#     "RANGE_START": {
#         "required": False,
#         "associated_fields": []
#     },
#     "RANGE_END": {
#         "required": False,
#         "associated_fields": []
#     },
#     "RANGE_STRAND": {
#         "required": False,
#         "associated_fields": []
#     },
#     "GI": {
#         "required": False,
#         "associated_fields": []
#     },
#     "GI_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "GI_RANGE": {
#         "required": False,
#         "associated_fields": []
#     },
#     "CLONE_ID": {
#         "required": False,
#         "associated_fields": []
#     },
#     "CLONE_ID_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "ORF": {
#         "required": False,
#         "associated_fields": []
#     },
#     "ORF_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "GENOME_ACC": {
#         "required": False,
#         "associated_fields": []
#     },
#     "SNP_ID": {
#         "required": False,
#         "associated_fields": []
#     },
#     "SNP_ID_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "miRNA_ID": {
#         "required": False,
#         "associated_fields": []
#     },
#     "miRNA_ID_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "SPOT_ID": {
#         "required": False,
#         "associated_fields": []
#     },
#     "ORGANISM": {
#         "required": False,
#         "associated_fields": []
#     },
#     "PT_ACC": {
#         "required": False,
#         "associated_fields": []
#     },
#     "PT_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "PT_GI": {
#         "required": False,
#         "associated_fields": []
#     },
#     "PT_GI_LIST": {
#         "required": False,
#         "associated_fields": []
#     },
#     "SP_ACC": {
#         "required": False,
#         "associated_fields": []
#     },
#     "SP_LIST": {
#         "required": False,
#         "associated_fields": []
#     }
# }