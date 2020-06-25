# class PlatformTableHandler:

#     #groupings
#     #

#     #lets start with GB_ACC only then add others, GB_ACC seems the most common


#     #organism on sample, what does it look like if multi-organism?

#     #only using single id fields

#     #will all of these work?


import re


#might have to be accession
#main_ids = ["GB_ACC", "GI", "CLONE_ID", "ORF", "GENOME_ACC", "SNP_ID", "miRNA_ID", "PT_ACC", "PT_GI", "SP_ACC"]

#will swiss protein work the same as the normal protein ids?
#RANGE_GB uses aux fields to show range, id should just be accession with version
single_ids = ["GB_ACC", "PT_ACC", "RANGE_GB", "GI", "PT_GI"]

range_ids = ["GB_RANGE", "GI_RANGE"]

list_ids = ["GB_LIST", "PT_LIST", "GI_LIST", "PT_GI_LIST"]

#match accession fields first to avoid unnecessary translations
acceptable_fields_preferred = ["GB_ACC", "PT_ACC", "RANGE_GB", "GB_RANGE", "GB_LIST", "PT_LIST"]
acceptable_fields = ["GI", "PT_GI", "GI_RANGE", "GI_LIST", "PT_GI_LIST"]

nuc_trans = ["GI", "GI_RANGE", "GI_LIST"]
pt_trans = ["PT_GI", "PT_GI_LIST"]

#map field to pipeline


#other_standard_fields = []

required = ["ID"]





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


parse_map = {}


def generate_parse_map():
    for item in single_ids:
        parse_map[item] = strip_version
    for item in list_ids:
        parse_map[item] = get_single_from_list
    for item in range_ids:
        parse_map[item] = strip_range


generate_parse_map()







# def __init__(self, table):
#     self.table = table

#return None if invalid or no standard field in list of usable ids found
def get_id_cols_and_validate(table):
    gene_id_cols = []
    for field in required:
        if field not in table.columns:
            return None
    #push preferred fields first
    for field in table.columns:
        if field in acceptable_fields_preferred:
            gene_id_cols.append(field)

    for field in table.columns:
        if field in acceptable_fields:
            gene_id_cols.append(field)

    return gene_id_cols

#python doesn't have switch statements, what a beautiful beautiful language
def parse_id_col(value, col):
    parser = parse_map.get(col)
    if parser is None:
        return None

    return parser(value)

def translate_to_acc(value, col, translator):
    if col in nuc_trans:
        return translator.get_gb_acc_from_nuc_id(value)

    if col in pt_trans:
        return translator.get_gb_acc_from_pt_id(value)

    return value

    
    




















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