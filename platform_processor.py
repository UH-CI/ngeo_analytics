# class PlatformTableHandler:

#     #groupings
#     #

#     #lets start with GB_ACC only then add others, GB_ACC seems the most common


#     #organism on sample, what does it look like if multi-organism?

#     #only using single id fields

#     #will all of these work?
platform_usable_ids = ["GB_ACC", "GI", "CLONE_ID", "ORF", "GENOME_ACC", "SNP_ID", "miRNA_ID", "PT_ACC", "PT_GI", "SP_ACC"]

required = ["ID"]



# def __init__(self, table):
#     self.table = table

#return None if invalid or no standard field in list of usable ids found
def get_id_col_and_validate(table):
    gene_id_col = None
    for field in required:
        if field not in table.columns:
            return None
    for field in platform_usable_ids:
        if field in table.columns:
            gene_id_col = field
            break

    return gene_id_col




















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