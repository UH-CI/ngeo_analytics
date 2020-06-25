from Bio import Entrez
import id_translator
import json

config_file = "entrez_config.json"

config = None  
with open(config_file) as f:
    config = json.load(f)

# 23337084
# BC037416
search_id = "1789480"

# #use accession field, for ids need to translate first, feed to nuccore, get accession, then feed back
# #protein accs should also work based on docs
# #https://www.ncbi.nlm.nih.gov/books/NBK3841/table/EntrezGene.T.fields_used_to_categorize_i/

# request_handler = id_translator.EntrezRequestHandler(config.get("email"), config.get("tool_name"), config.get("api_token"))

# handle = request_handler.submit_entrez_request("esearch", "gene", term = "%s[ACCN]" % search_id, retmode = "xml")

# res = Entrez.read(handle)
# print(res)
# gene_ids = res["IdList"]

# # print(gene_ids)

# for i in range(len(gene_ids)):

#     gene_id = res["IdList"][i]

#     handle = request_handler.submit_entrez_request("efetch", "gene", id = gene_id, retmode = "xml")

#     record = Entrez.read(handle)[0]

#     for item in record:
#         print(item)
#         #print(res[item])

#     with open("record.txt", "w") as f:
#         json.dump(record, f, indent = 4)

#     # #print(res["Entrezgene_locus"][0])

#     # for item in res["Entrezgene_locus"][0]:
#     #     print(item)

#     # print(res["Entrezgene_locus"][0]["Gene-commentary_products"])


#range_gb should just be accession, ignore start, end
#clone db no longer exists

def get_gb_acc_from_nuc_id(gi):
    request_handler = id_translator.EntrezRequestHandler(config.get("email"), config.get("tool_name"), config.get("api_token"))

    handle = request_handler.submit_entrez_request("efetch", "nuccore", id = gb_id, retmode = "xml")

    record = Entrez.read(handle)[0]

    return record["GBSeq_locus"]

def get_pt_acc_from_pt_id(pt_gi):
    request_handler = id_translator.EntrezRequestHandler(config.get("email"), config.get("tool_name"), config.get("api_token"))

    handle = request_handler.submit_entrez_request("efetch", "protein", id = pt_gi, retmode = "xml")

    record = Entrez.read(handle)[0]

    return record["GBSeq_locus"]


print(get_pt_acc_from_pt_id(search_id))