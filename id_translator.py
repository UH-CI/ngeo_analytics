from Bio import Entrez
from threading import Semaphore, Timer



class EntrezRequestHandler:
    

    __max_requests_per_time_no_api = 3
    __max_requests_per_time_api = 10
    __throttle_time_in_seconds = 1

    def __init__(self, email, tool_name, api_key = None):
        
        Entrez.email = email
        Entrez.tool = tool_name
        if api_key is not None:
            Entrez.api_key = api_key
            self.__max_requests_per_time = EntrezRequestHandler.__max_requests_per_time_api
        else:
            self.__max_requests_per_time = EntrezRequestHandler.__max_requests_per_time_no_api

        self.__throttle_semaphore = Semaphore(self.__max_requests_per_time)


    def submit_entrez_request(self, req_type, *args, **kwargs):
        self.__throttle_semaphore.acquire()
        req_funct = getattr(Entrez, req_type)
        handle = req_funct(*args, **kwargs)
        release_timer = Timer(EntrezRequestHandler.__throttle_time_in_seconds, self.__throttle_semaphore.release)
        release_timer.start()
        return handle

    # def read_xml_from_req_handle(self, handle):
    #     return Entrez.read(handle)



class AccessionTranslator:

    def __init__(self, entrez_request_handler):
        self.__entrez_request_handler = entrez_request_handler



    def translate_ids(self, id_list):
        result_list = []

        for search_id in id_list:
            data_list = []

            handle = self.__entrez_request_handler.submit_entrez_request("esearch", "gene", term = search_id, retmode = "xml")

            res = Entrez.read(handle)

            gene_ids = res["IdList"]

            # print(gene_ids)

            for i in range(len(gene_ids)):

                gene_id = res["IdList"][i]

                handle = self.__entrez_request_handler.submit_entrez_request("efetch", "gene", id = gene_id, retmode = "xml")

                # with open("res.xml", "w") as f:
                #     f.write(handle.read())

                res = Entrez.read(handle)[0]
                # print(res["GBSeq_moltype"])
                # for item in res:
                #     print(item)
                # exit()


                #print(res["Entrezgene_gene"])

                gene_data = res["Entrezgene_gene"]["Gene-ref"]
                # print(gene_data.keys())

                data = {
                    "gene_symbol": gene_data.get("Gene-ref_locus"),
                    "gene_synonyms": gene_data.get("Gene-ref_syn"),
                    "gene_description": gene_data.get("Gene-ref_desc")
                }

                data_list.append(data)

            result_list.append(data_list)


        return result_list




# data = {
#     "gene": res["Entrezgene_gene"]
# }

# print(data["gene"])


# for item in res:
#     print(item)

# with open("res.xml", "w") as f:
#     f.write(data.read())

# res = Entrez.read(data)

# for item in res[0]:
#     print(item)

#api_key = 3b60a5d390e42976d38ba5139892c9e12c08 
        # Entrez.email = "mcleanj@hawaii.edu"
        # Entrez.tool = "acc_id_translator"
def main():
    req_handler = EntrezRequestHandler("mcleanj@hawaii.edu", "acc_id_translator", "3b60a5d390e42976d38ba5139892c9e12c08")
    translator = AccessionTranslator(req_handler)
    data = translator.translate_ids(["n70041"])
    print(len(data))
    for l in data:
        print(l)

if __name__ == "__main__":
    main()