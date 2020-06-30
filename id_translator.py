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

#here have field param
#handle list separation and stripping in field

class PlatformFieldTranslator:

    def __init__(self, entrez_request_handler):
        self.__entrez_request_handler = entrez_request_handler


    def get_gb_acc_from_nuc_id(self, gi):
        handle = self.__entrez_request_handler.submit_entrez_request("efetch", "nuccore", id = gi, retmode = "xml")

        record = Entrez.read(handle)[0]

        return str(record.get("GBSeq_locus"))

    def get_gb_acc_from_pt_id(self, pt_gi):
        handle = self.__entrez_request_handler.submit_entrez_request("efetch", "protein", id = pt_gi, retmode = "xml")

        record = Entrez.read(handle)[0]

        return str(record.get("GBSeq_locus"))


    def translate_acc(self, acc):
        result_list = []

        handle = self.__entrez_request_handler.submit_entrez_request("esearch", "gene", "%s[ACCN]" % acc, retmode = "xml")

        res = Entrez.read(handle)

        gene_ids = res.get("IdList")
        if gene_ids is None:
            raise Exception("Entrez esearch returned invalid result. No IdList field.")

        for i in range(len(gene_ids)):

            gene_id = gene_ids[i]

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
            #cast types from parser special types to normal for ipc pickling
            if data["gene_symbol"] is not None:
                data["gene_symbol"] = str(data["gene_symbol"])
            if data["gene_description"] is not None:
                data["gene_description"] = str(data["gene_description"])
            if data["gene_synonyms"] is not None:
                data["gene_synonyms"] = [str(item) for item in data["gene_synonyms"]]

            result_list.append(data)


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



