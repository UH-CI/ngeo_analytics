
import id_translator
import json

config = None  
with open(config_file) as f:
    config = json.load(f)

#with version number, looks like only newest version works
#works without version number (probably maps to the newest version)
#assume newer versions should map to the same gene as older versions


#api_key = 3b60a5d390e42976d38ba5139892c9e12c08 
        # Entrez.email = "mcleanj@hawaii.edu"
        # Entrez.tool = "acc_id_translator"
def main():
    req_handler = id_translator.EntrezRequestHandler(config.get("email"), config.get("tool_name"), config.get("api_token"))
    translator = id_translator.AccessionTranslator(req_handler)
    data = translator.translate_ids(["NM_016951"])
    print(len(data))
    for l in data:
        print(l)

if __name__ == "__main__":
    main()