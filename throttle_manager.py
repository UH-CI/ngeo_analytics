from multiprocessing import Manager, Lock
from multiprocessing.managers import BaseManager, AcquirerProxy
from id_translator import EntrezRequestHandler, PlatformFieldTranslator
import json

class CoordManager(BaseManager):
    pass

CoordManager.register("EntrezRequestHandler", EntrezRequestHandler)
CoordManager.register("PlatformFieldTranslator", PlatformFieldTranslator)
CoordManager.register("Lock", Lock, AcquirerProxy)

# config_file = "config.json"

# config = None
# with open(config_file) as f:
#     config = json.load(f)

# entrez_config = config.get("entrez")




# if __name__ == "__main__":
#     with CoordManager() as manager:
#         handler = manager.EntrezRequestHandler(entrez_config.get("email"), entrez_config.get("tool_name"), entrez_config.get("api_token"))
#         trans = manager.PlatformFieldTranslator(handler)
#         lm = manager.Lock()

#         res = trans.get_gb_acc_from_pt_id("1789480")
#         print(res)
#         res2 = trans.translate_acc("AAC76129")
#         print(res2)