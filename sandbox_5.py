import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

res = None
url = "http://ci.its.hawaii.edu/ngeo/api/v1/values"
#url = "http://127.0.0.1:5000/api/v1/gene_gpl_ref"
data = {
    "gene_symbol": "e",
    "gene_synonyms": "d",
    "gene_description": "c",
    "gpl": "b",
    "ref_id": "a"
}
headers = {'Content-type': 'application/json'}
try:
    #res = requests.post(url, json = data, headers = headers, verify = False)
    res = requests.get(url, verify = False)
except Exception as e:
    print(e)
    exit()
print(res.content)