import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

res = None
url = "http://ci.its.hawaii.edu/ngeo/api/v1/values"
try:
    res = requests.get(url, verify = False)
except Exception as e:
    print(e)
    exit()
if res.status_code == 201:
    print(res.json())

else:
    print(res.json(),)