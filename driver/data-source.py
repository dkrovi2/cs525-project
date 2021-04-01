from pprint import pprint
from river import datasets

import json

dataset = datasets.Phishing()
for record in dataset:
    r_json = json.dumps(record)
    o_json = json.loads(r_json)
    print(r_json)

