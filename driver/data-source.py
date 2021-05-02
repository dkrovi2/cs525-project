from river import datasets

import json

dataset = datasets.sms_spam.SMSSpam()
for record in dataset:
    print(record)
    r_json = json.dumps(record)
    o_json = json.loads(r_json)
    print(r_json)

