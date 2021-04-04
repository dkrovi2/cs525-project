from pprint import pprint
from river import datasets

import json

# dataset = datasets.Phishing()
# dataset = datasets.airline_passengers.AirlinePassengers()
dataset = datasets.sms_spam.SMSSpam()
for record in dataset:
    print(record)
    r_json = json.dumps(record)
    o_json = json.loads(r_json)
    print(r_json)

