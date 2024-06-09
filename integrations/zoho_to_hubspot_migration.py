import os, sys
import json
from datetime import datetime, timedelta

import pandas as pd
#show all pd columns
pd.set_option('display.max_columns', None)

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MARKETING_DIR = parent_dir + "/marketing"
ZOHO_DIR = parent_dir + "/zoho"
HUBSPOT_DIR = parent_dir + "/hubspot_code"
print(HUBSPOT_DIR)

for x in [parent_dir, MARKETING_DIR, ZOHO_DIR, HUBSPOT_DIR]:
    sys.path.append(parent_dir)

from zoho.crm import main as zoho_crm
from hubspot_code.crm import main as hubspot_crm

zoho_auth_token = zoho_crm.refresh_authorization_token()['access_token']

deals = zoho_crm.get_records(zoho_auth_token, "Deals").json()

# print(json.dumps(deals, indent=4))

def zoho_stage_to_hubspot_remap(stage):
    mapping = {
        "Qualification": "First Response Received",
        "Needs Analysis": "Additional Information Requested",
        "Value Proposition": "Appointment Scheduled",
        "Proposal/Price Quote": "Proposal Requested",
        "Negotiation/Review": "Negotiation",
        "Closed Won": "Closed Won",
        "Closed Lost": "Closed Lost",
        "Closed Lost to Competition": "Closed Lost",
    }
    return mapping[stage]


deal_list = deals["data"]

print('Number of lists:\t',len(deal_list))

for x in deal_list:
    # print(json.dumps(x, indent=4))
    if 'Description' in list(x.keys()):
        try:
            notes = ["JOB DESCRIPTION:\n\n" + x['Description']]
        except:
            notes = []
    else:
        notes = []

    deal_name = x['Deal_Name']
    zoho_deal_stage = x['Stage']
    hubspot_deal_stage = zoho_stage_to_hubspot_remap(zoho_deal_stage)

    deal_stage_id = hubspot_crm.deal_stage_mapping(hubspot_deal_stage)
    deal_amount = x['Amount']

    #2 weeks from now, unix time
    print(x['Closing_Date'])
    close_date = datetime.strptime(x['Closing_Date'], "%Y-%m-%d") + timedelta(hours=5)

    close_date = int(close_date.timestamp())


    deal_properties = {
            "dealname": deal_name,
            "hubspot_owner_id": hubspot_crm.HUBSPOT_OWNER_ID,
            "lead_source": "UPWORK",
            "dealstage": deal_stage_id,
            "amount": deal_amount,
            "notes": notes,
            "closedate": close_date * 1000,
        }
    # print(json.dumps(deal_properties, indent=4))
    hubspot_crm.create_deal(deal_properties)
