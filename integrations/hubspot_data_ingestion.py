import os, sys

from datetime import datetime, timedelta

import urllib.parse

import pandas as pd

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MARKETING_DIR = parent_dir + "/marketing"
HUBSPOT_DIR = parent_dir + "/hubspot_code"
GOOGLE_DIR = parent_dir + "/google"
SMARTLEADAI_DIR = parent_dir + "/marketing/email/smartleadai"

for x in [parent_dir, HUBSPOT_DIR, GOOGLE_DIR, SMARTLEADAI_DIR]:
    sys.path.append(parent_dir)

from hubspot_code.crm import main as hubspot_crm
from google.sheets import main as gsheets
from marketing.email.smartleadai import main as smartlead

if __name__ == "__main__":
    if sys.argv[1] == "create_upwork_deal":
        sheet_data = gsheets.get_sheet_values(gsheets.UPWORK_LEADS_GOOGLE_SHEET_ID_V10, gsheets.UPWORK_LEADS_GOOGLE_SHEET_TAB)

        job_url = urllib.parse.unquote(sys.argv[2])

        # "Negotiation/Review", "Proposal/Price Quote"

        upwork_credits_used_on_proposal = int(sys.argv[3])
        
        sheet_data['job_url'] = sheet_data['job_url'].astype(str)
        sheet_data['job_url'] = [urllib.parse.unquote(x) for x in sheet_data['job_url']]
        print(sheet_data.shape)
        job_details = sheet_data[[job_url in str(x) for x in sheet_data['job_url'].to_list()]]
        job_details = job_details.to_dict('records')[0]

        deal_name = job_details['title']
        description = job_details['description']

        # close_date = datetime.now() + timedelta(days=7) + timedelta(hours=5)
        # close_date = datetime.strptime(datetime.now(), "%Y-%m-%d") + timedelta(days=7) + timedelta(hours=5)
        close_date = datetime.now() + timedelta(days=7) + timedelta(hours=5)
        # close_date = close_date.strftime("%Y-%m-%d")

        close_date = int(close_date.timestamp())

        #convert job_details['budget'] to float
        job_details['budget'] = float(job_details['budget'])
        job_details['upper_dollar_amount'] = float(job_details['upper_dollar_amount'])
        job_details['lower_dollar_amount'] = float(job_details['lower_dollar_amount'])


        if job_details['budget'] > 0:
            amount = job_details['budget']

        else:
            amount = (job_details['upper_dollar_amount'] + job_details['lower_dollar_amount']) / 2 * 10

        # deal_details = {
        #         "Deal_Name": deal_name,
        #         "Stage": stage,
        #         "Amount": amount,
        #         "Closing_Date": closing_date,
        #         # "Probability": 10,
        #         # "Account_Name": "Unmapped Upwork Deal",
        #         "Type": "New Business",
        #         "Lead_Source": "Upwork",
        #         "Description": description,
        #         "Marketing_Cost": .15 * upwork_credits_used_on_proposal,
        #     }
            
        deal_stage_id = hubspot_crm.deal_stage_mapping("Proposal Requested")

        notes = ["JOB DESCRIPTION:\n\n" + description, "JOB URL:\n\n" + job_url, "UPWORK CREDITS USED ON PROPOSAL:\n\n" + str(upwork_credits_used_on_proposal)]
        
        deal_properties = {
            "dealname": deal_name,
            "hubspot_owner_id": hubspot_crm.HUBSPOT_OWNER_ID,
            "lead_source": "UPWORK",
            "dealstage": deal_stage_id,
            "amount": amount,
            "notes": notes,
            "closedate": close_date * 1000,
        }

        print(deal_name)

        deals = hubspot_crm.create_deal(deal_properties)
        print(deals)

    if sys.argv[1] == 'create_hubspot_contacts_from_smartlead_interested_replies':
        # 245908
        campaign_id = sys.argv[2]
        replies = smartlead.campaign_replies(campaign_id)
        interested_replies = [x for x in replies if x["lead_category"] == "Interested"]

        print("# of replies:", len(replies))
        print("# of interested replies:", len(interested_replies))

        leads_who_replied = [x for x in replies]

        new_leads = smartlead.convert_interest_replies_to_leads(interested_replies)

        for x in new_leads:
            print(x)
            hubspot_crm.create_contact(x)