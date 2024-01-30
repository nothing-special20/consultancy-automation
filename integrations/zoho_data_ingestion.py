import os, sys
import json

import pandas as pd
#show all pd columns
pd.set_option('display.max_columns', None)

import time

import traceback

from datetime import datetime

import urllib.parse

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MARKETING_DIR = parent_dir + "/marketing"
EMAIL_DIR = parent_dir + "/marketing/email"
APOLLO_DIR = parent_dir + "/marketing/email/apollo.io"
MILLION_VERIFIER_DIR = parent_dir + "/marketing/email/million_verifier"
ZOHO_DIR = parent_dir + "/zoho"
GOOGLE_DIR = parent_dir + "/google"

for x in [parent_dir, MARKETING_DIR, EMAIL_DIR, APOLLO_DIR, ZOHO_DIR, MILLION_VERIFIER_DIR, GOOGLE_DIR]:
    sys.path.append(parent_dir)

from dotenv import dotenv_values
config = dotenv_values(".env")

DATA_FOLDER = config["DATA_FOLDER"]
APOLLO_DATA_FOLDER = DATA_FOLDER + "apollo/"
MILLION_VERIFIER_DATA_FOLDER = DATA_FOLDER + "million_verifier/"
INTEGRATION_DATA_FOLDER = DATA_FOLDER + "integrations/"

million_verifier_log = MILLION_VERIFIER_DATA_FOLDER + 'million_verifier_log.txt'

from marketing.email.apollo_io import main as apollo
from marketing.email.million_verifier import main as mil_ver

from zoho.crm import main as zoho_crm

from google.sheets import main as gsheets

def build_apollo_contacts_df(top_range, q_keywords):
    data_list = []
    for i in range(1, top_range):
        params = {
            "q_keywords": q_keywords,
            "sort_by_field": "contact_last_activity_date",
            "sort_ascending": False,
            "page": str(i),
        }
        search_list = apollo.search_contacts(params)["contacts"]

        # y = 0
        for x in search_list:
            # if y == 0:
            #     print(json.dumps(x, indent=4))
            try:
                cleaned_data = apollo.apollo_contact_etl_zoho_lead(x, [{"name": q_keywords}])
                data_list.append(cleaned_data)
            except:
                print(traceback.format_exc())

            # y += 1

        time.sleep(1.25)

    apollo_df = pd.concat([pd.DataFrame([x]) for x in data_list])

    return apollo_df

if __name__ == "__main__":
    if sys.argv[1] == "get_apollo_contacts":
        apollo_df = build_apollo_contacts_df(2, "sales manager")
        print(apollo_df)
        print(apollo_df.shape)
    
    elif sys.argv[1] == "verify_emails":
        file = APOLLO_DATA_FOLDER + 'apollo-zoho-crm-small-biz-owners.csv'
        df = pd.read_csv(file)

        emails = df['Email'].values.tolist()
        emails = [str(x) for x in emails]
        emails = [email for email in emails if '@' in email]

        # already_loaded_emails = get_already_validated_emails(export_folder)['email'].to_list()
        # already_loaded_emails = [x.lower() for x in already_loaded_emails]

        # emails = [email for email in emails if email.lower() not in already_loaded_emails]

        emails = pd.DataFrame(emails, columns=['email'])

        email_export = MILLION_VERIFIER_DATA_FOLDER + 'temp_emails.csv'

        emails.to_csv(email_export, index=False)

        print(emails.shape)

        x = mil_ver.verify_emails_bulk(email_export)

        # print(x)

        log_update = os.path.basename(file) + "|" #+ x["file_id"] + "\n"

        #append to file
        print(million_verifier_log)
        with open(million_verifier_log, 'a') as f:
            f.write(log_update)

    elif sys.argv[1] == "upload_validated_emails_prod":
        # ['status'] == 'ok'
        
        folder = MILLION_VERIFIER_DATA_FOLDER + 'verified_emails/'
        verified_emails = [pd.read_csv(folder + x) for x in os.listdir(folder) if '.csv' in x]
        verified_emails = pd.concat(verified_emails)
        verified_emails = verified_emails[verified_emails["quality"] == "good"]
        #in verified_emails, rename email to Email
        verified_emails.rename(columns={"email": "Email", "quality": "Email_Verification_Status"}, inplace=True)
        verified_emails['Email_Verification_Date'] = str(datetime.now())

        verified_emails = verified_emails[["Email", "Email_Verification_Status", "Email_Verification_Date"]]

        print(verified_emails)

        apollo_df = build_apollo_contacts_df(25, "owner")

        final_output = pd.merge(apollo_df, verified_emails, how="inner", on="Email")

        #deduplicate final_output by "Email"
        final_output = final_output.drop_duplicates(subset=['Email'])

        # test = final_output.head(1)

        test = final_output.head(20)


        #split data frame into list of jsons
        records = test.to_dict('records')

        zoho_crm.create_leads(zoho_crm.ZOHO_AUTH_TOKEN, records)

    elif sys.argv[1] == "upload_validated_emails_temp":
        mil_ver_folder = MILLION_VERIFIER_DATA_FOLDER + 'verified_emails/'
        verified_emails = [pd.read_csv(mil_ver_folder + x) for x in os.listdir(mil_ver_folder) if '.csv' in x]
        verified_emails = pd.concat(verified_emails)
        verified_emails = verified_emails[verified_emails["quality"] == "good"]
        #in verified_emails, rename email to Email
        verified_emails.rename(columns={"email": "Email", "quality": "Email_Verification_Status"}, inplace=True)
        verified_emails['Email_Verification_Date'] = str(datetime.now())

        verified_emails = verified_emails[["Email", "Email_Verification_Status", "Email_Verification_Date"]]

        apollo_file = APOLLO_DATA_FOLDER + 'apollo-zoho-crm-small-biz-owners.csv'
        apollo_df = pd.read_csv(apollo_file)

        final_output = pd.merge(apollo_df, verified_emails, how="inner", on="Email")

        #deduplicate final_output by "Email"
        final_output = final_output.drop_duplicates(subset=['Email'])
        
        apollo_verified_emails_file = INTEGRATION_DATA_FOLDER + os.path.basename(apollo_file).split('.')[0] + '_verified_emails.csv'
        final_output.to_csv(apollo_verified_emails_file)

    elif sys.argv[1] == "xyz":
        contact_records = [
            {
                "Email_Opt_Out": False,
                "Rating": "-None-",
                "Lead_Status": "-Contacted-",
                "Industry": "-None-",
                "Lead_Source": "Upwork",
                "Company": "test",
                "Last_Name": "me",
                "First_Name": "delete",
                "Designation": "test",
                "Phone": "1231231234",
                "Website": "test.com",
                "Description": "test",
            }
        ]

        # contacts = zoho_crm.create_contacts(zoho_crm.ZOHO_AUTH_TOKEN, contact_records)

    elif sys.argv[1] == "create_upwork_deal":
        sheet_data = gsheets.get_sheet_values(gsheets.UPWORK_LEADS_GOOGLE_SHEET_ID, gsheets.UPWORK_LEADS_GOOGLE_SHEET_TAB)

        job_url = urllib.parse.unquote(sys.argv[2])

        # "Negotiation/Review", "Proposal/Price Quote"
        stage = sys.argv[3]
        
        sheet_data['job_url'] = sheet_data['job_url'].astype(str)
        sheet_data['job_url'] = [urllib.parse.unquote(x) for x in sheet_data['job_url']]
        print(sheet_data.shape)
        job_details = sheet_data[[job_url in str(x) for x in sheet_data['job_url'].to_list()]]
        job_details = job_details.to_dict('records')[0]

        deal_name = job_details['title']
        description = job_details['description']

        closing_date = datetime.now() + pd.Timedelta(days=7)
        closing_date = closing_date.strftime("%Y-%m-%d")

        if job_details['budget'] != 0:
            amount = job_details['budget']

        else:
            amount = (job_details['upper_dollar_amount'] + job_details['lower_dollar_amount']) / 2 * 10

        deal_records = [
            {
                "Deal_Name": deal_name,
                "Stage": stage,
                "Amount": amount,
                "Closing_Date": closing_date,
                "Probability": 10,
                "Account_Name": "Unmapped Upwork Deal",
                "Type": "New Business",
                "Lead_Source": "Upwork",
                "Description": description,
            }
        ]

        print(deal_name)

        deals = zoho_crm.create_deal(zoho_crm.ZOHO_AUTH_TOKEN, deal_records)
        print(deals.text)