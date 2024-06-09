import os, sys
import json

import pandas as pd
#show all pd columns
pd.set_option('display.max_columns', None)

import time

import traceback

from datetime import datetime

import urllib.parse

import math

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
EVABOOT_DATA_FOLDER = DATA_FOLDER + "evaboot/"
EVABOOT_RAW_DATA_FOLDER = EVABOOT_DATA_FOLDER + "raw/"
EVABOOT_CLEANED_DATA_FOLDER = EVABOOT_DATA_FOLDER + "cleaned/"

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

    elif sys.argv[1] == "clean_data":
        if sys.argv[2] == "evaboot":
            sales_people_file_p1 = EVABOOT_RAW_DATA_FOLDER + 'Florida Sales Mgrs - 1-10 Employees.csv'
            sales_people_df_p1 = pd.read_csv(sales_people_file_p1, encoding='UTF-16', sep="\t")

            sales_people_file_p2 = EVABOOT_RAW_DATA_FOLDER + 'Sales Managers - 1-10 Employees - USA Excl CA.csv'
            sales_people_df_p2 = pd.read_csv(sales_people_file_p1, encoding='UTF-16', sep="\t")

            sales_people_df = pd.concat([sales_people_df_p1, sales_people_df_p2])

            sales_people_df = sales_people_df[[("sales" in x.lower() or "marketing" in x.lower()) and "manager" in x.lower() for x in sales_people_df['Prospect Position'].to_list()]]
            sales_people_df["Company Employee Count"] = sales_people_df["Company Employee Count"].astype(int)
            sales_people_df = sales_people_df[sales_people_df["Company Employee Count"] < 50]
            sales_people_df.to_csv(EVABOOT_CLEANED_DATA_FOLDER + 'Sales Mgrs - 1-10 Employees.csv', sep="\t", index=False)

            owner_interested_in_crm_file = EVABOOT_RAW_DATA_FOLDER + 'Owners - 1-10 USA - CRM Interest.csv'
            owner_interested_in_crm_df = pd.read_csv(owner_interested_in_crm_file, encoding='UTF-16', sep="\t")

            owner_interested_in_crm_df = owner_interested_in_crm_df[["owner" in x.lower() for x in owner_interested_in_crm_df['Prospect Position'].to_list()]]
            owner_interested_in_crm_df["Company Employee Count"] = owner_interested_in_crm_df["Company Employee Count"].astype(int)
            owner_interested_in_crm_df = owner_interested_in_crm_df[owner_interested_in_crm_df["Company Employee Count"] < 20]
            # owner_interested_in_crm_df.to_csv(EVABOOT_CLEANED_DATA_FOLDER + 'Owners - 1-10 USA - CRM Interest.csv', sep="\t", index=False)

    
    elif sys.argv[1] == "verify_emails":
        # file = APOLLO_DATA_FOLDER + 'apollo-zoho-crm-small-biz-owners.csv'
        # df = pd.read_csv(file)

        if sys.argv[2] == "apollo":
            files = [APOLLO_DATA_FOLDER + x for x in os.listdir(APOLLO_DATA_FOLDER) if '.csv' in x]
            # files = [x for x in files if (time.time() - os.path.getmtime(x)) < 20 * 60]
            files = sorted(files, key=os.path.getmtime, reverse=True)
            files = files[:1]

            for file in files:
                print(os.path.basename(file))

            # print([os.path.basename(x) for x in files])
            df = pd.concat([pd.read_csv(x) for x in files])

        elif sys.argv[2] == "evaboot":
            # file = EVABOOT_RAW_DATA_FOLDER + 'Email Marketing Software Interest - USA Excl CA.csv'
            files = [EVABOOT_CLEANED_DATA_FOLDER + x for x in os.listdir(EVABOOT_CLEANED_DATA_FOLDER) if '.csv' in x]
            # files = [x for x in files if (time.time() - os.path.getmtime(x)) < 120 * 60]
            files = sorted(files, key=os.path.getmtime, reverse=True)

            print(len(files))
            files = files[:1]
            print(files)
            # df = pd.concat([pd.read_csv(x, sep="\t") for x in files])
            df = pd.concat([pd.read_csv(x,  sep="\t", encoding='UTF-16') for x in files])

            df.rename(columns={"Evaboot Email": "Email"}, inplace=True)
            print(df.shape)

        emails = df['Email'].values.tolist()
        emails = [str(x) for x in emails]
        emails = [email for email in emails if '@' in email]
        emails = list(set(emails))

        # already_loaded_emails = get_already_validated_emails(export_folder)['email'].to_list()
        # already_loaded_emails = [x.lower() for x in already_loaded_emails]

        # emails = [email for email in emails if email.lower() not in already_loaded_emails]

        emails = pd.DataFrame(emails, columns=['email'])

        email_export = MILLION_VERIFIER_DATA_FOLDER + 'temp_emails.csv'

        emails.to_csv(email_export, index=False)

        print(emails.shape)

        for file in files:
            print(os.path.basename(file))

        x = mil_ver.verify_emails_bulk(email_export)

        print(x)

        for file in files:
            log_update = os.path.basename(file) + "|" + x["file_id"] + "\n"

        #append to file
        print(million_verifier_log)
        with open(million_verifier_log, 'a') as f:
            f.write(log_update)

    elif sys.argv[1] == "download_verified_emails":
        file_id = sys.argv[2]
        data = mil_ver.download_report(file_id)

        print(data)

        xpt_file = MILLION_VERIFIER_DATA_FOLDER + 'verified_emails/' + file_id + "-" + str(datetime.now()) + '.csv'
        data.to_csv(xpt_file, index=False)

    elif sys.argv[1] == "upload_validated_emails_prod":
        # ['status'] == 'ok'
        
        folder = MILLION_VERIFIER_DATA_FOLDER + 'verified_emails/'
        verified_emails_df_list = []
        print(folder)
        for x in os.listdir(folder):
            if '.csv' in x:
                temp = pd.read_csv(folder + x)
                file_date = '-'.join(x.split("-")[1:]).split(".")[0]
                file_date = datetime.strptime(file_date, '%Y-%m-%d %H:%M:%S')
                temp['file_date'] = file_date
                verified_emails_df_list.append(temp)

        verified_emails = pd.concat(verified_emails_df_list)
        #get the max file_date by email
        verified_emails = verified_emails.sort_values(by='file_date', ascending=False)
        verified_emails = verified_emails.drop_duplicates(subset=['email'], keep='first')

        verified_emails = verified_emails[verified_emails["quality"] == "good"]
        #in verified_emails, rename email to Email
        verified_emails.rename(columns={"email": "Email", "quality": "Email_Verification_Status"}, inplace=True)
        verified_emails['Email_Verification_Date'] = str(datetime.now())

        verified_emails = verified_emails[["Email", "Email_Verification_Status", "Email_Verification_Date"]]
        if sys.argv[2] == "apollo":
            # apollo_df = build_apollo_contacts_df(25, "owner")
            apollo_files = [APOLLO_DATA_FOLDER + x for x in os.listdir(APOLLO_DATA_FOLDER) if '.csv' in x]
            # filter apollo_files to only include files that were created in the last 48 hours
            # apollo_files = [x for x in apollo_files if (time.time() - os.path.getmtime(x)) < 150 * 60]
            #sort files by most recent
            apollo_files = sorted(apollo_files, key=os.path.getmtime, reverse=True)
            apollo_files = apollo_files[:2]
            # apollo_files = apollo_files[:1]

            for file in apollo_files:
                #what the fuck is this????? ^ V
                # apollo_df = pd.concat([pd.read_csv(file) for x in apollo_files])
                print(os.path.basename(file))
                apollo_df = pd.read_csv(file)

                del apollo_df["Email_Verification_Status"]
                del apollo_df["Email_Verification_Date"]

                final_output = pd.merge(apollo_df, verified_emails, how="inner", on="Email")
                #filter final_output to only include rows where Lead_Category is not null
                final_output = final_output[final_output["Lead_Category"].notnull()]

                #deduplicate final_output by "Email"
                final_output = final_output.drop_duplicates(subset=['Email'])

                #incorporate the following into the apollo to zoho etl function
                final_output['Annual_Revenue'] = final_output['Annual_Revenue'].fillna(0)
                final_output['Email_Opt_Out'] = final_output['Email_Opt_Out'].fillna(False)
                #replace null twitter with ""
                final_output['Twitter'] = final_output['Twitter'].fillna("")
                #convert twitter to text
                final_output['Twitter'] = final_output['Twitter'].astype(str)
                #split twitter into handle and url
                final_output['Twitter'] = final_output['Twitter'].apply(lambda x: x.split("/")[-1] if x != "" else "")

                print(final_output.shape)

                # test = final_output.head(1)

                # print(apollo_files[0])
                final_output.to_csv(INTEGRATION_DATA_FOLDER + "raw/" + os.path.basename(file))

        if sys.argv[2] == "evaboot":
            evaboot_files = [EVABOOT_CLEANED_DATA_FOLDER + x for x in os.listdir(EVABOOT_CLEANED_DATA_FOLDER) if '.csv' in x]
            # filter evaboot_files to only include files that were created in the last 48 hours
            # evaboot_files = [x for x in evaboot_files if (time.time() - os.path.getmtime(x)) < 150 * 60]
            #sort files by most recent
            evaboot_files = sorted(evaboot_files, key=os.path.getmtime, reverse=True)

            evaboot_files = evaboot_files[:4]

            for file in evaboot_files:
                try:
                    evaboot_df = pd.read_csv(file, encoding='UTF-16', sep="\t")
                except:
                    evaboot_df = pd.read_csv(file, sep="\t")

                evaboot_df.rename(columns={"Evaboot Email": "Email"}, inplace=True)

                final_output = pd.merge(evaboot_df, verified_emails, how="inner", on="Email")
                final_output.to_csv(INTEGRATION_DATA_FOLDER + "raw/" + "Evaboot - " + os.path.basename(file), index=False, encoding='UTF-16', sep="\t")
                print(os.path.basename(file))


        # #split final_output into chunks of 100
        # chunk_size = 100
        # chunks = [final_output[i:i + chunk_size] for i in range(0, final_output.shape[0], chunk_size)]


        # for chunk in chunks:
        # #split data frame into list of jsons
        #     records = chunk.to_dict('records')
            
        #     records_nans_removed = []
        #     for record in records:
        #         records_nans_removed.append({k: v for k, v in record.items() if pd.notna(v)})

        #     # for x in records_nans_removed:
        #     #     print(json.dumps(x, indent=4))

        #     x = zoho_crm.create_leads(zoho_crm.ZOHO_AUTH_TOKEN, records_nans_removed)
        #     print(x)

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
        sheet_data = gsheets.get_sheet_values(gsheets.UPWORK_LEADS_GOOGLE_SHEET_ID_V10, gsheets.UPWORK_LEADS_GOOGLE_SHEET_TAB)

        job_url = urllib.parse.unquote(sys.argv[2])

        # "Negotiation/Review", "Proposal/Price Quote"
        stage = sys.argv[3]

        upwork_credits_used_on_proposal = int(sys.argv[4])
        
        sheet_data['job_url'] = sheet_data['job_url'].astype(str)
        sheet_data['job_url'] = [urllib.parse.unquote(x) for x in sheet_data['job_url']]
        print(sheet_data.shape)
        job_details = sheet_data[[job_url in str(x) for x in sheet_data['job_url'].to_list()]]
        job_details = job_details.to_dict('records')[0]

        deal_name = job_details['title']
        description = job_details['description']

        closing_date = datetime.now() + pd.Timedelta(days=7)
        closing_date = closing_date.strftime("%Y-%m-%d")

        #convert job_details['budget'] to float
        job_details['budget'] = float(job_details['budget'])
        job_details['upper_dollar_amount'] = float(job_details['upper_dollar_amount'])
        job_details['lower_dollar_amount'] = float(job_details['lower_dollar_amount'])


        

        if job_details['budget'] > 0:
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
                "Marketing_Cost": .15 * upwork_credits_used_on_proposal,
            }
        ]

        print(deal_name)

        zoho_auth_token = zoho_crm.refresh_authorization_token()['access_token']

        deals = zoho_crm.create_deal(zoho_auth_token, deal_records)
        print(deals.text)


    elif sys.argv[1] == "finalize_list_for_smartlead":
        raw_folder = INTEGRATION_DATA_FOLDER + "raw/"
        raw_files = [raw_folder + x for x in os.listdir(raw_folder) if '.csv' in x]

        # for file in raw_files:
        #     print(os.path.basename(file))
        crm_files = ["apollo-zoho-crm-small-biz-owners_verified_emails.csv"]

        general_marketing_files = [
            "apollo-managers_-_1-10_employees_-_usa_excl_ca_-_interested_in_email_software.csv",
            "apollo-managers_-_11-20_employees_-_usa_excl_ca_-_interested_in_email_software.csv",
            "apollo-directors_-_1-10_employees_-_usa_excl_ca_-_interested_in_email_software.csv",
            "apollo_email_marketing_intent-small-biz-1-20-owners,managers.csv",
        ]

        general_marketing_data = [pd.read_csv(raw_folder + x) for x in general_marketing_files]
        general_marketing_data = pd.concat(general_marketing_data)
        general_marketing_data.drop_duplicates(subset=['Email'], inplace=True)
        del general_marketing_data["Unnamed: 0"]
        #split general_marketing_data into three dataframes based on row count
        split = math.ceil(general_marketing_data.shape[0] / 3)
        general_marketing_data_1 = general_marketing_data[:split]
        general_marketing_data_2 = general_marketing_data[split:split*2]
        general_marketing_data_3 = general_marketing_data[split*2:]

        general_marketing_data_1.to_csv(INTEGRATION_DATA_FOLDER + "smartlead_ready/" + "general_marketing_data_1.csv", index=False)
        general_marketing_data_2.to_csv(INTEGRATION_DATA_FOLDER + "smartlead_ready/" + "general_marketing_data_2.csv", index=False)
        general_marketing_data_3.to_csv(INTEGRATION_DATA_FOLDER + "smartlead_ready/" + "general_marketing_data_3.csv", index=False)

        sales_and_marketing_files = [
            "apollo-marketing_people_-_interested_in_email_marketing_solutions_-_usa_excl_ca_-_1-20_employees.csv",
            "apollo-marketing_people_-_interested_in_email_marketing_software_-_usa_excl_ca_-_1-20_employees.csv",
            "apollo-sales_-_1-20_employees_-_usa_excl_ca_-_interested_in_email_software.csv",
        ]

        sales_and_marketing_data = [pd.read_csv(raw_folder + x) for x in sales_and_marketing_files]
        sales_and_marketing_data = pd.concat(sales_and_marketing_data)
        sales_and_marketing_data.drop_duplicates(subset=['Email'], inplace=True)
        del sales_and_marketing_data["Unnamed: 0"]

        #split sales_and_marketing_data into two dataframes based on row count

        sales_and_marketing_data.to_csv(INTEGRATION_DATA_FOLDER + "smartlead_ready/" + "sales_and_marketing_data.csv", index=False)