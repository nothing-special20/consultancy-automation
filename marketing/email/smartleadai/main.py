import requests
import sys
import json
import os
import math
import pandas as pd
import re

from dotenv import dotenv_values

env_path = os.path.dirname(os.path.realpath(__file__)) + "/.env"
config = dotenv_values(env_path)

SMARTLEADAI_API_KEY = config["SMARTLEADAI_API_KEY"]
SMARTLEAD_BASE_URL = "https://server.smartlead.ai/api"

import time

# documentation
# https://help.smartlead.ai/API-Documentation-a0d223bdd3154a77b3735497aad9419f#23f380d414b847bcaa25ab6f7b752196


def fetch_lead_by_email(email):
    url = SMARTLEAD_BASE_URL + f"/v1/leads"
    params = {"api_key": SMARTLEADAI_API_KEY, "email": email}
    response = requests.get(url, params=params, timeout=10).json()
    return response


def campaign_replies(campaign_id, offset=None, limit=None):
    campaign_id = str(campaign_id)
    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/statistics"

    params = {"api_key": SMARTLEADAI_API_KEY}
    if offset:
        params["offset"] = offset

    if limit:
        params["limit"] = limit

    response = requests.get(url, params=params, timeout=10).json()
    data = response["data"]

    replies = [x for x in data if x["reply_time"] is not None]
    for x in replies:
        x["campaign_id"] = campaign_id

    return replies


def get_campaign_info(campaign_id):
    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}"

    params = {"api_key": SMARTLEADAI_API_KEY}

    response = requests.get(url, params=params, timeout=10).json()

    return response


def convert_interest_replies_to_leads(replies):
    already_loaded = []
    lead_list = []

    for x in replies:
        email = x["lead_email"]

        if email not in already_loaded:
            already_loaded.append(email)

            campaign_info = get_campaign_info(x["campaign_id"])
            campaign_name = campaign_info["name"]

            lead = {
                "first_name": x["lead_name"].split(" ")[0],
                "last_name": x["lead_name"].split(" ")[1],
                "email": email,
                "reply_time": x["reply_time"],
                "campaign_name": campaign_name,
                "contact_source": "Email Marketing",
            }

            lead_list.append(lead)

    return lead_list

def list_lead_by_campaign_id(campaign_id, offset=0, limit=100):
    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads"

    params = {"api_key": SMARTLEADAI_API_KEY,
              "offset": offset,
              "limit": limit}

    
    response = requests.get(url, params=params, timeout=10)
    try:
        response = response.json()
    except:
        print(response.text)
        response = {}

    return response

def fetch_all_leads_by_campaign_id(campaign_id, offset=0, limit=100):

    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads"

    params = {"api_key": SMARTLEADAI_API_KEY, "offset": offset, "limit": limit}

    response = requests.get(url, params=params, timeout=10).json()

    return response

def delete_lead_by_campaign_id(campaign_id, lead_id):
    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads/{lead_id}"

    params = {"api_key": SMARTLEADAI_API_KEY}

    response = requests.delete(url, params=params, timeout=10)
    try:
        response = response.json()
    except:
        print(response.text)
        response = {}

    return response

def remove_email_account_from_campaign(campaign_id, email_account_ids):
    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/email-accounts"

    params = {"api_key": SMARTLEADAI_API_KEY, "email_account_ids": email_account_ids}

    response = requests.delete(url, params=params, timeout=10)

    try:
        response = response.json()
    except:
        print(response.text)
        response = {}
    
    return response


def get_all_campaign_data():
    url = SMARTLEAD_BASE_URL + "/v1/campaigns"

    params = {"api_key": SMARTLEADAI_API_KEY}

    response = requests.get(url, params=params, timeout=10)

    try:
        output = response.json()
    except:
        print(response.text)
        output = {}
    
    return output

def get_campaign_statistics(campaign_id, offset=0, limit=100):
    url = f"https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/statistics"
    params = {
        "api_key": SMARTLEADAI_API_KEY,
        "offset": offset,
        "limit": limit,
        # Uncomment and set the optional parameters as needed
        # "email_sequence_number": "YOUR_EMAIL_SEQUENCE_NUMBER",  # Replace with your actual email sequence number
        # "email_status": "YOUR_EMAIL_STATUS"  # Replace with your actual email status
    }

    response = requests.get(url, params=params, timeout=10)

    try:
        output = response.json()
    except:
        print(response.text)
        output = {}
    
    return output

def campaign_statistics_df(campaign_response):
    data = []
    for x in campaign_response["data"]:
        temp = {
            "lead_name": x.get("lead_name", "_"),
            "lead_email": x.get("lead_email", "_"),
            "email_campaign_seq_id": x.get("email_campaign_seq_id", "_"),
            "stats_id": x.get("stats_id", "_"),
            "sent_time": x.get("sent_time", "_"),
            "open_time": x.get("open_time", "_"),
            "reply_time": x.get("reply_time", "_"),
            "click_count": x.get("click_count", "_"),
            "is_unsubscribed": x.get("is_unsubscribed", "_"),
            "is_bounced": x.get("is_bounced", "_"),
        }
        try:
            temp["sent_day"] = re.findall(r"(\d{4}-\d{2}-\d{2})", temp["sent_time"])[0]
        except:
            temp["sent_day"] = "_"

        try:
            temp["open_day"] = re.findall(r"(\d{4}-\d{2}-\d{2})", temp["open_time"])[0]
        except:
            temp["open_day"] = "_"
        
        try:
            temp["reply_day"] = re.findall(r"(\d{4}-\d{2}-\d{2})", temp["reply_time"])[0]
        except:
            temp["reply_day"] = "_"

        temp = pd.DataFrame([temp])
        #replace None with ""
        data.append(temp)

    df = pd.concat(data)
    return df

def get_campaign_statistics_by_date(campaign_id):
    url = f"https://server.smartlead.ai/api/v1/campaigns/{str(campaign_id)}/analytics-by-date"
    params = {
        "api_key": SMARTLEADAI_API_KEY,  # Replace with your actual API key
        "start_date": "2023-01-01",
        "end_date": "2025-12-31"
    }

    response = requests.get(url, params=params, timeout=10)

    try:
        output = response.json()
    except:
        print(response.text)
        output = {}
    
    return output

if __name__ == "__main__":
    if sys.argv[1] == "get_all_campaigns":
        pass
        

    elif sys.argv[1] == "get_campaign_emails":
        # List all email accounts per campaign
        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/email-accounts"

        params = {"api_key": SMARTLEADAI_API_KEY}

        response = requests.get(url, params=params, timeout=10)

        for x in response.json():
            print(x)

    elif sys.argv[1] == "get_all_leads_by_campaign_id":
        # List all leads by campaign id
        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads"

        print(url)

        params = {"api_key": SMARTLEADAI_API_KEY, "offset": 0, "limit": 1000}

        response = requests.get(url, params=params, timeout=10)

        try:
            response_json = response.json()

            data = response_json["data"]

            for x in data:
                print(x)

            print(len(data))
        except:
            print(response.text)

    elif sys.argv[1] == "reply_to_lead":
        # Reply To Lead From Master Inbox via API
        pass

    elif sys.argv[1] == "Add leads to a campaign by ID":
        # Add leads to a campaign by ID
        # https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/leads?api_key={API_KEY}
        # --data '{"lead_list": List<lead_input> *(max 100 leads)}'

        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads"

        params = {"api_key": SMARTLEADAI_API_KEY}

        lead_list = []

        data = {
            "lead_list": lead_list,
            "settings": {
                "ignore_global_block_list": True,  # true ignores leads uploaded in the lead list that are part of your global/client level block list
                "ignore_unsubscribe_list": True,  # true ignores leads uploaded in the lead list that have unsubsribed previously
                "ignore_duplicate_leads_in_other_campaign": False,  # false allows leads to be added to this campaign even if they exist in another campaign
            },
        }

        response = requests.get(url, params=params, data=data, timeout=10)

        for x in response.json():
            print(x)

    elif sys.argv[1] == "campaign_analytics_by_date":
        campaign_id = str(224440)
        url = "https://server.smartlead.ai/api/"
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/analytics-by-date"
        # ?api_key={API_KEY}&start_date=2022-12-16&end_date=2022-12-23

    elif sys.argv[1] == "campaign_stats":
        replies = campaign_replies(245908)
        interested_replies = [x for x in replies if x["lead_category"] == "Interested"]

        print("# of replies:", len(replies))
        print("# of interested replies:", len(interested_replies))

        leads_who_replied = [x for x in replies]

        new_leads = convert_interest_replies_to_leads(interested_replies)

        for x in new_leads:
            print(x)

    elif sys.argv[1] == "fetch_lead_by_email":
        leads_response = fetch_all_leads_by_campaign_id(245908)
        leads_data = leads_response['data']

        # print(leads.keys())
        print(json.dumps(leads_data[0], indent=4))

        def campaign_lead_transform(raw_lead):

            _lead = raw_lead["lead"]

            lead = {
                "first_name": _lead["first_name"],
                "last_name": _lead["last_name"],
                "email": _lead["email"],
                "phone_number": _lead["phone_number"],
                "company": _lead["company"],
                "contact_source": "Email Marketing",
                "website": _lead["website"],
                "company_url": _lead["company_url"],
                "linkedin_profile": _lead["linkedin_profile"],
                "is_unsubscribed": _lead["is_unsubscribed"],
                "campaign_lead_map_id": raw_lead["campaign_lead_map_id"],
                "campaign_status": raw_lead["status"],
                "campaign_lead_created_at": raw_lead["created_at"],
            }

    elif sys.argv[1] == "delete_leads_by_campaign_id":
        # campaign_id_list = ["110634", "112424", "122519", "122546", "123343", "127394", "152613", "127110", "127388", "226497"]
        # campaign_id_list = ["378983", "373514", "319383", "127394"]
        # campaign_id_list = ["323367"]
        campaign_id_list = ["350921", "352437", "356576"]
        for campaign_id in campaign_id_list:
            print(campaign_id)
            stop_looping = False

            lead_statuses = []

            offset = 0

            for i in range(0, 40):
                if stop_looping:
                    print("No more leads to delete", campaign_id)
                    break

                try:
                    leads = list_lead_by_campaign_id(campaign_id, offset, 100)
                    leads_list = leads["data"]
                    if len(leads_list) == 0:
                        stop_looping = True
                        break
                except:
                    leads = []

                print("Total number of leads:\t", len(leads["data"]))
                offset += 100


                for x in leads_list:
                    # print(x)
                    lead_id = x["lead"]["id"]
                    lead_status = x["status"]
                    lead_statuses.append(lead_status)
                    if lead_status != "BLOCKED":
                        print("Deleting lead", lead_id, "from campaign", campaign_id, "with status", lead_status)
                        try:
                            msg = delete_lead_by_campaign_id(campaign_id, lead_id)
                            print(msg)
                        except:
                            print("Error deleting lead", lead_id)
                            time.sleep(20)
                        time.sleep(.5)


            lead_statuses = list(set(lead_statuses))
            print(lead_statuses)

    elif sys.argv[1] == "download_all_leads_by_campaign":
        data = get_all_campaign_data()

        campaign_ids = [x["id"] for x in data]
        folder = "/Users/robquin/Documents/Professional/Entrepreneur/Bill More Tech/consultancy-automation-data/smartlead_data/leads/"


        for campaign_id in campaign_ids:
            print(campaign_id)
            for i in range(0, 10):
                try:
                    test = list_lead_by_campaign_id(campaign_id, 0, 100)
                    time.sleep(1.6)
                    break
                except:
                    pass


            lead_count = int(test["total_leads"])

            #number of lead pages
            num_pages = math.ceil(lead_count / 100)
            print("# of leads", lead_count)
            print("Number of pages:\t", num_pages)

            for i in range(0, num_pages + 1):
                leads = list_lead_by_campaign_id(campaign_id, i * 100, 100)
                time.sleep(1.6)
                #write leads to a file
                file = folder + "campaign_" + str(campaign_id) + "_leads_pg_" + str(i) + ".json"
                with open(file, "w") as f:
                    f.write(json.dumps(leads, indent=4))

            #round up num_pages
                    
    elif sys.argv[1] == "analyze_all_campaign_leads":
        folder = "/Users/robquin/Documents/Professional/Entrepreneur/Bill More Tech/consultancy-automation-data/smartlead_data/leads/"
        files = [folder + x for x in os.listdir(folder) if x.endswith(".json")]
        data = []
        for file in files:
            with open(file, "r") as f:
                data.append(json.load(f))
        
        print(len(data))

        def flatten_lead_data(data):
            flattened_data = {}
            
            for key, value in data.items():
                if key == "lead":
                    for lead_key, lead_value in value.items():
                        flattened_data[f"lead_{lead_key}"] = lead_value
                else:
                    flattened_data[key] = value

            return pd.DataFrame([flattened_data])

        # print(json.dumps(data[5]["data"][0], indent=4))
        # print(data[2]["data"][0])

        lead_data_list = []

        for x in data:
            if "data" in x:
                for y in x["data"]:
                    lead_data_list.append(flatten_lead_data(y))


        df = pd.concat(lead_data_list, axis=0)

        df.to_csv(folder + "all_leads.csv", index=False)