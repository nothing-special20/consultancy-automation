import requests
import json

import sys, os

from dotenv import dotenv_values

env_path = os.path.dirname(os.path.realpath(__file__))+ "/.env"
config = dotenv_values(env_path)
ZOHO_CLIENT_ID = config["ZOHO_CLIENT_ID"]
ZOHO_CLIENT_SECRET = config["ZOHO_CLIENT_SECRET"]
ZOHO_API_KEY = config["ZOHO_API_KEY"]
EMAIL = config["EMAIL"]
ZOHO_REDIRECT_URI = config["ZOHO_REDIRECT_URI"]
ZOHO_TEMP_CODE = config["ZOHO_TEMP_CODE"]
ZOHO_CRM_OWNER_ID = config["ZOHO_CRM_OWNER_ID"]
ZOHO_CRM_OWNER_NAME = config["ZOHO_CRM_OWNER_NAME"]
ZOHO_AUTH_TOKEN = config["ZOHO_AUTH_TOKEN"]
ZOHO_REFRESH_TOKEN = config["ZOHO_REFRESH_TOKEN"]


zoho_accounts_url = "https://accounts.zoho.com"

auth_url = zoho_accounts_url + "/oauth/v2/token"  # + "/oauth/v2/auth"

# https://www.zoho.com/accounts/protocol/oauth/self-client/client-credentials-flow.html
def authorization_token():
    authorization_token_data = {
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": ZOHO_TEMP_CODE,
    }

    auth_url = "https://accounts.zoho.com/oauth/v2/token"

    response = requests.post(url=auth_url, params=authorization_token_data)

    return response.json()

def refresh_authorization_token():
    url = "https://accounts.zoho.com/oauth/v2/token"

    params = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }

    response = requests.post(url=url, params=params)

    return response.json()
    

#####Leads
def create_leads(auth_token, records):
    api_headers = {
        "Authorization": "Zoho-oauthtoken " + auth_token,
    }

    for rec in records:
        rec["Owner"] = {"id": ZOHO_CRM_OWNER_ID, "full_name": ZOHO_CRM_OWNER_NAME}

    leads_url = "https://www.zohoapis.com/crm/v2.2/Leads"
    request_body = {
        "data": records,
    }

    request_body = json.dumps(request_body)

    response = requests.post(url=leads_url, headers=api_headers, data=request_body)

    return response.json()

#####Contacts
def create_contacts(auth_token, records):
    api_headers = {
        "Authorization": "Zoho-oauthtoken " + auth_token,
    }

    for rec in records:
        rec["Owner"] = {"id": ZOHO_CRM_OWNER_ID, "full_name": ZOHO_CRM_OWNER_NAME}

    leads_url = "https://www.zohoapis.com/crm/v2.2/Contacts"
    request_body = {
        "data": records,
    }

    request_body = json.dumps(request_body)

    response = requests.post(url=leads_url, headers=api_headers, data=request_body)

    return response.json()

def get_records(auth_token, module, id):
    api_headers = {
        "Authorization": "Zoho-oauthtoken " + auth_token,
    }

    module_url = f"https://www.zohoapis.com/crm/v2.2/{module}" #/{id}

    response = requests.get(url=module_url, headers=api_headers)

    return response

def get_modules(auth_token):
    api_headers = {
        "Authorization": "Zoho-oauthtoken " + auth_token,
    }

    url = "https://www.zohoapis.com/crm/v2/settings/modules"

    response = requests.get(url=url, headers=api_headers)

    return response

#####Deals
def create_deal(auth_token, records):
    headers = {
        "Authorization": "Zoho-oauthtoken " + auth_token,
    }

    for rec in records:
        rec["Owner"] = {"id": ZOHO_CRM_OWNER_ID, "full_name": ZOHO_CRM_OWNER_NAME}

    leads_url = "https://www.zohoapis.com/crm/v2.2/Deals"
    request_body = {
        "data": records,
    }

    request_body = json.dumps(request_body)

    response = requests.post(url=leads_url, headers=headers, data=request_body)

    return response


if __name__ == "__main__":
    if sys.argv[1] == "get_auth_token":
        auth_token_json = authorization_token()
        print(auth_token_json)
        auth_token = auth_token_json["access_token"]
        print(auth_token)

    elif sys.argv[1] == "refresh_authorization_token":
        print(refresh_authorization_token())

    elif sys.argv[1] == "create_leads":
        records = [
            {
                "Email_Opt_Out": False,
                "Rating": "-None-",
                "Lead_Status": "-None-",
                "Industry": "-None-",
                "Lead_Source": "-None-",
                "Company": "test",
                "Last_Name": "me",
                "First_Name": "delete",
                "Designation": "test",
                "Phone": "1231231234",
                "Website": "test.com",
                "Description": "test",
            }
        ]

        lead = create_leads(ZOHO_AUTH_TOKEN, records)

        print(lead)

    elif sys.argv[1] == "create_contacts":
        records = [
            {
                "Email_Opt_Out": False,
                "Rating": "-None-",
                "Lead_Status": "-None-",
                "Industry": "-None-",
                "Lead_Source": "-None-",
                "Company": "test",
                "Last_Name": "me",
                "First_Name": "delete",
                "Designation": "test",
                "Phone": "1231231234",
                "Website": "test.com",
                "Description": "test",
            }
        ]

        contacts = create_contacts(ZOHO_AUTH_TOKEN, records)

        print(contacts)

    elif sys.argv[1] == "create_deal":
        records = [
            {
                "Ownership": "-None-",
                "Rating": "-None-",
                "Industry": "-None-",
                "Account_Type": "New Business",
                "Account_Name": "test account",
                "$zia_owner_assignment": "owner_recommendation_unavailable",
                "zia_suggested_users": {},
            }
        ]

        create_deal(ZOHO_AUTH_TOKEN, records)


    elif sys.argv[1] == 'meetings':
        # https://www.zoho.com/meeting/api-integration/meeting-api/create-a-meeting.html
    
        zsoid = '6068214000000587001'

        url = f'https://meeting.zoho.com/api/v2/{zsoid}/sessions.json'
        

        api_headers = {
            "Authorization": "Zoho-oauthtoken " + ZOHO_AUTH_TOKEN,
        }

        data = {
            "session": {
                "topic": "Monthly Marketing Meeting",
                "agenda": "Points to get noted during meeting.",
                "presenter": 123456789,
                "startTime": "Jun 19, 2020 07:00 PM",
                "duration": 3600000,
                "timezone": "Asia/Calcutta",
                "participants": [
                    {
                        "email": "dummy@email.com",
                        "type": "lead",
                        "id": ""
                    }
                ]
            }
        }

        response = requests.post(url, headers=api_headers, json=data)
        print(response.json())

    elif sys.argv[1] == 'get_meetings':
        zsoid = "org840250224"
        meeting_key = "6068214000000587001"
        url = f"https://meeting.zoho.com/api/v2/{zsoid}/sessions/{meeting_key}.json"
        api_headers = {
            "Authorization": "Zoho-oauthtoken " + ZOHO_AUTH_TOKEN,
        }

        response = requests.get(url, headers=api_headers)
        print(response.text)
        print(response.status_code)

    elif sys.argv[1] == 'get_modules':
        x = get_modules(ZOHO_AUTH_TOKEN)
        print(x.text)
        print(x.status_code)
