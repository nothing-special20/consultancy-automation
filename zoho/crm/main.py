import requests
import json

import sys

from dotenv import dotenv_values

config = dotenv_values(".env")
ZOHO_CLIENT_ID = config["ZOHO_CLIENT_ID"]
ZOHO_CLIENT_SECRET = config["ZOHO_CLIENT_SECRET"]
ZOHO_API_KEY = config["ZOHO_API_KEY"]
EMAIL = config["EMAIL"]
ZOHO_REDIRECT_URI = config["ZOHO_REDIRECT_URI"]
ZOHO_TEMP_CODE = config["ZOHO_TEMP_CODE"]
ZOHO_CRM_OWNER_ID = config["ZOHO_CRM_OWNER_ID"]
ZOHO_CRM_OWNER_NAME = config["ZOHO_CRM_OWNER_NAME"]
ZOHO_AUTH_TOKEN = config["ZOHO_AUTH_TOKEN"]


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


#####Leads
def create_lead(auth_token, records):
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


#####Deals
def create_deal(auth_token, records):
    headers = {
        "Authorization": "Zoho-oauthtoken " + auth_token,
    }

    for rec in records:
        rec["Owner"] = {"id": ZOHO_CRM_OWNER_ID, "full_name": ZOHO_CRM_OWNER_NAME}

    leads_url = "https://www.zohoapis.com/crm/v2.2/Leads"
    request_body = {
        "data": records,
    }

    request_body = json.dumps(request_body)

    response = requests.post(url=leads_url, headers=headers, data=request_body)

    print(response.text)


if __name__ == "__main__":
    if sys.argv[1] == "get_auth_token":
        auth_token_json = authorization_token()
        print(auth_token_json)
        auth_token = auth_token_json["access_token"]
        print(auth_token)

    elif sys.argv[1] == "create_lead":
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

        lead = create_lead(ZOHO_AUTH_TOKEN, records)

        print(lead)

    elif sys.argv[1] == "create_deal":
        records = [
            {
                "Ownership": "-None-",
                "Rating": "-None-",
                "Industry": "-None-",
                "Account_Type": "-None-",
                "Account_Name": "test account",
                "$zia_owner_assignment": "owner_recommendation_unavailable",
                "zia_suggested_users": {},
            }
        ]

        create_deal(ZOHO_AUTH_TOKEN, records)
