import requests
import json

import urllib.parse

from dotenv import dotenv_values

config = dotenv_values(".env")
ZOHO_CLIENT_ID = config["ZOHO_CLIENT_ID"]
ZOHO_CLIENT_SECRET = config["ZOHO_CLIENT_SECRET"]
ZOHO_API_KEY = config["ZOHO_API_KEY"]
EMAIL = config["EMAIL"]
ZOHO_REDIRECT_URI = config["ZOHO_REDIRECT_URI"]
ZOHO_TEMP_CODE = config["ZOHO_TEMP_CODE"]


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


auth_token_json = authorization_token()
auth_token = auth_token_json["access_token"]
print(auth_token)

def update_lead(auth_token):
    api_headers = {
        "Authorization": "Zoho-oauthtoken " + auth_token,
    }

    leads_url = "https://www.zohoapis.com/crm/v2.2/Leads"
    request_body = {
        "data": [
            {
                "Owner": {"id": "123", "full_name": "xyz"},
                "Email_Opt_Out": False,
                "Rating": "-None-",
                "Lead_Status": "-None-",
                "Industry": "-None-",
                "Lead_Source": "-None-",
                "Company": "test",
                "Last_Name": "testerer",
                "First_Name": "test",
                "Designation": "test",
                "Phone": "1231231234",
                "Website": "test.com",
                "Description": "test",
            }
        ]
    }

    request_body = json.dumps(request_body)

    response = requests.post(url=leads_url, headers=api_headers, data=request_body)

    print(response.text)
