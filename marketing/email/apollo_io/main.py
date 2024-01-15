import requests
import json
import pandas as pd
import re
from bs4 import BeautifulSoup

import os, sys

from dotenv import dotenv_values

env_path = os.path.dirname(os.path.realpath(__file__))+ "/.env"
config = dotenv_values(env_path)

APOLLO_API_KEY = config["APOLLO_API_KEY"]
APOLLO_API_BASE_URL = "https://api.apollo.io"
APOLLO_HEADERS = {"Content-Type": "application/json", "Cache-Control": "no-cache"}


def post_contacts(params):
    url = APOLLO_API_BASE_URL + "/v1/contacts"
    params["api_key"] = APOLLO_API_KEY

    response = requests.post(url, headers=APOLLO_HEADERS, params=params)
    print(response.text)
    print(response.status_code)
    print(json.dumps(response.json(), indent=4))


def search_contacts(params):
    url = APOLLO_API_BASE_URL + "/v1/contacts/search"

    params["api_key"] = APOLLO_API_KEY

    response = requests.post(url, headers=APOLLO_HEADERS, data=json.dumps(params))

    return response.json()


def search_people(params):
    url = APOLLO_API_BASE_URL + "/v1/mixed_people/search"
    params["api_key"] = APOLLO_API_KEY

    response = requests.post(url, headers=APOLLO_HEADERS, data=json.dumps(params))

    return response.json()


def get_users():
    url = APOLLO_API_BASE_URL + "/v1/contacts"
    params = {"api_key": APOLLO_API_KEY}

    response = requests.post(url, headers=APOLLO_HEADERS, params=params)

    return response.json()


def apollo_contact_etl_zoho_lead(x, tags=None):
    extracted_data = {
        "Annual_Revenue": None,  # Not available in the JSON
        "Average_Time_Spent_(Minutes)": None,  # Not available
        "City": x.get("city", None),
        "Company": x.get("organization_name", None),
        "Converted_Account": None,  # Not available
        "Converted_Contact": None,  # Not available
        "Converted_Deal": None,  # Not available
        "Country": x.get("country", None),
        "Days_Visited": None,  # Not available
        "Description": None,  # Not available
        "Email": x.get("email", None),
        "Email_Opt_Out": x.get("email_unsubscribed", None),
        "Email_Verification_Date": None,  # Not available
        "Email_Verification_Status": x.get("email_true_status", None),
        "Fax": None,  # Not available
        "First_Name": x.get("first_name", None),
        "First_Page_Visited": None,  # Not available
        "First_Visit": None,  # Not available
        "Industry": None,  # Not available
        "Last_Name": x.get("last_name", None),
        "Record_Image": x.get("photo_url", None),
        "Lead_Owner": x.get("owner_id", None),
        "Lead_Status": None,  # Not available
        "Mobile": None,  # Not available
        "Most_Recent_Visit": None,  # Not available
        "No. of Employees": None,  # Not available
        "Number Of Chats": None,  # Not available
        "Phone": x.get("sanitized_phone", None),
        "Rating": None,  # Not available
        "Referrer": None,  # Not available
        "Secondary Email": None,  # Not available
        "Skype ID": None,  # Not available
        "State": x.get("state", None),
        "Street": None,  # Not available
        "Designation": x.get("title", None),
        "Twitter": x["account"].get("twitter_url", None) if "account" in x else None,
        "Visitor Score": None,  # Not available
        "Website": x["account"].get("website_url", None) if "account" in x else None,
        "Zip Code": None,  # Not available
        "Lead_Source": "apollo.io",
        "Tag": tags
    }

    return extracted_data


if __name__ == "__main__":
    if sys.argv[1] == "search":
        params = {
            "page": 1,
            "per_page": 50,
            "organization_locations": ["United States"],
            # "person_seniorities": ["senior", "manager"],
            "organization_num_employees_ranges": ["1,20"],
            "person_titles": ["sales manager"],
            "technology_names": ["Zoho CRM"],
        }

        # params = {
        #     # "q_organization_domains": "apollo.io\ngoogle.com",
        #     "page": 1,
        #     "per_page": 2,
        #     # "organization_locations": ["California, US"],
        #     "organization_locations": ["United States"],
        #     "person_seniorities": ["senior", "manager"],
        #     "organization_num_employees_ranges": ["1,1000000"],
        #     "person_titles": ["sales manager", "engineer manager"],
        #     "api_key": APOLLO_API_KEY,
        # }

        x = search_people(params)

        # write to file
        with open("apollo_output.json", "w") as outfile:
            json.dump(x, outfile)

    if sys.argv[1] == "get_users":
        get_users()

    if sys.argv[1] == "search_contacts":
        params = {
            "q_keywords": "sales manager",
            "sort_by_field": "contact_last_activity_date",
            "sort_ascending": False,
            "page": "5",
        }
        x = search_contacts(params)

        # print(json.dumps(x, indent=4))

        print(x.keys())

        print(len(x["contacts"]))

        with open("apollo_search_contact.json", "w") as outfile:
            json.dump(x, outfile)

        # print(json.dumps(x["contacts"][0], indent=4))

        etl = apollo_contact_etl_zoho_lead(x["contacts"][0])
        print(json.dumps(etl, indent=4))
        # print(x["contacts"][0])
        # print(x["contacts"][0].get("city", None))
        print(x["contacts"][0]["city"])

    if sys.argv[1] == "zoho_leads_fields":
        html_file = "zoho_leads_module.html"

        with open(html_file, "r") as file:
            data = file.read()

        rows = data.split("</lyte-tr>")

        # Extract the column names from the first row
        columns = [
            x.get_text()
            for x in BeautifulSoup(rows[0], "html.parser").find_all("lyte-th")
        ]

        # Extract the data from the remaining rows
        data = [
            [
                x.get_text()
                for x in BeautifulSoup(row, "html.parser").find_all("lyte-td")
            ]
            for row in rows[1:-1]
        ]

        # Create the pandas DataFrame
        df = pd.DataFrame(data, columns=columns)

        fields = df["Fields"].to_list()
        fields = [re.sub("\t", "", x.strip()) for x in fields]

        print(", ".join(fields))
