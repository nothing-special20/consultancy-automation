import requests
import json
import pandas as pd
import re
from bs4 import BeautifulSoup
import math
import traceback
import time

import os, sys

from dotenv import dotenv_values

env_path = os.path.dirname(os.path.realpath(__file__)) + "/.env"
ENV_VARS = dotenv_values(env_path)

APOLLO_API_KEY = ENV_VARS["APOLLO_API_KEY"]
APOLLO_API_BASE_URL = "https://api.apollo.io"
APOLLO_HEADERS = {"Content-Type": "application/json", "Cache-Control": "no-cache"}
APOLLO_FOLDER = ENV_VARS["APOLLO_FOLDER"]

# Documentation
# https://knowledge.apollo.io/hc/en-us/articles/4416173158541-Use-the-Apollo-REST-API
# https://apolloio.github.io/apollo-api-docs/?shell#search-for-contacts

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

    try:
        return response.json()
    except:
        print(response.text)
        print(response.status_code)
        return None


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


def get_lists_of_lists():
    url = "https://api.apollo.io/v1/labels"
    params = {"api_key": APOLLO_API_KEY}

    response = requests.get(url, headers=APOLLO_HEADERS, params=params)
    return response.json()

def saved_searches():
    url = APOLLO_API_BASE_URL + "/v1/labels/search"

    params = {
        "api_key": APOLLO_API_KEY,
        "label_modality": "contacts",
        "page": 1,
        "display_mode": "explorer_mode",
        "open_factor_names": [],
        "num_fetch_result": 1,
        "show_suggestions": False,
    }

    response = requests.post(url, headers=APOLLO_HEADERS, params=params)

    return response.json()


def get_people_from_list(list_id, page_num=1):
    url = APOLLO_API_BASE_URL + "/v1/mixed_people/search"

    params = {
        "api_key": APOLLO_API_KEY,
        "finder_table_layout_id": None,
        # "contact_label_ids": [list_id],
        "contact_label_id": list_id,
        "prospected_by_current_team": ["yes"],
        "page": page_num,
        "display_mode": "explorer_mode",
        "per_page": 25,
        "open_factor_names": [],
        "num_fetch_result": 1,
        "context": "people-index-page",
        "show_suggestions": False,
    }

    response = requests.get(url, headers=APOLLO_HEADERS, params=params)

    status_code = response.status_code

    if status_code > 200:
        print(status_code)
        print(response.text)

    return response.json()

def map_label_id_to_name(label_id, label_records):
    label_ids = label_id.split(";")

    output = [x["name"] for x in label_records if x["id"] in label_ids]

    if len(output) > 0:
        return "; ".join(output)
    else:
        return None


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
        "Lead_Category_ID": ";".join(x["label_ids"]),
        "Tag": tags,
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
            # "q_keywords": "sales manager",
            "label_ids": [""],
            "sort_by_field": "contact_last_activity_date",
            "sort_ascending": False,
            "page": "1",
        }
        x = search_contacts(params)

        contacts = x["contacts"]

        for y in contacts:
            print(y["first_name"], y["last_name"], y["email"])

        print(json.dumps(contacts[0], indent=4))

        print(json.dumps(x, indent=4))

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

    if sys.argv[1] == "saved_searches":
        x = saved_searches()
        print(x)

    if sys.argv[1] == "lists_of_lists":
        data = get_lists_of_lists()
        print(data)

    if sys.argv[1] == "get_people_from_list":
        _id = ""
        data = get_people_from_list(_id)
        print(json.dumps(data, indent=4))

        people = data["contacts"]
        print("number of people:\t", len(people))

    if sys.argv[1] == "export_saved_lists_to_csv":
        saved_lists = get_lists_of_lists()
        # sort data by created_at
        saved_lists = sorted(saved_lists, key=lambda k: k["created_at"], reverse=True)

        for x in saved_lists[:6]:
            saved_list_df = []
            file_name = APOLLO_FOLDER + "apollo-" + re.sub(":| ", "_",x["name"]).lower() + ".csv"
            saved_list_size = x["cached_count"]
            print("saved list name:\t", x["name"])
            print("saved list size:\t", saved_list_size)
            for i in range(1, (math.ceil(saved_list_size / 25) + 1)):
                try:
                    params = {
                        "label_ids": [x["_id"]],
                        "sort_by_field": "contact_last_activity_date",
                        "sort_ascending": False,
                        "page": i,
                        "per_page": 25,
                    }
                    main_contact_data = search_contacts(params)

                    contacts = main_contact_data["contacts"]
                    contacts = pd.concat(
                        [
                            pd.DataFrame([apollo_contact_etl_zoho_lead(x)])
                            for x in contacts
                        ]
                    )
                    print(contacts.shape)
                    saved_list_df.append(contacts)
                except:
                    print(traceback.format_exc())

                time.sleep(3)

            saved_list_df = pd.concat(saved_list_df)
            saved_list_df.drop_duplicates(inplace=True)
            saved_list_df["Lead_Category"] = saved_list_df["Lead_Category_ID"].apply(lambda x: map_label_id_to_name(x, saved_lists))
            saved_list_df.to_csv(file_name, index=False)


            print("saved_list_df rows:\t", saved_list_df.shape[0])
            print("saved_list_size :\t", saved_list_size)
