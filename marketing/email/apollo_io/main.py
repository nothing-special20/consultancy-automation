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
# APOLLO_API_KEY = ENV_VARS["APOLLO_API_KEY_SECOND"]
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

    response = requests.post(
        url, headers=APOLLO_HEADERS, data=json.dumps(params), timeout=10
    )

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


def search_accounts(params):
    url = APOLLO_API_BASE_URL + "/v1/accounts/search"
    params["api_key"] = APOLLO_API_KEY

    response = requests.post(url, headers=APOLLO_HEADERS, data=json.dumps(params))

    return response.json()


def search_companies(params):
    url = APOLLO_API_BASE_URL + "/v1/mixed_companies/search"
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
        "linkedin": x["account"].get("linkedin_url", None) if "account" in x else None,
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

        # print(saved_lists)

        # for x in saved_lists:
        #     print(x)

        for x in saved_lists[:1]:
            print(x["name"])
            # for x in []:
            saved_list_df = []
            file_name = (
                APOLLO_FOLDER
                + "apollo-"
                + re.sub(":| ", "_", x["name"]).lower()
                + ".csv"
            )
            saved_list_size = x["cached_count"]
            print("saved list name:\t", x["name"])
            print("saved list size:\t", saved_list_size)
            print((math.ceil(saved_list_size / 25) + 1))

            page_count = math.ceil(saved_list_size / 25)  # + 21
            for i in range(1, page_count):
                try:
                    params = {
                        "label_ids": [x["_id"]],
                        "sort_by_field": "contact_last_activity_date",
                        "sort_ascending": False,
                        "page": i,
                        "per_page": 25,
                    }
                    main_contact_data = search_contacts(params)

                    _contacts = main_contact_data["contacts"]

                    contacts = []

                    for contact in _contacts:
                        try:
                            temp = apollo_contact_etl_zoho_lead(contact)
                            temp = pd.DataFrame([temp])
                            contacts.append(temp)
                        except:
                            print(traceback.format_exc())

                    contacts = pd.concat(contacts)

                    # contacts = pd.concat(
                    #     [
                    #         pd.DataFrame([apollo_contact_etl_zoho_lead(x)])
                    #         for x in contacts
                    #     ]
                    # )
                    print(contacts.shape)
                    saved_list_df.append(contacts)
                except:
                    print(traceback.format_exc())

                time.sleep(3)

            saved_list_df = pd.concat(saved_list_df)
            print("saved_list_df - pre drop duplicate rows:\t", saved_list_df.shape[0])
            saved_list_df.drop_duplicates(inplace=True)
            saved_list_df["Lead_Category"] = saved_list_df["Lead_Category_ID"].apply(
                lambda x: map_label_id_to_name(x, saved_lists)
            )
            saved_list_df.to_csv(file_name, index=False)

            print("saved_list_df rows:\t", saved_list_df.shape[0])
            print("saved_list_size :\t", saved_list_size)
            print("saved to file:\t", file_name)

    if sys.argv[1] == "split_data_for_smartlead":
        folder = "/Users/robquin/Documents/Professional/Entrepreneur/Bill More Tech/consultancy-automation-data/integrations/smartlead_ready/"
        # file = folder + "Sales_Mgrs_-_USA_-_21-50_1714431430_x14273_OK_ONLY_MILLIONVERIFIER.COM.csv"
        file = (
            folder
            + "apollo - Sales Managers - Using Hubspot - 11-20 Employees_OK_ONLY_MILLIONVERIFIER.COM.csv"
        )

        data = pd.read_csv(file)

        def company_name_cleaner(x):
            output = re.sub(
                "Inc.|LLC|Corp.|Co.|Ltd.|Limited|Incorporated|Corporation", "", x
            )
            output = re.sub("  ", " ", output)
            return output

        data["Company"] = [company_name_cleaner(x) for x in data["Company"]]

        # data["domain"] = data["email"].apply(lambda x: x.split("@")[1] if "@" in x else None)

        # split df into 2
        data1 = data.iloc[:500]
        data2 = data.iloc[500:]
        # data3 = data.iloc[2400:]

        data1.to_csv(file.split(".csv")[0] + "_1.csv", index=False)
        data2.to_csv(file.split(".csv")[0] + "_2.csv", index=False)
        # data2.to_csv(file.split(".csv")[0] + "_3.csv", index=False)

    if sys.argv[1] == "search_accounts":

        def parse_account_data(item):
            extracted_item = {
                "id": item["id"],
                "name": item["name"],
                "website_url": item["website_url"],
                "blog_url": item["blog_url"],
                "angellist_url": item["angellist_url"],
                "linkedin_url": item["linkedin_url"],
                "twitter_url": item["twitter_url"],
                "facebook_url": item["facebook_url"],
                "phone": item["phone"],
                "linkedin_uid": item["linkedin_uid"],
                "founded_year": item["founded_year"],
                "publicly_traded_symbol": item["publicly_traded_symbol"],
                "publicly_traded_exchange": item["publicly_traded_exchange"],
                "logo_url": item["logo_url"],
                "crunchbase_url": item["crunchbase_url"],
                "primary_domain": item["primary_domain"],
                # 'sanitized_phone': item['sanitized_phone'],
                "organization_raw_address": item["organization_raw_address"],
                "organization_city": item["organization_city"],
                "organization_street_address": item["organization_street_address"],
                "organization_state": item["organization_state"],
                "organization_country": item["organization_country"],
                "organization_postal_code": item["organization_postal_code"],
                "suggest_location_enrichment": item["suggest_location_enrichment"],
                "domain": item["domain"],
                "team_id": item["team_id"],
                "organization_id": item["organization_id"],
                "account_stage_id": item["account_stage_id"],
                "source": item["source"],
                "original_source": item["original_source"],
                "creator_id": item["creator_id"],
                "owner_id": item["owner_id"],
                "created_at": item["created_at"],
                "phone_status": item["phone_status"],
                "hubspot_id": item["hubspot_id"],
                "salesforce_id": item["salesforce_id"],
                "crm_owner_id": item["crm_owner_id"],
                "parent_account_id": item["parent_account_id"],
                "existence_level": item["existence_level"],
                "modality": item["modality"],
                "crm_record_url": item["crm_record_url"],
                "num_contacts": item["num_contacts"],
                "last_activity_date": item["last_activity_date"],
                "intent_strength": item["intent_strength"],
                "show_intent": item["show_intent"],
                "has_intent_signal_account": item["has_intent_signal_account"],
                "intent_signal_account": item["intent_signal_account"],
            }

            return extracted_item

        # print(x)

        saved_lists = get_lists_of_lists()
        # sort data by created_at
        saved_lists = sorted(saved_lists, key=lambda k: k["created_at"], reverse=True)

        latest_list = saved_lists[0]
        latest_list_id = latest_list["_id"]
        latest_list_name = latest_list["name"]
        print(latest_list.keys())
        print(json.dumps(latest_list, indent=4))

        latest_list_num_of_pages = math.ceil(latest_list["cached_count"] / 25)

        response_df_lists = []

        for page in range(1, latest_list_num_of_pages + 1):
            params = {
                "label_ids": [latest_list_id],
                "sort_ascending": False,
                "page": page,
            }
            accounts = search_accounts(params)

            temp = [pd.DataFrame([parse_account_data(x)]) for x in accounts["accounts"]]
            temp = pd.concat(temp)

            response_df_lists.append(temp)

            response_df = pd.concat(response_df_lists)

            file = APOLLO_FOLDER + "apollo_companies" + latest_list_name + ".csv"
            response_df.to_csv(file, index=False)

            print("scraped page:\t", page, "of", latest_list_num_of_pages)

            time.sleep(3)

    if sys.argv[1] == "smartlead_cleaning_detailed_analysis":
        folder = "/Users/robquin/Documents/Professional/Entrepreneur/Bill More Tech/consultancy-automation-data/integrations/smartlead_ready/"
        # file = folder + "Sales_Mgrs_-_USA_-_21-50_1714431430_x14273_OK_ONLY_MILLIONVERIFIER.COM.csv"
        file = (
            folder
            + "apollo - Biz Dev - Marketing & Advertising - USA - 1-50_OK_ONLY_MILLIONVERIFIER.COM.csv"
        )

        data = pd.read_csv(file)

        def company_name_cleaner(x):
            output = str(x)
            output = re.sub(
                "Inc.|LLC|Corp.|Co.|Ltd.|Limited|Incorporated|Corporation", "", output
            )
            output = re.sub("  ", " ", output)
            output = output.strip()
            return output

        try:
            del data["email_status"]
        except:
            pass

        # rename columns
        data.rename(
            columns={
                "first_name": "First Name",
                "last_name": "Last Name",
                "email": "Email",
                "organization_name": "Company Name",
            },
            inplace=True,
        )

        data["Company Name"] = [company_name_cleaner(x) for x in data["Company Name"]]

        # ignore blank companies
        data = data[data["Company Name"] != ""]
        data = data[data["Company Name"].notnull()]
        # data = data[data["title"] == "Business Development Manager"]

        data1 = data.iloc[:1300]
        data2 = data.iloc[1300:]

        data1.to_csv(file.split(".csv")[0] + "_1.csv", index=False)
        data2.to_csv(file.split(".csv")[0] + "_2.csv", index=False)

    if sys.argv[1] == "search_companies":
        agency_websites_file = "agency_websites.json"

        agency_domains = []

        with open(agency_websites_file, "r") as f:
            for line in f:
                data = line.strip()
                if data.endswith(","):
                    data = data[:-1]
                agency = json.loads(data)
                agency_domains.append(agency["company_domain"])

        agency_domains = [re.sub("www\\.", "", x) for x in agency_domains]
        print(len(agency_domains))
        agency_domains = list(set(agency_domains))
        print(len(agency_domains))

        # split agency_domains into chunks of 10
        chunk_size = 10
        chunks = [
            agency_domains[i : i + chunk_size]
            for i in range(0, len(agency_domains), chunk_size)
        ]

        org_ids = []

        for chunk in chunks:
            params = {
                "page": 1,
                "per_page": 10,
                "q_organization_domains": "\n".join(chunk),
            }

            x = search_companies(params)

            temp_organization_ids = [x["id"] for x in x["organizations"]]

            org_ids.extend(temp_organization_ids)

            time.sleep(3)

        print(org_ids)

        org_id_url = ["&organizationIds[]=" + str(x) for x in org_ids]
        org_id_url = "".join(org_id_url)

        print(org_id_url)

        # params = {
        #     "page": 1,
        #     "per_page": 10,
        #     "q_organization_domains": "themetainbound.com"
        # }

        # x = search_companies(params)

        # print(json.dumps(x, indent=4))

    if sys.argv[1] == "test":
        saved_lists = get_lists_of_lists()
        # sort data by created_at
        saved_lists = sorted(saved_lists, key=lambda k: k["created_at"], reverse=True)

        latest_list = saved_lists[0]

        print(latest_list)
        print(latest_list.keys())

        params = {
            "label_ids": [latest_list["_id"]],
            "sort_by_field": "contact_last_activity_date",
            "sort_ascending": False,
            "page": 1,
            # "per_page": 25,
        }

        x = search_companies(params)

        print(json.dumps(x, indent=4))
        print(latest_list["_id"])
