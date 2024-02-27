import requests
import sys
import json
import os

from dotenv import dotenv_values

env_path = os.path.dirname(os.path.realpath(__file__)) + "/.env"
config = dotenv_values(env_path)

SMARTLEADAI_API_KEY = config["SMARTLEADAI_API_KEY"]
SMARTLEAD_BASE_URL = "https://server.smartlead.ai/api"

# documentation
# https://help.smartlead.ai/API-Documentation-a0d223bdd3154a77b3735497aad9419f#23f380d414b847bcaa25ab6f7b752196


def fetch_lead_by_email(email):
    url = SMARTLEAD_BASE_URL + f"/v1/leads"
    params = {"api_key": SMARTLEADAI_API_KEY, "email": email}
    response = requests.get(url, params=params).json()
    return response


def campaign_replies(campaign_id, offset=None, limit=None):
    campaign_id = str(campaign_id)
    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/statistics"

    params = {"api_key": SMARTLEADAI_API_KEY}
    if offset:
        params["offset"] = offset

    if limit:
        params["limit"] = limit

    response = requests.get(url, params=params).json()
    data = response["data"]

    replies = [x for x in data if x["reply_time"] is not None]
    for x in replies:
        x["campaign_id"] = campaign_id

    return replies


def get_campaign_info(campaign_id):
    url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}"

    params = {"api_key": SMARTLEADAI_API_KEY}

    response = requests.get(url, params=params).json()

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


if __name__ == "__main__":
    if sys.argv[1] == "get_all_campaigns":
        # List all Campaigns
        url = SMARTLEAD_BASE_URL + "/v1/campaigns"

        params = {"api_key": SMARTLEADAI_API_KEY}

        response = requests.get(url, params=params)

        for x in response.json():
            print(x)

    elif sys.argv[1] == "get_campaign_emails":
        # List all email accounts per campaign
        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/email-accounts"

        params = {"api_key": SMARTLEADAI_API_KEY}

        response = requests.get(url, params=params)

        for x in response.json():
            print(x)

    elif sys.argv[1] == "get_all_leads_by_campaign_id":
        # List all leads by campaign id
        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads"

        print(url)

        params = {"api_key": SMARTLEADAI_API_KEY, "offset": 0, "limit": 1000}

        response = requests.get(url, params=params)

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

        response = requests.get(url, params=params, data=data)

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
