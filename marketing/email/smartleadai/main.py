import requests
import sys
import json

from dotenv import dotenv_values

config = dotenv_values(".env")

SMARTLEADAI_API_KEY = config['SMARTLEADAI_API_KEY']
SMARTLEAD_BASE_URL = 'https://server.smartlead.ai/api'

#documentation
#https://help.smartlead.ai/API-Documentation-a0d223bdd3154a77b3735497aad9419f#23f380d414b847bcaa25ab6f7b752196

if __name__ == '__main__':
    if sys.argv[1] == 'get_all_campaigns':
        # List all Campaigns
        url = SMARTLEAD_BASE_URL + '/v1/campaigns'

        params = {'api_key': SMARTLEADAI_API_KEY}

        response = requests.get(url, params=params)

        for x in response.json():
            print(x)

    elif sys.argv[1] == 'get_campaign_emails':
        # List all email accounts per campaign
        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/email-accounts"

        params = {'api_key': SMARTLEADAI_API_KEY}

        response = requests.get(url, params=params)

        for x in response.json():
            print(x)

    elif sys.argv[1] == 'get_all_leads_by_campaign_id':
        #List all leads by campaign id
        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads"

        print(url)

        params = {'api_key': SMARTLEADAI_API_KEY,
                "offset": 0,
                "limit": 1000
                }

        response = requests.get(url, params=params)

        try:
            response_json = response.json()

            data = response_json['data']

            for x in data:
                print(x)

            print(len(data))
        except:
            print(response.text)

    elif sys.argv[1] == 'reply_to_lead':
        #Reply To Lead From Master Inbox via API
        pass

    elif sys.argv[1] == 'Add leads to a campaign by ID':
        # Add leads to a campaign by ID
        # https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/leads?api_key={API_KEY}
        # --data '{"lead_list": List<lead_input> *(max 100 leads)}'

        campaign_id = str(sys.argv[2])
        url = SMARTLEAD_BASE_URL + f"/v1/campaigns/{campaign_id}/leads"

        params = {'api_key': SMARTLEADAI_API_KEY}

        lead_list = []

        data = {
            'lead_list': lead_list,
            "settings": {
            "ignore_global_block_list": True, # true ignores leads uploaded in the lead list that are part of your global/client level block list
            "ignore_unsubscribe_list": True, # true ignores leads uploaded in the lead list that have unsubsribed previously
            "ignore_duplicate_leads_in_other_campaign": False # false allows leads to be added to this campaign even if they exist in another campaign
        }
        }

        response = requests.get(url, params=params, data=data)

        for x in response.json():
            print(x)