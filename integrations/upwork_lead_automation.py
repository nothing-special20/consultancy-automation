import os, sys
import json
import requests

import pandas as pd
#show all pd columns
pd.set_option('display.max_columns', None)

import time

import traceback

from datetime import datetime

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MARKETING_DIR = parent_dir + "/marketing"
UPWORK_DIR = MARKETING_DIR + "/upwork"
GOOGLE_DIR = parent_dir + "/google"
GOOGLE_SHEETS_DIR = GOOGLE_DIR + "/sheets"

for x in [parent_dir, MARKETING_DIR, UPWORK_DIR, GOOGLE_DIR, GOOGLE_SHEETS_DIR]:
    sys.path.append(parent_dir)

from dotenv import dotenv_values
MAIN_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
env_path = MAIN_DIR + ".env"
ENV_VARS = dotenv_values(env_path)

UPWORK_SLACK_CHANNEL_WEBHOOK_URL = ENV_VARS["UPWORK_SLACK_CHANNEL_WEBHOOK_URL"]

from marketing.upwork import job_search as upwork
from marketing.upwork.main import rss_to_df as rss_to_df
from google.sheets.main import add_new_values_to_sheet


def post_jobs_to_slack(jobs_df, slack_url):
    for row in range(jobs_df.shape[0]):
        budget = jobs_df.iloc[row]["budget"]
        title = jobs_df.iloc[row]["title"]
        pay = ""

        if budget != 0:
            pay = "Flat Project Budget: $" + str(budget)

        else:
            lower_hourly_range = jobs_df.iloc[row]["lower_dollar_amount"]
            higher_hourly_range = jobs_df.iloc[row]["upper_dollar_amount"]
            pay = (
                "Hourly Rate: $"
                + str(lower_hourly_range)
                + " - $"
                + str(higher_hourly_range)
            )

        text = title + "\n" + pay + "\n" + jobs_df.iloc[row]["job_url"] + "\n"
        payload = {"text": text}
        headers = {"Content-type": "application/json"}
        response = requests.post(slack_url, json=payload, headers=headers)

        return response

if __name__ == "__main__":
    if sys.argv[1] == 'upwork_lead_automation':
        params_list = upwork.build_params_list()
        # loop through the same process ad nauseum
        for i in range(0, 1000):
            for params in params_list:
                search_url = upwork.add_params_to_url(upwork.base_url, params)
                print(search_url)
                try:
                    search_result = rss_to_df(search_url)
                    search_result['search_url'] = search_url
                    search_result['search_query'] = params['q']
                    # create a field called word_count that counts the number of words in the description
                    search_result['word_count'] = search_result['description'].apply(lambda x: len(x.split()))

                    # creat a field called field_count that counts the number of characters in the description
                    search_result['char_count'] = search_result['description'].apply(lambda x: len(x))
                    new_records = add_new_values_to_sheet(search_result)

                    if new_records is not None:
                        filtered_records = upwork.filter_results(new_records)
                        if filtered_records.shape[0] > 0:
                            post_jobs_to_slack(filtered_records, UPWORK_SLACK_CHANNEL_WEBHOOK_URL)
                            print('# of new jobs posted to slack:\t', filtered_records.shape[0])
                        
                except:
                    print('error with:\t', search_url)

                time.sleep(1.5)