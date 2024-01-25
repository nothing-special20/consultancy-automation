import os, sys
import json
import requests
import traceback

import pandas as pd
#show all pd columns
pd.set_option('display.max_columns', None)

import time

import traceback

from datetime import datetime
import asyncio

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
        requests.post(slack_url, json=payload, headers=headers)

def log_to_file(text):
    log_file = "upwork_lead_automation_log.txt"
    with open(log_file, 'a') as f:
        f.write(text + '\n')


#write an async function that will run the search and post to slack
async def upload_new_records(search_result, search_url):
    new_records = await add_new_values_to_sheet(search_result)

    if new_records is None:
        no_new_records_msg = str(datetime.now()) + "\t" + 'No new records:\t' + search_url
        log_to_file(no_new_records_msg)

    else:
        filtered_records = await upwork.filter_results(new_records)
        filtered_records_msg = 'Number of filtered records:\t' + str(filtered_records.shape[0])
        print(filtered_records_msg)
        log_to_file(filtered_records_msg)
        if filtered_records.shape[0] > 0:
            post_jobs_to_slack(filtered_records, UPWORK_SLACK_CHANNEL_WEBHOOK_URL)
            slack_msg = str(datetime.now()) + "\t" + '# of new jobs posted to slack:\t' + str(filtered_records.shape[0])
            print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            print(slack_msg)
            log_to_file(slack_msg)

# def upload_new_records_old(search_result, search_url):
#     new_records = add_new_values_to_sheet(search_result)

#     if new_records is None:
#         no_new_records_msg = str(datetime.now()) + "\t" + 'No new records:\t' + search_url
#         log_to_file(no_new_records_msg)

#     else:
#         filtered_records = upwork.filter_results(new_records)
#         filtered_records_msg = 'Number of filtered records:\t' + str(filtered_records.shape[0])
#         print(filtered_records_msg)
#         log_to_file(filtered_records_msg)
#         if filtered_records.shape[0] > 0:
#             post_jobs_to_slack(filtered_records, UPWORK_SLACK_CHANNEL_WEBHOOK_URL)
#             slack_msg = str(datetime.now()) + "\t" + '# of new jobs posted to slack:\t' + str(filtered_records.shape[0])
#             print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#             print(slack_msg)
#             log_to_file(slack_msg)

if __name__ == "__main__":
    if sys.argv[1] == 'upwork_lead_automation':
        params_list = upwork.build_params_list()
        # params_list = upwork.build_params_from_kws(['video edit'])
        # print(params_list)
        
        # loop through the same process ad nauseum
        ignore_urls = []
        for i in range(0, 1):
            start_iteration_msg = f'{str(datetime.now())} Iteration {str(i)} - # of urls to search:\t' + str(len(params_list) - len(ignore_urls))
            print('####################################################################')
            print(start_iteration_msg)
            print('####################################################################')
            log_to_file(start_iteration_msg)
            for params in params_list:
                start_search_time = datetime.now()
                search_url = upwork.add_params_to_url(upwork.base_url, params)
                msg = str(datetime.now()) + "\t" + search_url
                log_to_file(msg)

                if search_url in ignore_urls:
                    skip_msg = str(datetime.now()) + "\t" + 'skipping:\t' + search_url
                    log_to_file(skip_msg)
                    continue

                try:
                    rss_fetch_start_time = datetime.now()
                    search_result = rss_to_df(search_url)
                    rss_time_msg = str(datetime.now()) + "\t" + 'RSS fetch time:\t' + str(datetime.now() - rss_fetch_start_time)
                    log_to_file(rss_time_msg)
                    search_result['search_url'] = search_url
                    search_result['search_query'] = params['q']
                    # create a field called word_count that counts the number of words in the description
                    search_result['word_count'] = search_result['description'].apply(lambda x: len(x.split()))

                    # creat a field called field_count that counts the number of characters in the description
                    search_result['char_count'] = search_result['description'].apply(lambda x: len(x))
                    
                    asyncio.run(upload_new_records(search_result, search_url))
                    # upload_new_records_old(search_result, search_url)
                        
                except:
                    error_msg = str(datetime.now()) + "\t" + 'error with:\t' + search_url
                    print(error_msg)
                    log_to_file(error_msg)
                    log_to_file(traceback.format_exc())
                    ignore_urls.append(search_url)
                    print('Number of urls to ignore:\t' + str(len(ignore_urls)))


                end_search_time = datetime.now()
                #if the search took less than 1 second, wait 1 second
                time_delta = end_search_time - start_search_time
                if time_delta.seconds < 1.5:
                    time.sleep(1.5-time_delta.seconds)
                else:
                    time.sleep(0.001)
