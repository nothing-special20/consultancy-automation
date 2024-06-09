import os, sys
import json
import requests
import traceback

import pandas as pd
#show all pd columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1200)

import time

import traceback

from datetime import datetime

from dotenv import dotenv_values
ANALYTICS_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
env_path = ANALYTICS_DIR + ".env"
ENV_VARS = dotenv_values(env_path)

SMARTLEAD_GOOGLE_SHEET_ID = ENV_VARS["SMARTLEAD_GOOGLE_SHEET_ID"]

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MARKETING_DIR = parent_dir + "/marketing"
EMAIL_DIR = MARKETING_DIR + "/email"
SMARTLEAD_DIR = EMAIL_DIR + "/smartlead"
GOOGLE_DIR = parent_dir + "/google"
GOOGLE_SHEETS_DIR = GOOGLE_DIR + "/sheets"

for x in [parent_dir, MARKETING_DIR, EMAIL_DIR, SMARTLEAD_DIR, GOOGLE_DIR, GOOGLE_SHEETS_DIR]:
    sys.path.append(parent_dir)

from marketing.email.smartleadai.main import get_all_campaign_data, get_campaign_statistics, campaign_statistics_df
from google.sheets.main import google_append_sheet, clear_all_sheet_values

if __name__ == '__main__':
    if sys.argv[1] == "update_campaign_data":
        all_campaign_data = get_all_campaign_data()

        print(f"about to load data to sheet for {len(all_campaign_data)} campaigns")

        for data in all_campaign_data:
            campaign_id = data["id"]
            campaign_name = data["name"]

            print("campaign name:\t", campaign_name)

            campaign_stats = get_campaign_statistics(campaign_id)

            record_count = int(campaign_stats["total_stats"])

            print("record count:\t", record_count)

            #round record_count up to 100
            record_count_rounded_up = record_count + 100 - record_count % 100
            page_count = record_count_rounded_up // 100

            for offset in range(0, page_count):
                iteration_start_time = time.time()
                campaign_stats = get_campaign_statistics(campaign_id, offset * 100)
                campaign_stats_df = campaign_statistics_df(campaign_stats)
                campaign_stats_df["campaign_name"] = campaign_name
                col_names = [campaign_stats_df.columns.values.tolist()]
                campaign_stats_values = campaign_stats_df.values.tolist()

                if data["id"] == all_campaign_data[0]["id"] and offset == 0:
                    clear_all_sheet_values(SMARTLEAD_GOOGLE_SHEET_ID, "campaigns")
                    google_append_sheet(col_names,  SMARTLEAD_GOOGLE_SHEET_ID, "campaigns")
                    print("clearing sheet")

                google_append_sheet(campaign_stats_values, SMARTLEAD_GOOGLE_SHEET_ID, "campaigns")
                print(f"added {len(campaign_stats_values)} records to sheet")
                total_time = time.time() - iteration_start_time
                print(f"iteration {offset} took {total_time} seconds")
                if total_time < 1.6:
                    time.sleep(1.6 - total_time)
            