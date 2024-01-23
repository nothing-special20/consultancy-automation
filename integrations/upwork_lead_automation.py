import os, sys
import json

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

from marketing.upwork import job_search as upwork
from marketing.upwork.main import rss_to_df as rss_to_df
from google.sheets.main import add_new_values_to_sheet

if __name__ == "__main__":
    if sys.argv[1] == 'upwork_lead_automation':
        params_list = upwork.build_params_list()

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
                add_new_values_to_sheet(search_result)
            except:
                print('error with:\t', search_url)

            time.sleep(1.5)