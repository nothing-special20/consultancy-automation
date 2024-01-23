import requests
from urllib.parse import urlencode
import pandas as pd
import sys, os

import time
import html

from datetime import datetime

#pandas show all columns
pd.set_option('display.max_columns', None)

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from main import rss_to_df

base_url = "https://www.upwork.com/ab/feed/jobs/rss"

from urllib.parse import urlencode

def add_params_to_url(base_url, params):
    return base_url + '?' + urlencode(params)

def concatenate_search_from_queries(params_list):
    data_list = []
    for params in params_list:
        search_url = add_params_to_url(base_url, params)
        print(search_url)
        try:
            search_result = rss_to_df(search_url)
            search_result['search_url'] = search_url
            search_result['search_query'] = params['q']
            print(search_result.shape)
            data_list.append(search_result)
        except:
            print('error with:\t', search_url)

        time.sleep(1.5)

    df = pd.concat(data_list)
    df.drop_duplicates(inplace=True)
    return df

def build_params_from_kws(kw_list):
    params_list = []
    for kw in kw_list:
        params_list.append({"q": kw, "sort": "recency", "paging": "0"})
    return params_list

def filter_results(df):
    ignore_descriptions = ["join our team"]
    ignore_descriptions = '|'.join(ignore_descriptions)

    df = df[~df['description'].str.contains(ignore_descriptions, case=False)]

    df = df[(df['upper_dollar_amount'] > 30) | (df['budget'] > 500)]

    return df

def remove_titles(df):
    rm_titles = ["appointment setter", "sales development rep", "wordpress", "android", "ios"]
    rm_titles = '|'.join(rm_titles)

    df = df[~df['title'].str.contains(rm_titles, case=False)]

    return df


def build_params_list():
    base_kw_list = ["zoho crm", "zoho flow", "zoho", "make.com", "make.com", "airtable", "crm cleaning", "django", "langchain", "zapier", "gpt", "smartlead.ai", "apollo.io", "automation", "marketing automation", "automated workflow", "integrate", "integration", "crm integration", "crm", "crm automation", "email automation", "Customer Relationship Management", "project workflows", "api integration", "business process automation", "streamline", "data enrichment", "data cleaning", "data cleansing", "neverbounce", "never bounce", "million verifier", "crm consultant", "AI Systems", "automation systems", "ai automation", "ai business", "business automation", "sales process", "sales automation", "digital marketing automation", "automation expert", "digital transformation", "llm", "large language model","generative ai", "spacy", "hugging face", "air table", "calendly", "integrations", "tech stack automation", "omnichannel automation", "notion", "lucidchart", "lucid chart", "airtable system"]

    base_kw_list = list(set(base_kw_list))

    title_searches = [f'title:("{kw}")' for kw in base_kw_list]
    description_searches = [f'description:("{kw}")' for kw in base_kw_list]

    all_searches = title_searches + description_searches

    #ignore searches
    ignore_searches = ["PHP", "video editor", "blockchain", "trading automation", ".net", "zoho books", "appointment setter", "sales development rep", "wordpress", "android", "ios"]
    ignore_searches = [f'"{x}"' for x in ignore_searches]
    ignore_searches = " OR ".join(ignore_searches)
    ignore_searches = f" AND NOT ({ignore_searches})"
    all_searches = [x + ignore_searches  for x in all_searches]

    params_list = build_params_from_kws(all_searches)

    return params_list

if __name__ == "__main__":
    if sys.argv[1] == 'test':
        # Example usage
        # params = {"q": 'title:("zoho crm")', "sort": "recency", "paging": "0"}

        params_list = build_params_list()

        for x in params_list:
            print(x)

        result = concatenate_search_from_queries(params_list)

        result['description'] = result['description'].apply(lambda x: html.unescape(x))

        # create a field called word_count that counts the number of words in the description
        result['word_count'] = result['description'].apply(lambda x: len(x.split()))

        # creat a field called field_count that counts the number of characters in the description
        result['char_count'] = result['description'].apply(lambda x: len(x))

        #convert pub date to datetime
        result['pub_date'] = pd.to_datetime(result['pub_date'])
        result["pub_date"] = [x - pd.Timedelta(hours=5) for x in result["pub_date"]]
        #convert utc time to etc

        print('1', result.shape)

        result = filter_results(result)
        print('2', result.shape)
        result = remove_titles(result)
        print('3', result.shape)

        result.sort_values(by=['pub_date'], inplace=True, ascending=False)

        #sort by pub_date descending

        result.to_csv("upwork_search_results_new.csv", index=False)


        #unique jobs
        unique_jobs = result.drop_duplicates(subset=['job_url'])

        unique_jobs.to_csv("upwork_search_results_new_UNIQUE.csv", index=False)

        # # # # print(result)
        # print(result.shape)
        print(datetime.now())