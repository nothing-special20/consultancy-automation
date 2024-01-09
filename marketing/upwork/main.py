import sys
import requests
import re
import time
import os

import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'

from datetime import datetime
import xml.etree.ElementTree as ET

from collections import Counter

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError



from dotenv import dotenv_values

config = dotenv_values(".env")
UPWORK_FOLDER = config["UPWORK_FOLDER"]
SLACK_TOKEN_KEY = config["SLACK_TOKEN_KEY"]

def rss_to_df(url):
    response = requests.get(url)

    if response.status_code > 200:
        print(response.status_code)

    # parse xml
    root = ET.fromstring(response.text)

    # get all items
    items = root.findall("channel/item")

    jobs_list = []

    for item in items:
        title = item.find("title").text
        job_url = item.find("link").text
        description = item.find("description").text
        pub_date = item.find("pubDate").text
        # pub_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")
        pub_date = datetime.strptime(pub_date.split(" +")[0], "%a, %d %b %Y %H:%M:%S")

        try:
            # lower_dollar_amount = float(re.findall("\$(\d+\.\d+)-", description)[0])
            lower_dollar_amount = float(
                re.findall("Hourly Range</b>: \$(\d+\.\d+)", description)[0]
            )
        except:
            lower_dollar_amount = 0

        try:
            upper_dollar_amount = float(re.findall("-\$(\d+\.\d+)", description)[0])
        except:
            upper_dollar_amount = 0

        try:
            budget = str(re.findall("Budget</b>:(.*?)\n", description)[0])
            budget = re.sub("\\$|,", "", budget)
            budget = budget.strip()
            budget = float(budget)
        except:
            budget = 0

        try:
            skills = str(re.findall("<b>Skills</b>:(.*?)\n", description)[0]).strip()
        except:
            skills = "_"

        temp = {
            "title": title,
            "job_url": job_url,
            "description": description,
            "pub_date": pub_date,
            "lower_dollar_amount": lower_dollar_amount,
            "upper_dollar_amount": upper_dollar_amount,
            "budget": budget,
            "skills": skills,
        }

        temp = pd.DataFrame([temp])
        jobs_list.append(temp)

        # print(json.dumps(final, indent=4))

    jobs_df = pd.concat(jobs_list)

    return jobs_df


def num_of_jobs(all_search_job_df, phrase):
    num_jobs = len(
        [
            x
            for x in all_search_job_df["description"].tolist()
            if phrase.lower() in x.lower()
        ]
    )

    print(f"Total number of {phrase} jobs:\t", num_jobs)


def num_of_jobs_by_month(all_search_job_df, phrase):
    # convert pub_date to date
    all_search_job_df["pub_date"] = pd.to_datetime(all_search_job_df["pub_date"])

    all_search_job_df["pub_date"] = [
        x.strftime("%Y-%m") for x in all_search_job_df["pub_date"]
    ]

    all_search_job_df = all_search_job_df[
        [
            phrase.lower() in x.lower()
            for x in all_search_job_df["description"].tolist()
        ]
    ]

    num_jobs_by_month = all_search_job_df.groupby("pub_date").count()["title"].to_dict()

    num_jobs_by_month = {
        k: v for k, v in sorted(num_jobs_by_month.items(), key=lambda item: item[0])
    }

    print(f"Total number of {phrase} jobs by month:\t", num_jobs_by_month)

def fetch_new_jobs():
    search_urls_file = "upwork_urls.xlsx"
    search_urls_tab = "search_urls"

    start_time = datetime.now()

    print("Fetching upwork jobs at:\t", start_time)

    search_urls_df = pd.read_excel(search_urls_file, sheet_name=search_urls_tab)

    search_urls = list(set(search_urls_df["Search URLs"].tolist()))

    all_search_jobs_dfs_list = []

    for url in search_urls:
        try:
            jobs_df = rss_to_df(url)
            all_search_jobs_dfs_list.append(jobs_df)
            time.sleep(1)
        except:
            print("Error fetching data for url:\t", url)

    all_search_job_df = pd.concat(all_search_jobs_dfs_list)
    # all_search_job_df.drop_duplicates(subset=['job_url'], inplace=True)
    all_search_job_df.drop_duplicates(inplace=True)

    print("Finished fetching rss data:\t", datetime.now() - start_time)

    print("Pre-filtered total records:\t", all_search_job_df.shape[0])

    all_search_job_df["pub_date"] = [
        x - pd.Timedelta(hours=5) for x in all_search_job_df["pub_date"]
    ]

    ignore_skills = [
        "excel",
        "solana",
        "video",
        "klaviyo",
        "blockchain",
        "crypto",
        "javascript",
        "sap,",
        "typescript",
        "game dev",
        "C#",
        ".net",
        "3d Model",
        "NinjaTrader",
        "bookkeep",
        "telemarketing",
        "macos",
        "php",
        "3D Model",
        "next.js",
        "grant research",
        "android",
        "desktop appl",
        "appointment set",
        "writing",
        "virtual assis",
        "graphic design",
        "sharepoint",
        "active directory",
        "research",
        "node.js",
        "react.js",
        "react ",
        "Virtualization",
        "scrum",
        "Community Relations",
        "home automation",
        "photography",
        "financial",
        "web design",
        "mobile app",
        "security",
        "india",
        "zoho {0,1}books",
        "game dev",
        "android",
        "ios",
        "quickbooks",
        "next.js",
    ]
    ignore_skills = "|".join(ignore_skills)
    ignore_skills = ignore_skills.lower()
    
    keep_jobs = [
        "pipedrive",
        "highlevel",
        "zoho",
        "hubspot",
        "monday.com",
        "crm",
        "customer relationship management",
        "make.com",
        "zapier",
        "gpt",
        "artificial intelligence",
        "openai",
        "automation",
        "automate",
        "natural language process",
        "phone",
        "airtable",
        "workflow optimization",
        "integration",
        "integrate",
        "dashboard",
        "analytics",
        "langchain",
        "chatbot",
        "huggingface",
        "llama",
    ]
    keep_jobs = "|".join(keep_jobs)
    keep_jobs = keep_jobs.lower()

    all_search_job_df = all_search_job_df[
        (
            all_search_job_df["skills"].str.contains(
                keep_jobs, case=False, regex=True
            )
        )
        | (
            all_search_job_df["title"].str.contains(
                keep_jobs, case=False, regex=True
            )
        )
        | (
            all_search_job_df["description"].str.contains(
                keep_jobs, case=False, regex=True
            )
        )
    ]

    #ignore all records where description, title, or skills contains ignore_skills
    all_search_job_df = all_search_job_df[
        (
            all_search_job_df["skills"].str.contains(
                ignore_skills, case=False, regex=True
            )
        )
        | (
            all_search_job_df["title"].str.contains(
                ignore_skills, case=False, regex=True
            )
        )
        | (
            all_search_job_df["description"].str.contains(
                ignore_skills, case=False, regex=True
            )
        )
        == False
    ]

    # remove all records where title contains xyz
    all_search_job_df = all_search_job_df[
        [
            bool(
                re.search(
                    "sdr|bookkeep|admin|customer serv|cold email|assistant|setter|manager|entry level|sales assoc|sales clos|representative|marketing specialist|sales rep|keyword research|sales special|full[ -]{1}time",
                    x.lower(),
                )
            )
            == False
            for x in all_search_job_df["title"].tolist()
        ]
    ]

    # remove all records where skill contains xyz
    all_search_job_df = all_search_job_df[
        [
            bool(
                re.search(
                    "writing|wordpress|php|video|wix|3d|lead generation|business development|iOS Deve",
                    x.lower(),
                )
            )
            == False
            for x in all_search_job_df["skills"].tolist()
        ]
    ]

    # remove all records where description contains xyz
    all_search_job_df = all_search_job_df[
        [
            bool(re.search("join our team|lead generation|medical|salary", x.lower()))
            == False
            for x in all_search_job_df["description"].tolist()
        ]
    ]

    # sort all_search_job_df by pub_date descending
    all_search_job_df = all_search_job_df.sort_values(
        by=["pub_date"], ascending=False
    )

    # for crm_name in ["pipedrive", "highlevel", "hubspot", "salesforce", "monday", "zoho", "gpt", "artificial intelligence", "openai", "langchain"]:
    for crm_name in ["zoho", "hubspot", "gpt", "automation", "langchain", "make.com"]:
        all_search_job_df[crm_name + "_flag"] = ""
        all_search_job_df[crm_name + "_flag"][
            [
                bool(re.search(crm_name, x.lower()))
                for x in all_search_job_df["description"].tolist()
            ]
        ] = "X"

    # only include jobs with in the last N hours
    last_n_hours = 24
    all_search_job_df_latest = all_search_job_df[
        all_search_job_df["pub_date"]
        > (datetime.now() - pd.Timedelta(hours=last_n_hours))
    ]

    # all_search_job_df_latest.to_excel("upwork_jobs.xlsx", index=False)
    non_flag_columns = [x for x in all_search_job_df_latest.columns if "_flag" not in x]
    gpt_jobs = all_search_job_df_latest[all_search_job_df_latest["gpt_flag"] == "X"][non_flag_columns]
    zoho_jobs = all_search_job_df_latest[all_search_job_df_latest["zoho_flag"] == "X"][non_flag_columns]
    langchain_jobs = all_search_job_df_latest[all_search_job_df_latest["langchain_flag"] == "X"][non_flag_columns]
    make_jobs = all_search_job_df_latest[all_search_job_df_latest["make.com_flag"] == "X"][non_flag_columns]
    #add gpt_jbos to upwork_jobs.xlsx
    # gpt_jobs.to_excel("upwork_jobs.xlsx", index=False, sheet_name="gpt_jobs")

    with pd.ExcelWriter("upwork_jobs.xlsx", engine='xlsxwriter') as writer:
    # Write each dataframe to a different worksheet
        all_search_job_df_latest.to_excel(writer, sheet_name='all_jobs', index=False)
        gpt_jobs.to_excel(writer, sheet_name='gpt_jobs', index=False)
        zoho_jobs.to_excel(writer, sheet_name='zoho_jobs', index=False)
        langchain_jobs.to_excel(writer, sheet_name='langchain_jobs', index=False)
        make_jobs.to_excel(writer, sheet_name='make_jobs', index=False)


    all_search_job_df.to_csv(
        UPWORK_FOLDER + "upwork_jobs-" + str(datetime.now()) + ".csv", index=False
    )

    print("Total time taken:\t", datetime.now() - start_time)

    return all_search_job_df

def all_jobs():
    all_files = [
        UPWORK_FOLDER + x for x in os.listdir(UPWORK_FOLDER) if ".csv" in x
    ]

    all_data = [pd.read_csv(x) for x in all_files]
    all_data = pd.concat(all_data)
    all_data.drop_duplicates(inplace=True)
    all_data = all_data.sort_values(by=["pub_date"], ascending=False)

    return all_data

def post_jobs_to_slack(jobs_df, slack_url):
    for row in range(jobs_df.shape[0]):
        title = jobs_df.iloc[row]['title']
        if title not in already_loaded:
            already_loaded.append(title)
            budget = jobs_df.iloc[row]['budget']

            pay = ''

            if budget != 0:
                pay = 'Flat Project Budget: $' + str(budget)
            
            else:
                lower_hourly_range = jobs_df.iloc[row]['lower_dollar_amount']
                higher_hourly_range = jobs_df.iloc[row]['upper_dollar_amount']
                pay = 'Hourly Rate: $' + str(lower_hourly_range) + ' - $' + str(higher_hourly_range)

            text = title + '\n' + pay + '\n' + jobs_df.iloc[row]['job_url'] + '\n'

            payload = {'text': text}
            print(text)
            response = requests.post(slack_url, json=payload, headers=headers)

def jobs_filter(jobs_df, phrases):
    return jobs_df[
        (
            jobs_df["skills"].str.contains(
                phrases, case=False, regex=True
            )
        )
        | (
            jobs_df["title"].str.contains(
                phrases, case=False, regex=True
            )
        )
        | (
            jobs_df["description"].str.contains(
                phrases, case=False, regex=True
            )
        )
    ]

if __name__ == "__main__":
    if sys.argv[1] == "upwork_find_best_jobs":
        all_search_job_df = fetch_new_jobs()

        last_n_hours = 24
        latest_search_job_df = all_search_job_df[
            all_search_job_df["pub_date"]
            > (datetime.now() - pd.Timedelta(hours=last_n_hours))
        ]

        all_skills = latest_search_job_df["skills"].tolist()

        all_skills = ", ".join(all_skills)
        all_skills = all_skills.lower()
        all_skills = re.sub(" {2,9}", " ", all_skills.strip())
        all_skills = all_skills.split(", ")

        most_common_skills = Counter(all_skills).most_common(25)

        all_skills = list(set(all_skills))
        all_skills.sort()

        # for x in all_skills:
        #     print(x)

        num_of_jobs(latest_search_job_df, "pipedrive")
        num_of_jobs(latest_search_job_df, "highlevel")
        num_of_jobs(latest_search_job_df, "hubspot")
        num_of_jobs(latest_search_job_df, "salesforce")
        num_of_jobs(latest_search_job_df, "monday")
        num_of_jobs(latest_search_job_df, "zoho")
        num_of_jobs(latest_search_job_df, "langchain")
        num_of_jobs(latest_search_job_df, "gpt")

        print("Post-filtered total records:\t", latest_search_job_df.shape[0])

    if sys.argv[1] == "upwork_job_trends":
        all_data = all_jobs()

        # all_data.to_excel('all_upwork_jobs.xlsx', index=False)
        all_data.to_csv("all_upwork_jobs.csv", index=False)
        # print(all_data)

        num_of_jobs(all_data, "pipedrive")
        num_of_jobs(all_data, "highlevel")
        num_of_jobs(all_data, "hubspot")
        num_of_jobs(all_data, "salesforce")
        num_of_jobs(all_data, "monday")
        num_of_jobs(all_data, "zoho")
        num_of_jobs(all_data, "langchain")
        num_of_jobs(all_data, "retrieval augmented")
        num_of_jobs(all_data, "gpt")

        num_of_jobs_by_month(all_data, "zoho")
        num_of_jobs_by_month(all_data, "gpt")

        all_skills = all_data["skills"].tolist()

        all_skills = ", ".join(all_skills)
        all_skills = all_skills.lower()
        all_skills = re.sub(" {2,9}", " ", all_skills.strip())
        all_skills = all_skills.split(", ")

        most_common_skills = Counter(all_skills).most_common(25)

        print("most common skills")
        for x in most_common_skills:
            print(x)

    if sys.argv[1] == 'upload_to_slack_test':
        client = WebClient(token=SLACK_TOKEN_KEY)
        random_channel_webhook_url = 'https://hooks.slack.com/services/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'
        upwork_jobs_channel_url = 'https://hooks.slack.com/services/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'

        payload = {'text': 'Hey it worked!!!!'}
        headers = {'Content-type': 'application/json'}

        response = requests.post(random_channel_webhook_url, json=payload, headers=headers)

    if sys.argv[1] == 'post_jobs_to_slack':
        upwork_jobs_channel_url = 'https://hooks.slack.com/services/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'
        hubspot_jobs_channel_url = 'https://hooks.slack.com/services/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'

        headers = {'Content-type': 'application/json'}

        fetch_new_jobs()

        all_jobs_df = all_jobs()

        all_jobs_df["pub_date"] = pd.to_datetime(all_jobs_df["pub_date"])
        all_jobs_df["pub_date"] = [
            x - pd.Timedelta(hours=5) for x in all_jobs_df["pub_date"]
        ]

        last_n_minutes = 900
        all_jobs_df = all_jobs_df[
            all_jobs_df["pub_date"]
            > (datetime.now() - pd.Timedelta(minutes=last_n_minutes))
        ]

        hubspot_jobs = jobs_filter(all_jobs_df, 'hubspot')

        #for row in hubspot_jobs
        already_loaded = []

        post_jobs_to_slack(hubspot_jobs, hubspot_jobs_channel_url)

        gpt_jobs = jobs_filter(all_jobs_df, 'gpt|langchain')
        print(all_jobs_df)
        # post_jobs_to_slack(all_jobs_df, upwork_jobs_channel_url)


