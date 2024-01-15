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

env_path = os.path.dirname(os.path.realpath(__file__))+ "/.env"
config = dotenv_values(env_path)

UPWORK_FOLDER = config["UPWORK_FOLDER"]
SLACK_TOKEN_KEY = config["SLACK_TOKEN_KEY"]


def rss_to_df(url):
    response = requests.get(url, timeout=5)

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
        list(
            set(
                [
                    x
                    for x in all_search_job_df["description"].tolist()
                    if phrase.lower() in x.lower()
                ]
            )
        )
    )

    print(f"Total number of {phrase} jobs:\t", num_jobs)


def num_of_jobs_by_month(all_search_job_df, phrase):
    # convert pub_date to date
    all_search_job_df["pub_date"] = pd.to_datetime(all_search_job_df["pub_date"])

    all_search_job_df["pub_date"] = [
        x.strftime("%Y-%m") for x in all_search_job_df["pub_date"]
    ]

    # all_search_job_df = all_search_job_df[
    #     [phrase.lower() in x.lower() for x in all_search_job_df["description"].tolist()]
    # ]

    all_search_job_df = all_search_job_df[
        all_search_job_df["title"].str.contains(phrase, case=False, regex=True)
    ]

    all_search_job_df = all_search_job_df[["title", "pub_date"]]
    # only get the first record per title
    all_search_job_df.drop_duplicates(subset=["title"], inplace=True)

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
    # search_urls_df = search_urls_df.tail(1)

    all_search_jobs_dfs_list = []

    counter = 0

    for record in search_urls_df.to_dict(orient="records"):
        url = record["Search URLs"]
        file_name = (
            UPWORK_FOLDER
            + "upwork_jobs-"
            + record["Search Type"]
            + "-"
            + str(datetime.now())
            + ".csv"
        )
        counter += 1
        try:
            jobs_df = rss_to_df(url)
            jobs_df["pub_date"] = [
                x - pd.Timedelta(hours=5) for x in jobs_df["pub_date"]
            ]
            jobs_df.to_csv(file_name, index=False)
            all_search_jobs_dfs_list.append(jobs_df)
            # time.sleep(1.75)
            time.sleep(1.5)
        except:
            print("Error fetching data for url:\t", url)

        print(
            "finished fetching rss data from url:\t",
            counter,
            "of",
            search_urls_df.shape[0],
        )

    all_search_job_df = pd.concat(all_search_jobs_dfs_list)
    all_search_job_df.drop_duplicates(inplace=True)

    #######

    print("Finished fetching rss data:\t", datetime.now() - start_time)

    # export raw results to csv
    # all_search_job_df.to_csv(
    #     UPWORK_FOLDER + "upwork_jobs-" + str(datetime.now()) + ".csv", index=False
    # )

    print("Pre-filtered total records:\t", all_search_job_df.shape[0])

    all_search_job_df["pub_date"] = [
        x - pd.Timedelta(hours=5) for x in all_search_job_df["pub_date"]
    ]

    ignore_skills = [
        "microsoft excel",
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
        "ninjatrader",
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
        # "research",
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
        "mobile app dev",
    ]
    ignore_skills = "|".join(ignore_skills)
    ignore_skills = ignore_skills.lower()

    ignore_from_everything = [
        "mobile app",
        "security",
        "india",
        "zoho {0,1}books",
        "game dev",
        "android",
        "ios",
        "quickbooks",
        "next.js",
        "trading bot",
        "blockchain",
    ]
    ignore_from_everything = "|".join(ignore_from_everything)
    ignore_from_everything = ignore_from_everything.lower()

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
        "slack",
        "retrieval augmented",
        "machine learning",
        "django",
        "data mining",
    ]
    keep_jobs = "|".join(keep_jobs)
    keep_jobs = keep_jobs.lower()

    all_search_job_df = all_search_job_df[
        (all_search_job_df["skills"].str.contains(keep_jobs, case=False, regex=True))
        | (all_search_job_df["title"].str.contains(keep_jobs, case=False, regex=True))
        | (
            all_search_job_df["description"].str.contains(
                keep_jobs, case=False, regex=True
            )
        )
    ]

    print("Total jobs before removing ignored skills:\t", all_search_job_df.shape[0])

    # ignore all records where description, title, or skills contains ignore_skills
    all_search_job_df = all_search_job_df[
        (
            all_search_job_df["skills"].str.contains(
                ignore_skills, case=False, regex=True
            )
        )
        == False
    ]

    all_search_job_df = all_search_job_df[
        (
            all_search_job_df["skills"].str.contains(
                ignore_from_everything, case=False, regex=True
            )
        )
        | (
            all_search_job_df["title"].str.contains(
                ignore_from_everything, case=False, regex=True
            )
        )
        | (
            all_search_job_df["description"].str.contains(
                ignore_from_everything, case=False, regex=True
            )
        )
        == False
    ]

    print("Total jobs after removing ignored skills:\t", all_search_job_df.shape[0])

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
    all_search_job_df = all_search_job_df.sort_values(by=["pub_date"], ascending=False)

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

    all_search_job_df_latest = all_search_job_df

    # all_search_job_df_latest.to_excel("upwork_jobs.xlsx", index=False)
    non_flag_columns = [x for x in all_search_job_df_latest.columns if "_flag" not in x]
    gpt_jobs = all_search_job_df_latest[all_search_job_df_latest["gpt_flag"] == "X"][
        non_flag_columns
    ]
    zoho_jobs = all_search_job_df_latest[all_search_job_df_latest["zoho_flag"] == "X"][
        non_flag_columns
    ]
    langchain_jobs = all_search_job_df_latest[
        all_search_job_df_latest["langchain_flag"] == "X"
    ][non_flag_columns]
    make_jobs = all_search_job_df_latest[
        all_search_job_df_latest["make.com_flag"] == "X"
    ][non_flag_columns]
    # add gpt_jbos to upwork_jobs.xlsx
    # gpt_jobs.to_excel("upwork_jobs.xlsx", index=False, sheet_name="gpt_jobs")

    with pd.ExcelWriter("upwork_jobs.xlsx", engine="xlsxwriter") as writer:
        # Write each dataframe to a different worksheet
        all_search_job_df_latest.to_excel(writer, sheet_name="all_jobs", index=False)
        gpt_jobs.to_excel(writer, sheet_name="gpt_jobs", index=False)
        zoho_jobs.to_excel(writer, sheet_name="zoho_jobs", index=False)
        langchain_jobs.to_excel(writer, sheet_name="langchain_jobs", index=False)
        make_jobs.to_excel(writer, sheet_name="make_jobs", index=False)

    # sort all_search_job_df by descending pub_date
    all_search_job_df = all_search_job_df.sort_values(by=["pub_date"], ascending=False)

    print("Total time taken:\t", datetime.now() - start_time)

    return all_search_job_df


def all_jobs():
    all_files = [UPWORK_FOLDER + x for x in os.listdir(UPWORK_FOLDER) if ".csv" in x]

    all_data = [pd.read_csv(x) for x in all_files]
    all_data = pd.concat(all_data)
    all_data.drop_duplicates(inplace=True)
    all_data = all_data.sort_values(by=["pub_date"], ascending=False)

    return all_data

def main_jobs_filter(jobs_df):
    remove_from_everything = [
        "trading",
        "visual basic",
        "full[ -]time",
        "keap",
        ".net",
        "pharma",
        "casino",
        "gambl",
        "photoshop",
        "woocommerce",
        "brand strategy",
        "angular",
        "wix",
    ]

    keep_descriptions = []
    keep_descriptions = "|".join(keep_descriptions)
    keep_descriptions = keep_descriptions.lower()

    jobs_df = jobs_df[
        (jobs_df["description"].str.contains(keep_descriptions, case=False, regex=True))
    ]

    remove_descriptions = remove_from_everything + ["zoho {0,1}book"]
    remove_descriptions = "|".join(remove_descriptions)
    remove_descriptions = remove_descriptions.lower()

    jobs_df = jobs_df[
        (
            jobs_df["description"].str.contains(
                remove_descriptions, case=False, regex=True
            )
        )
        == False
    ]

    remove_titles = remove_from_everything + [
        "director",
        "ux design",
        "virtual_assistant",
        "senior",
        "crypto",
        "ghostwrite",
        "sr\\.",
        "assistant",
        "manager",
        "media marketer",
        "clickup",
    ]
    remove_titles = "|".join(remove_titles)
    remove_titles = remove_titles.lower()

    jobs_df = jobs_df[
        (jobs_df["title"].str.contains(remove_titles, case=False, regex=True)) == False
    ]

    remove_skills = remove_from_everything + [
        "mechanical",
        "3d",
        "mobile app",
        "php",
        "content writing",
        "video",
        "android",
        "ios dev",
    ]
    remove_skills = "|".join(remove_skills)
    remove_skills = remove_skills.lower()

    jobs_df = jobs_df[
        (jobs_df["skills"].str.contains(remove_skills, case=False, regex=True)) == False
    ]

    return jobs_df


def jobs_analysis(jobs_df):
    for flag_phrase in [
        "zoho",
        "hubspot",
        "gpt",
        "automation",
        "langchain",
        "make.com",
        "integrat",
        "machine learn",
        "zapier",
    ]:
        jobs_df[flag_phrase + "_flag"] = ""
        jobs_df[flag_phrase + "_flag"][
            [
                bool(re.search(flag_phrase, x.lower()))
                for x in jobs_df["description"].tolist()
            ]
        ] = "X"

    return jobs_df


def post_jobs_to_slack(jobs_df, slack_url):
    for row in range(jobs_df.shape[0]):
        title = jobs_df.iloc[row]["title"]
        if title not in already_loaded:
            already_loaded.append(title)
            budget = jobs_df.iloc[row]["budget"]

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
            print(text)
            response = requests.post(slack_url, json=payload, headers=headers)


def jobs_filter(jobs_df, phrases):
    return jobs_df[
        (jobs_df["skills"].str.contains(phrases, case=False, regex=True))
        | (jobs_df["title"].str.contains(phrases, case=False, regex=True))
        | (jobs_df["description"].str.contains(phrases, case=False, regex=True))
    ]


def most_common_skills(jobs_df):
    print(jobs_df.shape)
    skills = jobs_df["skills"].tolist()

    skills = ", ".join(skills)
    skills = skills.lower()
    skills = re.sub(" {2,9}", " ", skills.strip())
    skills = skills.split(", ")

    _most_common_skills = Counter(skills).most_common(50)

    return _most_common_skills


def find_jobs_by_phrase(jobs_df, skill):
    return jobs_df[
        (jobs_df["skills"].str.contains(skill, case=False, regex=True))
        | (jobs_df["title"].str.contains(skill, case=False, regex=True))
        | (jobs_df["description"].str.contains(skill, case=False, regex=True))
    ]

def best_jobs_flag(record):
    best_jobs_list = ["zoho(?!\\s+ book)", "langchain", "gpt", "make.com"]
    best_jobs_list = "|".join(best_jobs_list)
    best_jobs_list = best_jobs_list.lower()

    if (
        (
            bool(re.search(best_jobs_list, record["skills"].lower()))
            | bool(re.search(best_jobs_list, record["title"].lower()))
            | bool(re.search(best_jobs_list, record["description"].lower()))
        )
        & (
            (record["lower_dollar_amount"] > float(15))
            | (record['budget'] > float(299))
        )
    ):
        return "X"
    else:
        return ""

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

        most_common_skills_all = Counter(all_skills).most_common(50)

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
        num_of_jobs(latest_search_job_df, "pipedrive")
        num_of_jobs(latest_search_job_df, "langchain")
        num_of_jobs(latest_search_job_df, "gpt")

        print("Post-filtered total records:\t", latest_search_job_df.shape[0])

    if sys.argv[1] == "upwork_job_trends":
        all_data = all_jobs()
        all_data.drop_duplicates(subset=["title"], inplace=True)
        all_data["pub_date"] = pd.to_datetime(all_data["pub_date"])

        # all_data.to_excel('all_upwork_jobs.xlsx', index=False)
        all_data.to_csv("all_upwork_jobs.csv", index=False)
        # print(all_data)

        # num_of_jobs(all_data, "pipedrive")
        # num_of_jobs(all_data, "highlevel")
        # num_of_jobs(all_data, "hubspot")
        # num_of_jobs(all_data, "salesforce")
        # num_of_jobs(all_data, "monday")
        # num_of_jobs(all_data, "zoho")
        # num_of_jobs(all_data, "langchain")
        # num_of_jobs(all_data, "retrieval augmented")
        # num_of_jobs(all_data, "gpt")

        num_of_jobs_by_month(all_data, "zoho(?!\\s+ book)")
        num_of_jobs_by_month(all_data, "pipedrive")
        num_of_jobs_by_month(all_data, "gpt")
        num_of_jobs_by_month(all_data, "langchain")
        num_of_jobs_by_month(all_data, "openai")
        num_of_jobs_by_month(all_data, "make.com")
        num_of_jobs_by_month(all_data, "data entry")

        most_common_skills_all = most_common_skills(all_data)

        # print("most common skills")
        # for x in most_common_skills_all:
        #     print(x)

        # print("most common skills associated with 'gpt'")
        # gpt_data = find_jobs_by_phrase(all_data, "gpt")
        # most_common_skills_gpt = most_common_skills(gpt_data)

        # for x in most_common_skills_gpt:
        #     print(x)

        # print("most common skills associated with 'automation'")
        # automation_data = find_jobs_by_phrase(all_data, "automation")
        # most_common_skills_automation = most_common_skills(automation_data)
        # for x in most_common_skills_automation:
        #     print(x)

        print("most common skills associated with 'zoho'")
        zoho_data = find_jobs_by_phrase(all_data, "zoho")
        most_common_skills_zoho = most_common_skills(zoho_data)
        for x in most_common_skills_zoho:
            print(x)

    if sys.argv[1] == "upload_to_slack_test":
        client = WebClient(token=SLACK_TOKEN_KEY)
        random_channel_webhook_url = "https://hooks.slack.com/services/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"
        upwork_jobs_channel_url = "https://hooks.slack.com/services/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"

        payload = {"text": "Hey it worked!!!!"}
        headers = {"Content-type": "application/json"}

        response = requests.post(
            random_channel_webhook_url, json=payload, headers=headers
        )

    if sys.argv[1] == "post_jobs_to_slack":
        upwork_jobs_channel_url = "https://hooks.slack.com/services/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"
        hubspot_jobs_channel_url = "https://hooks.slack.com/services/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"

        headers = {"Content-type": "application/json"}

        # fetch_new_jobs()

        all_jobs_df = all_jobs()

        all_jobs_df["pub_date"] = pd.to_datetime(all_jobs_df["pub_date"])
        # all_jobs_df["pub_date"] = [
        #     x - pd.Timedelta(hours=5) for x in all_jobs_df["pub_date"]
        # ]

        print(all_jobs_df.shape)

        # unique_pub_dates = list(set(all_jobs_df["pub_date"].tolist()))
        # unique_pub_dates.sort()

        # for x in unique_pub_dates:
        #     print(x)

        last_n_minutes = 600
        all_jobs_df = all_jobs_df[
            all_jobs_df["pub_date"]
            > (datetime.now() - pd.Timedelta(minutes=last_n_minutes))
        ]

        print(all_jobs_df.shape)

        hubspot_jobs = jobs_filter(all_jobs_df, "hubspot")
        print(hubspot_jobs)

        # for row in hubspot_jobs
        already_loaded = []

        post_jobs_to_slack(hubspot_jobs, hubspot_jobs_channel_url)

        # gpt_jobs = jobs_filter(all_jobs_df, 'gpt|langchain')
        # print(all_jobs_df)
        # post_jobs_to_slack(all_jobs_df, upwork_jobs_channel_url)

    if sys.argv[1] == "aggregate_jobs":
        start_time = datetime.now()
        print("start time:\t", start_time)
        all_jobs_df = all_jobs()
        all_jobs_df = main_jobs_filter(all_jobs_df)
        all_jobs_df = jobs_analysis(all_jobs_df)
        all_jobs_df.drop_duplicates(subset=["title"], inplace=True)
        all_jobs_df["pub_date"] = pd.to_datetime(all_jobs_df["pub_date"])

        all_jobs_df = all_jobs_df.sort_values(by=["pub_date"], ascending=False)

        all_jobs_df = all_jobs_df[
            (all_jobs_df["budget"] > 199) | (all_jobs_df["budget"] == 0)
        ]

        all_jobs_df["any_flag"] = ""
        # if any of the columns with the word 'flag' in it contains an 'X', then set any_flag to 'X'
        all_jobs_df["any_flag"] = all_jobs_df[
            [x for x in all_jobs_df.columns if "flag" in x]
        ].apply(lambda x: "X" if "X" in x.values else "", axis=1)

        all_jobs_df['best_job_flag'] = all_jobs_df.apply(lambda x: best_jobs_flag(x), axis=1)

        all_jobs_df.to_csv("all_upwork_jobs.csv", index=False)

        print('total run time:\t', datetime.now() - start_time)
