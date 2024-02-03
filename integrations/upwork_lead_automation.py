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
UPWORK_AI_IMG_SLACK_CHANNEL_WEBHOOK_URL = ENV_VARS["UPWORK_AI_IMG_SLACK_CHANNEL_WEBHOOK_URL"]

from marketing.upwork import job_search as upwork
from marketing.upwork.main import rss_to_df as rss_to_df
from google.sheets import main as gsheets

from upwork_machine_learning_analysis import hf_topic_classification

def post_jobs_to_slack(jobs_df):
    for row in range(jobs_df.shape[0]):
        budget = jobs_df.iloc[row]["budget"]
        title = jobs_df.iloc[row]["title"]
        pay = ""

        slack_url = UPWORK_SLACK_CHANNEL_WEBHOOK_URL

        # topics = ["other", "image or design related"]
        # title_tag = hf_topic_classification(title, topics)

        keywords = ["graphic", "illustration", "image", "photo"]
        keywords = [x.lower() for x in keywords]

        if any([x in title.lower() for x in keywords]):
            slack_url = UPWORK_AI_IMG_SLACK_CHANNEL_WEBHOOK_URL

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
def upload_new_records(search_result):
    try:
        search_urls = "\t".join(search_result['search_url'].unique())
    except:
        search_urls = "_"

    try:
        new_records = gsheets.add_new_values_to_sheet(search_result)

        if new_records is None:
            no_new_records_msg = str(datetime.now()) + "\t" + 'No new records:\t' + search_urls
            log_to_file(no_new_records_msg)

        else:
            filtered_records = upwork.filter_results(new_records)
            filtered_records_msg = 'Number of filtered records:\t' + str(filtered_records.shape[0])
            print(filtered_records_msg)
            log_to_file(filtered_records_msg)
            if filtered_records.shape[0] > 0:
                post_jobs_to_slack(filtered_records)
                slack_msg = str(datetime.now()) + "\t" + '# of new jobs posted to slack:\t' + str(filtered_records.shape[0])
                print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                print(slack_msg)
                log_to_file(slack_msg)
    
    except:
        error_msg = str(datetime.now()) + "\t" + 'error with:\t' + search_urls
        print(error_msg)
        log_to_file(error_msg)
        log_to_file(traceback.format_exc())


if __name__ == "__main__":
    if sys.argv[1] == 'upwork_lead_automation':
        params_list = upwork.build_params_list()#[:40]

        # params_list = upwork.build_params_from_kws(["We are looking for a fulfillment partner to build custom marketing"])
        
        num_parallel_uploads = 30#1 #30
        print("# of parallel procs:\t", num_parallel_uploads)

        ignore_urls = []
        for i in range(0, 1000):
            iteration_start_time = datetime.now()
            start_iteration_msg = f'{str(iteration_start_time)} Iteration {str(i)} - # of urls to search:\t' + str(len(params_list) - len(ignore_urls))
            print('####################################################################')
            print(start_iteration_msg)
            print('####################################################################')
            log_to_file(start_iteration_msg)
            tasks = []
            search_result_list = []
            search_url_list = []
            tasks_loaded = 0
            search_count = 0
            for params in params_list:
                start_search_time = datetime.now()
                search_url = upwork.add_params_to_url(upwork.base_url, params)
                msg = str(datetime.now()) + "\t" + search_url
                log_to_file(msg)

                search_count += 1


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

                    tasks_loaded += 1

                    tasks.append((search_result, search_url))

                    search_result_list.append(search_result)
                    search_url_list.append(search_url)

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
                seconds_left = time_delta.microseconds / 1000000
                #.5 & .75 & .85 are too fast
                wait_time_per_rss_request = 1.5 #1.15 #1#.25
                if seconds_left < wait_time_per_rss_request:
                    time_left = wait_time_per_rss_request - seconds_left
                    time.sleep(time_left)
                else:
                    time.sleep(0.001)

                load_time_msg = f'Total load time:\t: {str(datetime.now() - start_search_time)}'
                log_to_file(load_time_msg)

                if tasks_loaded > (num_parallel_uploads - 1):
                    parallel_load_start_time = datetime.now()
                    parallel_load_msg = str(parallel_load_start_time) + "\t" + 'Number of parallel uploads:\t' + str(tasks_loaded)
                    log_to_file(parallel_load_msg)
                    # upload_inputs = zip(search_result_list, search_url_list)
                    # pool = multiprocessing.Pool(processes=len(search_result_list))
                    # pool.starmap(upload_new_records, upload_inputs)

                    concat_search_result = pd.concat(search_result_list)

                    upload_new_records(concat_search_result)

                    tasks_loaded = 0
                    search_result_list = []
                    search_url_list = []
                    
                    parallel_load_end_time = datetime.now()
                    parallel_load_time_msg = str(parallel_load_end_time) + "\t" + 'Parallel load time:\t' + str(parallel_load_end_time - parallel_load_start_time)
                    log_to_file(parallel_load_time_msg)
                    print('Searches complete on this iteration:\t', search_count)
                    # time.sleep(120)

            iteration_time_msg = str(datetime.now()) + "\t" + 'Iteration time:\t' + str(datetime.now() - iteration_start_time)
            print(iteration_time_msg)
            log_to_file(iteration_time_msg)

    elif sys.argv[1] == "skills_research":
        sheet_data = gsheets.get_sheet_values(gsheets.UPWORK_LEADS_GOOGLE_SHEET_ID, gsheets.UPWORK_LEADS_GOOGLE_SHEET_TAB)
        skills = sheet_data['skills'].values.tolist()
        skills = [x.split(',') for x in skills]
        #convert list of lists into flat list
        skills = [item for sublist in skills for item in sublist]
        skills = [x.strip() for x in skills]
        skills = list(set(skills))
        skills.sort()

        # for x in skills:
        #     print(x)

        #write skills to txt file
        with open('upwork_skills.txt', 'w') as f:
            for item in skills:
                f.write("%s\n" % item)

    elif sys.argv[1] == "filter_testing":
        sheet_data = gsheets.get_sheet_values(gsheets.UPWORK_LEADS_GOOGLE_SHEET_ID, gsheets.UPWORK_LEADS_GOOGLE_SHEET_TAB)
        sheet_data['pub_date'] = pd.to_datetime(sheet_data['pub_date'])
        print(sheet_data.head())
        print(sheet_data.shape)
        #filter to today only
        today = datetime.now()
        today = today.strftime("%Y-%m-%d")
        sheet_data = sheet_data[sheet_data['pub_date'] > datetime.now() - pd.Timedelta(hours=2)]

        filtered_records = upwork.filter_results(sheet_data)

        filtered_records.drop_duplicates(subset=['job_url'], inplace=True)

        print(filtered_records.shape)
        post_jobs_to_slack(filtered_records)

    elif sys.argv[1] == "analyze_trends":
        sheet_data = gsheets.get_sheet_values(gsheets.UPWORK_LEADS_GOOGLE_SHEET_ID, gsheets.UPWORK_LEADS_GOOGLE_SHEET_TAB)
        sheet_data['pub_date'] = pd.to_datetime(sheet_data['pub_date'])


        # filter_hours = 0
        filter_hours = 24
        filtered_records = upwork.filter_results(sheet_data, filter_hours)

        filtered_records.drop_duplicates(subset=['job_url'], inplace=True)

        #group number of jobs by day
        filtered_records['pub_day'] = filtered_records['pub_date'].dt.date
        filtered_records_daily = filtered_records[['pub_day', 'title']].groupby('pub_day').count().reset_index()

        skill_analysis = filtered_records#.head(5)


        #if zoho crm is in the description, add zoho crm to the skills
        skill_analysis['skills'] = skill_analysis.apply(lambda x: x['skills'] + 'zoho crm' if 'zoho crm' in x['description'].lower() else x['skills'], axis=1)

        skill_analysis['skills'] = skill_analysis['skills'].str.split(', ')
        skill_analysis = skill_analysis.explode('skills')

        

        skill_analysis = skill_analysis[['title', 'lower_dollar_amount', 'upper_dollar_amount', 'skills']]

        skill_analysis['lower_dollar_amount'] = skill_analysis['lower_dollar_amount'].astype(float)
        skill_analysis['upper_dollar_amount'] = skill_analysis['upper_dollar_amount'].astype(float)
        skill_analysis.drop_duplicates(subset=['title'], inplace=True)

        skill_analysis_stats = skill_analysis.groupby('skills').agg({'title': 'count', 'lower_dollar_amount': 'mean', 'upper_dollar_amount': 'mean'}).reset_index()

        #sort skill_analysis by upper_dollar_amount
        skill_analysis_stats.sort_values(by=['upper_dollar_amount'], ascending=False, inplace=True)
        skill_analysis_stats = skill_analysis_stats[skill_analysis_stats['title'] > 1]

        print(skill_analysis_stats.head(25))

        skill_df = filtered_records[(filtered_records['description'].str.contains("zoho crm", case=False)) | (filtered_records['description'].str.contains("zoho crm", case=False))]

        skill_df = skill_df[['title', 'lower_dollar_amount', 'upper_dollar_amount']]

        print(skill_df)
        # make_df['skills'] = 'make.com'


        # # make_df = skill_analysis[~skill_analysis['skills'].str.contains("make", case=False)]
        # print(make_df)
        # print(make_df.shape)

        # skill_analysis_stats_two = make_df.groupby('skills').agg({'title': 'count', 'lower_dollar_amount': 'mean', 'upper_dollar_amount': 'mean'}).reset_index()
        # print(skill_analysis_stats_two)