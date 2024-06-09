from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google.oauth2 import service_account
import os
import sys

import pandas as pd

# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


from dotenv import dotenv_values
GOOGLE_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
env_path = GOOGLE_DIR + ".env"
ENV_VARS = dotenv_values(env_path)

GOOGLE_SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

GOOGLE_SERVICE_ACCOUNT_FILE = GOOGLE_DIR + ENV_VARS["GOOGLE_SERVICE_ACCOUNT_FILE"]
GOOGLE_SHEET_MAX_RANGE = str(ENV_VARS["GOOGLE_SHEET_MAX_RANGE"])
UPWORK_LEADS_GOOGLE_SHEET_ID = ENV_VARS["UPWORK_LEADS_GOOGLE_SHEET_ID"]
UPWORK_LEADS_GOOGLE_SHEET_ID_V10 = ENV_VARS["UPWORK_LEADS_GOOGLE_SHEET_ID_V10"]
UPWORK_LEADS_GOOGLE_SHEET_TAB = ENV_VARS["UPWORK_LEADS_GOOGLE_SHEET_TAB"]
UPWORK_DATA_CSV = ENV_VARS['UPWORK_DATA_CSV']

UPWORK_AI_RESEARCH_GOOGLE_SHEET_ID = ENV_VARS["UPWORK_AI_RESEARCH_GOOGLE_SHEET_ID"]
UPWORK_AI_RESEARCH_GOOGLE_SHEET_TAB = ENV_VARS["UPWORK_AI_RESEARCH_GOOGLE_SHEET_TAB"]
EMAIL_ADDRESS = ENV_VARS["EMAIL_ADDRESS"]

creds = None
creds = service_account.Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE, scopes=GOOGLE_SCOPES)

SPREAD_COLS_BASE = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 
                    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

SPREAD_COLS = SPREAD_COLS_BASE
SPREAD_COLS.extend(['a' + x for x in SPREAD_COLS_BASE])

GOOGLE_SHEETS_SERVICE = build('sheets', 'v4', credentials=creds)

def google_append_sheet(values, spreadsheet_id, tab_name=''):
    try:
        end_col = SPREAD_COLS[len(values[0])]

        if tab_name!='':
            tab_name = "'" + tab_name + "'!"
            
        SHEET_RANGE = tab_name + 'A1:'+ end_col + GOOGLE_SHEET_MAX_RANGE

        # Call the Sheets API
        GOOGLE_SHEETS_SERVICE.spreadsheets().values().append(spreadsheetId=spreadsheet_id, 
                                                range=SHEET_RANGE, 
                                                valueInputOption="USER_ENTERED", 
                                                body={"values": values}).execute()

        if not values:
            print('No data found.')

    except Exception as e:
        print(e)

def google_update_sheet(values, spreadsheet_id, tab_name=''):
    try:
        end_col = SPREAD_COLS[len(values[0])]

        if tab_name!='':
            tab_name = "'" + tab_name + "'!"
            
        SHEET_RANGE = tab_name + 'A1:'+ end_col + GOOGLE_SHEET_MAX_RANGE

        # Call the Sheets API
        GOOGLE_SHEETS_SERVICE.spreadsheets().values().update(spreadsheetId=spreadsheet_id, 
                                                range=SHEET_RANGE, 
                                                valueInputOption="USER_ENTERED", 
                                                body={"values": values}).execute()

        if not values:
            print('No data found.')

    except Exception as e:
        print(e)

def google_create_sheet(values, file_name):
    try:
        spreadsheet = {
            'properties': {
                'title': file_name
            }
        }
        request = GOOGLE_SHEETS_SERVICE.spreadsheets().create(body=spreadsheet)
        response = request.execute()
        spreadsheet_id = response['spreadsheetId']

        end_col = SPREAD_COLS[len(values[0])]

        SHEET_RANGE = 'A1:'+ end_col + '1'

        # Call the Sheets API
        GOOGLE_SHEETS_SERVICE.spreadsheets().values().update(spreadsheetId=spreadsheet_id, 
                                                range=SHEET_RANGE, 
                                                valueInputOption="USER_ENTERED", 
                                                body={"values": values}).execute()

        return spreadsheet_id

    except HttpError as err:
        print(err)

def google_share_file(real_file_id, email):
    """Batch permission modification.
    Args:
        real_file_id: file Id
        real_user: User ID
        real_domain: Domain of the user ID
    Prints modified permissions

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """

    try:
        # create gmail api client
        service = build('drive', 'v3', credentials=creds)
        ids = []
        file_id = real_file_id

        def callback(request_id, response, exception):
            if exception:
                # Handle error
                print(exception)
            else:
                print(f'Request_Id: {request_id}')
                print(F'Permission Id: {response.get("id")}')
                ids.append(response.get('id'))

        # pylint: disable=maybe-no-member
        batch = service.new_batch_http_request(callback=callback)
        user_permission = {
            "type": "user",
            "role": "writer",
            "emailAddress": email
        }
        batch.add(service.permissions().create(fileId=file_id,
                                               body=user_permission,
                                               fields='id',))
        batch.execute()

    except HttpError as error:
        print(F'An error occurred: {error}')
        ids = None

    return ids

def get_sheet_values(sheet_id, tab_name):
    google_values = GOOGLE_SHEETS_SERVICE.spreadsheets().values().get(spreadsheetId=sheet_id, range=tab_name).execute()
    records = google_values['values'][1:]
    col_names = google_values['values'][0]
    df = pd.DataFrame(records, columns=col_names)
    return df

def add_new_values_to_sheet(upwork_data):
    existing_df = get_sheet_values(UPWORK_LEADS_GOOGLE_SHEET_ID_V10, UPWORK_LEADS_GOOGLE_SHEET_TAB)
    existing_df['duplicate_key'] = existing_df['job_url'] + existing_df['search_query']
    already_loaded_jobs = list(set(existing_df['duplicate_key'].tolist()))

    already_loaded_urls = list(set(existing_df['job_url'].tolist()))

    upwork_data['duplicate_key'] = upwork_data['job_url'] + upwork_data['search_query']

    new_records = upwork_data[~upwork_data['duplicate_key'].isin(already_loaded_jobs)]
    new_records.drop_duplicates(inplace=True)

    new_records = new_records[existing_df.columns.tolist()]

    del new_records['duplicate_key']

    if new_records.shape[0] > 0:
        google_append_sheet(new_records.values.tolist(), UPWORK_LEADS_GOOGLE_SHEET_ID_V10, UPWORK_LEADS_GOOGLE_SHEET_TAB)
        print('number of records added to sheet:\t', new_records.shape[0])

    deduplicated_jobs = upwork_data[~upwork_data['job_url'].isin(already_loaded_urls)]

    if deduplicated_jobs.shape[0] > 0:
        return deduplicated_jobs

def clear_all_sheet_values(sheet_id, tab_name):
    request = GOOGLE_SHEETS_SERVICE.spreadsheets().values().clear(spreadsheetId=sheet_id, range=tab_name, body={})
    response = request.execute()

    print(response)

if __name__ == "__main__":
    upwork_data = pd.read_csv(UPWORK_DATA_CSV)
    
    if sys.argv[1] == "create_sheet":
        col_names = [upwork_data.columns.tolist()]
        print(col_names)
        id = google_create_sheet(col_names, "upwork_ai_research")
        google_share_file(id, EMAIL_ADDRESS)

    elif sys.argv[1] == "update_sheet":
        add_new_values_to_sheet()


    elif sys.argv[1] == "get_sheet_values":
        df = get_sheet_values(UPWORK_LEADS_GOOGLE_SHEET_ID, UPWORK_LEADS_GOOGLE_SHEET_TAB)

        print(df)

    elif sys.argv[1] == "create_custom_sheet":
        col_names = ["dummy"]
        sheet_name = sys.argv[2]
        id = google_create_sheet(col_names, sheet_name)
        google_share_file(id, EMAIL_ADDRESS)
        
        

    