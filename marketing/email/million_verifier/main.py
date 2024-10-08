import requests
import os, sys
import io

import pandas as pd

from dotenv import dotenv_values

env_path = os.path.dirname(os.path.realpath(__file__)) + "/.env"
config = dotenv_values(env_path)

MILLION_VERIFIER_API_KEY = config["MILLION_VERIFIER_API_KEY"]
MILLION_VERIFIER_DATA_FOLDER = config["MILLION_VERIFIER_DATA_FOLDER"]

def verify_email(email):
    url = f"https://api.millionverifier.com/api/v3/?api={MILLION_VERIFIER_API_KEY}&email={email}&timeout=timer3"

    response = requests.get(url).json()

    code = response["resultcode"]

    if code == 1:
        email_status = "Ok"
    elif code == 2:
        email_status = "Catch All"
    elif code == 3:
        email_status = "Unknown"
    elif code == 4:
        print("Error: " + response["error"])
        email_status = "Error"
    elif code == 5:
        email_status = "Disposable"
    elif code == 6:
        email_status = "Invalid"

    return {"email": email, "status": email_status}


def verify_emails_bulk(csv_file):
    url = f"https://bulkapi.millionverifier.com/bulkapi/v2/upload?key={MILLION_VERIFIER_API_KEY}"

    files = [("file_contents", ("filename", open(csv_file, "rb"), "text/plain"))]

    response = requests.request("POST", url, files=files)

    return response.json()

def download_report(id):
    url = "https://bulkapi.millionverifier.com/bulkapi/v2/download"

    params = {
        "key": MILLION_VERIFIER_API_KEY,
        "file_id": int(id),
        "filter": "all"
    }

    response = requests.get(url, params=params)

    csv_data = io.StringIO(response.text)

    df = pd.read_csv(csv_data)

    return df

if sys.argv[1] == "verify":
    email = sys.argv[2]
    print(verify_email(email))