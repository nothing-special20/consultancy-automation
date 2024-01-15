import requests

from dotenv import dotenv_values

config = dotenv_values(".env")
TLDV_API_KEY = config['TLDV_API_KEY']
MEETING_ID = config['MEETING_ID']

call_highlights_url = f"https://pasta.tldv.io/v1alpha1/meetings/{MEETING_ID}/highlights"
call_transcripts_url = f"https://pasta.tldv.io/v1alpha1/meetings/{MEETING_ID}/transcript"

headers = {
    "x-api-key": TLDV_API_KEY,
    'Content-Type': 'application/json'
}

response = requests.get(call_transcripts_url, headers=headers)

print(response.text)
print(response.status_code)