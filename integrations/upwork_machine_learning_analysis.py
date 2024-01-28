import os, sys
import requests
from datetime import datetime

from dotenv import dotenv_values
GOOGLE_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
env_path = GOOGLE_DIR + ".env"
ENV_VARS = dotenv_values(env_path)
HUGGINGFACEHUB_API_TOKEN = ENV_VARS["HUGGINGFACEHUB_API_TOKEN"]


def hf_inference_query(payload, model_id):
    start_time = datetime.now()
    headers = {"Authorization": f"Bearer {HUGGINGFACEHUB_API_TOKEN}"}
    API_URL = f"https://api-inference.huggingface.co/models/{model_id}"
    response = requests.post(API_URL, headers=headers, json=payload)
    print(str(datetime.now() - start_time))
    return response.json()


def hf_topic_classification(text, topics):
    payload = {
        "inputs": text,
        "parameters": {"candidate_labels": topics, "task": "zero-shot-classification"},
    }

    model_id = "valhalla/distilbart-mnli-12-1"

    result = hf_inference_query(payload, model_id)

    try:
        labels = result['labels']
        scores = result['scores']
        max_index = scores.index(max(scores))
        return labels[max_index]
    
    except:
        print(result)
        return "Error!"



if __name__ == "__main__":
    job_title = "Funnel Strategy for High Ticketing Course"
    topics = ["zoho crm", "automation", "analytics", "artificial intelligence", "other"]
    result = hf_topic_classification(job_title, topics)
    
    