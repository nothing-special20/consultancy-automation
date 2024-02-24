import os, sys

from dotenv import dotenv_values

HUBSPOT_DIR = os.path.dirname(os.path.realpath(__file__)) + "/"
env_path = HUBSPOT_DIR + ".env"
ENV_VARS = dotenv_values(env_path)
ACCESS_TOKEN = ENV_VARS.get("ACCESS_TOKEN")
HUBSPOT_OWNER_ID = ENV_VARS.get("HUBSPOT_OWNER_ID")

from hubspot import HubSpot

from hubspot.auth.oauth import ApiException

api_client = HubSpot(access_token=ACCESS_TOKEN)

# or set your access token later
api_client = HubSpot()
api_client.access_token = ACCESS_TOKEN

from hubspot.crm.contacts import SimplePublicObjectInputForCreate
from hubspot.crm.contacts.exceptions import ApiException


def deal_stage_mapping(stage_name):
    mapping = [
        {"LABEL": "First Response Received", "INTERNAL VALUE": 158179859},
        {
            "LABEL": "Additional Information Requested",
            "INTERNAL VALUE": 158179860,
        },
        {
            "LABEL": "Appointment Scheduled",
            "INTERNAL VALUE": "appointmentscheduler",
        },
        {
            "LABEL": "Proposal Requested",
            "INTERNAL VALUE": "qualifiedtobuy",
        },
        {"LABEL": "Negotiation", "INTERNAL VALUE": 158179861},
        {"LABEL": "Contract Sent", "INTERNAL VALUE": "contractsent"},
        {"LABEL": "Invoice Sent", "INTERNAL VALUE": 158179862},
        {"LABEL": "Closed Lost", "INTERNAL VALUE": "closedlost"},
        {"LABEL": "Closed Won", "INTERNAL VALUE": "closedwon"},
    ]

    return [item["INTERNAL VALUE"] for item in mapping if item["LABEL"] == stage_name][0]

def create_record(schema, properties):
    try:
        simple_public_object_input_for_create = SimplePublicObjectInputForCreate(
            properties=properties
        )
        # api_response = api_client.crm.contacts.basic_api.create(
        entity_api = getattr(api_client.crm, schema).basic_api.create
        api_response = entity_api(
            simple_public_object_input_for_create=simple_public_object_input_for_create
        )

    except ApiException as e:
        print("Exception when creating contact: %s\n" % e)

if __name__ == "__main__":
    if sys.argv[1] == "create_contact":
        contact_properties = properties = {"email": "email3@example.com"}
        create_record("contacts", contact_properties)

    elif sys.argv[1] == "create_deal":
        deal_stage_id = deal_stage_mapping("Proposal Requested")

        deal_properties = properties = {
            "dealname": "deletemetwo",
            "hubspot_owner_id": HUBSPOT_OWNER_ID,
            "lead_source": "UPWORK",
            "dealstage": deal_stage_id,
            "amount": 1000,
        }
        create_record("deals", deal_properties)

    elif sys.argv[1] == "create_company":
        company_properties = {
            "hubspot_owner_id": HUBSPOT_OWNER_ID,
            "lifecyclestage": "lead",
            "domain": "fakefakefake.fakexy",
            "name": "Fake Company",
            }
        create_record("companies", company_properties)