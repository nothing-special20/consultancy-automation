import os, sys
from datetime import datetime, timedelta
import requests


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

    return [item["INTERNAL VALUE"] for item in mapping if item["LABEL"] == stage_name][
        0
    ]


def create_crm_record(schema, properties):
    try:
        simple_public_object_input_for_create = SimplePublicObjectInputForCreate(
            properties=properties
        )

        entity_api = getattr(api_client.crm, schema).basic_api.create

        api_response = entity_api(
            simple_public_object_input_for_create=simple_public_object_input_for_create,
        )

        return api_response  # .json()

    except ApiException as e:
        print("Exception when creating contact: %s\n" % e)
        return {"error": e}


def create_object_record(schema, properties, associations=[]):
    try:
        simple_public_object_input_for_create = SimplePublicObjectInputForCreate(
            properties=properties,
            associations=associations,
        )
        entity_api = getattr(api_client.crm.objects, schema).basic_api.create

        api_response = entity_api(
            simple_public_object_input_for_create=simple_public_object_input_for_create
        )

        return api_response  # .json()

    except ApiException as e:
        print("Exception when creating contact: %s\n" % e)
        return {"error": e}


def create_deal(deal_properties):
    try:
        notes = deal_properties["notes"]
        del deal_properties["notes"]
    except:
        notes = []

    deal_record = create_crm_record("deals", deal_properties)

    deal_id = deal_record.id

    current_time = (datetime.now() + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

    for note in notes:
        note_properties = {
            "hubspot_owner_id": HUBSPOT_OWNER_ID,
            "hs_timestamp": current_time,
            "hs_note_body": note,
        }

        # https://developers.hubspot.com/docs/api/crm/associations
        associations = [
            {
                "to": {"id": deal_id},
                "types": [
                    {"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 214}
                ],
            },
        ]

        create_object_record("notes", note_properties, associations)

    return deal_record


def get_analytics_reports():
    url = "https://api.hubapi.com/analytics/v2/reports/totals/total?start=20180101&end=20180301"

    response = requests.get(url, headers={"Authorization": f"Bearer {ACCESS_TOKEN}"})

    return response.text


def create_contact(_contact_properties):
    contact_properties = [
        {"name": "lifecyclestage", "value": "lead"},
        {"name": "hubspot_owner_id", "value": HUBSPOT_OWNER_ID},
        {"name": "email", "value": _contact_properties['email']},
        {"name": "firstname", "value": _contact_properties['first_name']},
        {"name": "lastname", "value": _contact_properties['last_name']},
        {"name": "hs_lead_status", "value": "IN_PROGRESS"},
    ]
    contact_properties = {
        "lifecyclestage": "lead",
        "hubspot_owner_id": HUBSPOT_OWNER_ID,
        "email": _contact_properties['email'],
        "firstname": _contact_properties['first_name'],
        "lastname": _contact_properties['last_name'],
        "hs_lead_status": "IN_PROGRESS",
        "contact_source": _contact_properties['contact_source'],
    }

    create_crm_record("contacts", contact_properties)

def get_all_contacts():
    return api_client.crm.contacts.get_all()

def get_all_contact_emails():
    contacts = get_all_contacts()
    return [x.properties["email"] for x in contacts]

if __name__ == "__main__":
    if sys.argv[1] == "create_contact":
        contact_properties = {"email": "email3@example.com"}
        create_crm_record("crm.contacts", contact_properties)

    elif sys.argv[1] == "create_deal":
        deal_stage_id = deal_stage_mapping("Proposal Requested")

        deal_properties = {
            "dealname": "deletemefourty",
            "hubspot_owner_id": HUBSPOT_OWNER_ID,
            "lead_source": "UPWORK",
            "dealstage": deal_stage_id,
            "amount": 1000,
        }
        deal_record = create_crm_record("deals", deal_properties)
        print(deal_record)

    elif sys.argv[1] == "create_company":
        company_properties = {
            "hubspot_owner_id": HUBSPOT_OWNER_ID,
            "lifecyclestage": "lead",
            "domain": "fakefakefake.fakexy",
            "name": "Fake Company",
        }
        create_crm_record("companies", company_properties)

    # elif sys.argv[1] == "create_note":
    #     ticket_properties = {
    #         "hs_pipeline": "0",
    #         "hs_pipeline_stage": "1",
    #         "subject": "Fake Ticket",
    #         "content": "Fake Ticket Content",
    #         "hubspot_owner_id": HUBSPOT_OWNER_ID,
    #     }
    #     create_crm_record("", ticket_properties)

    elif sys.argv[1] == "create_note":
        create_deal()

    elif sys.argv[1] == "get_analytics_reports":
        print(get_analytics_reports())


    elif sys.argv[1] == "get_all_contacts":
        contacts = get_all_contact_emails()
        for x in contacts:
            print(x)