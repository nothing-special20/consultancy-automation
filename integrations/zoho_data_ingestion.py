import os, sys
import json

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MARKETING_DIR = parent_dir + "/marketing"
EMAIL_DIR = parent_dir + "/marketing/email"
APOLLO_DIR = parent_dir + "/marketing/email/apollo.io"
ZOHO_DIR = parent_dir + "/zoho"

for x in [parent_dir, MARKETING_DIR, EMAIL_DIR, APOLLO_DIR, ZOHO_DIR]:
    sys.path.append(parent_dir)

from marketing.email.apollo_io import main as apollo

from zoho.crm import main as zoho_crm


if __name__ == "__main__":
    data_list = []
    for i in range(1,2):
        params = {
            "q_keywords": "sales manager",
            "sort_by_field": "contact_last_activity_date",
            "sort_ascending": False,
            "page": str(i),
        }
        search_list = apollo.search_contacts(params)["contacts"]

        for x in search_list[:1]:
            try:
                print(json.dumps(x, indent=4))
                cleaned_data = apollo.apollo_contact_etl_zoho_lead(x, [{"name": "sales managers"}])
                data_list.append(cleaned_data)
            except:
                print(x)


    print(data_list[:1])
    print(len(data_list))

    print(json.dumps(data_list[:1], indent=4))

    x = zoho_crm.create_lead(zoho_crm.ZOHO_AUTH_TOKEN, data_list[:1])

    print(x)
    
