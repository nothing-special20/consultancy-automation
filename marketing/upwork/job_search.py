import requests
from urllib.parse import urlencode
import pandas as pd
import sys, os

import time
import html

from datetime import datetime

#pandas show all columns
pd.set_option('display.max_columns', None)

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

from main import rss_to_df

base_url = "https://www.upwork.com/ab/feed/jobs/rss"

def deduplicate_list(x):
    already_seen = set()
    result = []
    for item in x:
        if item not in already_seen:
            already_seen.add(item)
            result.append(item)
    
    return result

def add_params_to_url(base_url, params):
    return base_url + '?' + urlencode(params)

def concatenate_search_from_queries(params_list):
    data_list = []
    for params in params_list:
        search_url = add_params_to_url(base_url, params)
        print(search_url)
        try:
            search_result = rss_to_df(search_url)
            search_result['search_url'] = search_url
            search_result['search_query'] = params['q']
            print(search_result.shape)
            data_list.append(search_result)
        except:
            print('error with:\t', search_url)

        time.sleep(1.5)

    df = pd.concat(data_list)
    df.drop_duplicates(inplace=True)
    return df

def build_params_from_kws(kw_list):
    params_list = []
    for kw in kw_list:
        params_list.append({"q": kw, "sort": "recency", "paging": "0"})
    return params_list


def remove_titles(df):
    rm_titles = ["appointment setter", "sales development rep", "wordpress", "android", "ios", "sales manager", "image editing", "sharepoint", "paid media", "ui/ux", "ux/ui", "data scientist", "spanish", "bookkeeper", "3d print", "qa test", "accountant", "Solidworks", "lavarvel", "vue js", "data analyst", "mern stack", " sap ", "blog post", "advertising specialist", "media buyer", "co founder", "co-founder", "firmware", "salesforce", "accountant", "betting", "3d", "virtual assistant", "powerpoint", "firebase", "ebay", "plugin", "writer", "tax", "crypto", "windows", "vba", "cisco", "theme", "director", "part time", "part-time", "coordinator", "senior", "react", "project manager", "P.h.D", "professor", "trader", "Kubernete", "devops", "Google Tag Manager", "Marketing Strategist", "Social Media Marketing expert", "network admin", "web design", "Customer Service Rep", "cloud security", "it analyst", "system[s]{0,1} analyst", "Ads Manager", "Blog .{1,10} Writing", "Google ads management", "google ads expert", "Google Adword", "ppc manage", "drug", "brand consult", "c\\+\\+", "Strategic Marketing Partner", "Social Media Strategist", "Ads Specialist", "Lead Generation Expert", "operations manager", "mobile app", "dating", "assistant", "Product Manager", "trading", "Media Buying Expert", "for everyone", "girlfriend", "PPC Specialist", "Affiliate Marketing", "affiliate manager", "funnel strategy", "seo specialist", "Extension Developer", "blockchain", "mern", "facebook ads expert", "UX Design", "Content Creator", "Roulette", "E-commerce Specialist", "publishing consultant", "Social Media Manage", "Figma", "penetration test", "Cart Optimization", "Microsoft Power", "Create email campaigns", "Squarespace", "teacher", "Management Consultant", "Online course", "Medical", "healthcare", "influencer", "social media marketing", "game server", "Marketo", "lawyer", "attorney", "odoo", "sdr remote", "Videographer", "video editor", "cold caller", "german", "soap api", "video game", "sales rep", "dropshipping", "kindle", "tiktok", "mlm", "java ", "Cannabis", "marijuana", "recruiter", "IICS", "macro", "Public Relations", "informatica", "Administrative Assist", "Lead Generation Specialist", "OKTA", "blogger", "Angular", "Conversion Rate Optimization", "Cloud Admin", "market research", "rebrand", "browser extension", "Marketing manager", "Social Media Expert", "Sage 300", "Kajabi", "manager", "personal brand", "bookkeep", "kajabi", "salary", "Cyber {0,1}security", "SMMA", "Q/A", "Banking", "Expert PPC", "Website Dev", "Supervisor", "Pardot", "video", "Commission", "Head of", "trainer", "insurance sales", "paypal", "join the team", "sex doll"]

    crypto = ["nft", "crypto", "blockchain", "opensea", "web3", "web 3", "trading", "trader", "coingecko", "bitcoin", "solana", "ethereum"]

    microsoft = ["vba", "\\.net", "Microsoft 365", "vba", "visual basic", "dynamics", "microsoft access", "excel", "powerbi", "power bi", "Intune", "PowerShell"]

    ignore = ["Casino", "gamble", "marijuana", "dating", "girlfriend", "Cannabis", "THC", "gummies", "dispensary", "LGBT", "gay", "model agency", "Green Energy", "Social Justice", "onlyfans", "only fans", "political", "smoke", "culture", "change your life", "Remote Work", "harder you work", "for beginner", "of chatter", "female", "gambling", "gamble", "dreams"]

    code_tools = ["Playwright", "SAP BW", "SQL dev", "NET dev", "AWS EKS", "architect", "fin tech", "C#", "MSSQL"]

    ecom = ["klaviyo", "shopify"]

    social_media = ["instagram"]

    sales_titles = ["Business Development Executive"]

    misc_technical = ["website build", "website for", "website design", "framer", "full.stack dev", "Office 365", "sap hana", "oracle", "cisco", "citrix", "docker", "kubernetes", "cloud base", "wweb design", "End Engineer", "webflow", "GraphQL", "Database Builder", "Chrome Extension", "scrape", "scraping", "azure", "Google Apps", "aws ", "hana", "osint", "Microsoft Dynamics 365", "axiom", "rust", "sap int", "Browser Automation", "Grafana", "snowflake", "tableau", "scripts", "cobol", "Siemens", "TypeScript", "sap security", "Adobe", "Website update", "vue.{0,1}js", "cockroach", "virtual machine", "virtual desktop", "Svelte", "PowerAutomate", "Connectwise", "quickbook", "unreal engine", "Apache", "CI/CD", "Microsoft SQL", "flutter", "zoho dev", "zoho flow", "kafka"]

    misc_biz_software = ["asana", "jira", "ZenDesk"]

    no_code_low_code = ["bubble", "n8n"]

    digital_marketing = ["amazon ppc", "digital marketing for", "conversion track", "seo for", "seo audit", "Digital Marketing Consultant", "Landing Page", "advertising expert", "convertkit", "ui design", "Click {0,1}Funnels", "Keyword Specialist", "SEO Campaign", "wordpress", "Google merchant", "wix", "NetSuite", "LinkedIn Campaign Setup", "Facebook Campaign Setup", "Build a website", "Generate Leads", "MS SQL", "Marketing Advice", "ad optimiz", "marketing plan", "Digital Marketer Needed", "youtube", "LinkedIn Ads", "website redesign", "Digital marketer", "seo ", "Elementor", "Woocommerce", "Media buying", "PPC Campaign", "Amazon Marketing", "reddit", " bing", "link build", "ad words", "PPC Ads expert", "PPC - Google Ads", "Google Local Ads", "PPC", "copy edit", "twitter", "dropship", "Google Shopping", "Google Ad", "macOS", "advertising for", "Google MyBusiness", "Marketing Consultant", "Digital Marketing Specialist", "PPC Expert", "Business Plan", "ads expert", "Social Media Strategy"]

    misc_professional = ["payroll", "bookkeep", "mechanical engineer", "talent scout", "regulat", "legal", "data engineer", "civil engineer", "Business Development Executive", "AutoCAD", "Business Analyst", "quality assu", "Google Expert", "sales exec", "High Ticket Closer", "sales closer", " pr ", "non.{0,1}profit", "Sales Expert", "Fractional", "Team Leader", "event marketing", "financial statement", "cpa ", "patent", "work is simple", "Pipedrive", "Publicist"]

    random = ["newsletter", "discord", "guru", "google maps", "brand", "Students", "stripe", "research", "translat", "presentation", "bonus", "amazon", "game", "stories", "Exchange", "stock", "novices", "pitch deck", "magento"]

    rm_titles_all = rm_titles + crypto + microsoft + ignore + code_tools + sales_titles + ecom + social_media + misc_technical + digital_marketing + no_code_low_code + misc_professional + misc_biz_software + random

    rm_titles_all = list(set(rm_titles_all))

    # rm_titles = [x.replace('+', '\\+') for x in rm_titles]
    rm_titles_all = '|'.join(rm_titles_all)
    rm_titles_all = rm_titles_all.lower()

    df = df[~df['title'].str.contains(rm_titles_all, case=False)]

    return df

def remove_descriptions(df):
    rm_descriptions = ["join our [team|agency]", "cover letter", "c\\+\\+", "IoT", " va ", "mobile app dev", "php", "admin assistant", "resume", "guru", "Responsibilities", "Solidworks", "NO PREVIOUS EXPERIENCE NECESSARY", "cross-{0,1}functional teams", "stock opportunities", "Digital Marketing specialist", "youtube", "virtual machine", "crypto", "security", "full.{0,1}time"] 
    # rm_descriptions = [x.replace('+', '\\+') for x in rm_descriptions]
    rm_descriptions = '|'.join(rm_descriptions)

    df = df[~df['description'].str.contains(rm_descriptions, case=False)]

    return df


def filter_results(df, filter_hours=8):
    #convert upper_dollar_amount to int
    df['upper_dollar_amount'] = df['upper_dollar_amount'].astype(float)
    df['budget'] = df['budget'].astype(float)
    df = df[((df['upper_dollar_amount'] > 50)) | (df['budget'] > 1000)] # & (df['lower_dollar_amount'] > 15)

    df = remove_titles(df)
    df = remove_descriptions(df)

    #convert pub date to datetime
    df['pub_date'] = pd.to_datetime(df['pub_date'])

    if filter_hours > 0:
        df = df[df['pub_date'] > datetime.now() - pd.Timedelta(hours=filter_hours)]

    df.drop_duplicates(subset=['job_url'], inplace=True)

    return df

def build_params_list():
    crm_automation_kw_list = ["hubspot", "hubspot crm", "hubspot marketing", "hubspot integration", "make.com", "make.com", "airtable", "langchain", "zapier", "gpt", "gpt4", "gpt-4", "smartlead.ai", "apollo.io", "automation", "marketing automation", "automated workflow", "integrate", "integration", "crm integration", "crm", "crm automation", "email automation", "Customer Relationship Management", "project workflows", "automate workflow", "api integration", "business process automation", "streamline", "streamlining", "data enrichment", "data cleaning", "data cleansing", "crm consultant", "AI Systems", "automation systems", "ai automation", "ai business", "business automation", "sales process", "sales automation", "digital marketing automation", "automation expert", "digital transformation", "llm", "large language model", "generative ai", "spacy", "hugging face", "air table", "calendly", "integrations", "omnichannel automation", "airtable system", "data mining", "data scrubbing", "data extraction", "data pipeline", "chatgpt", "chat gpt", "crm optimization", "google analytics", "ga4", "slack","process optimization", "system integration", "task automation", "automated reporting", "operation automation tools", "integration specialist", "integration expert", "workflow integration", "automation specialist", "google sheet", "no-code", "no code", "calendly integration", "sales system", "sales onboarding", "lead tracking", "streamlining", "audience segmentation", "WhatsApp", "customer lifecycle", "procedure development", "Standard Operating Procedures", "key performance indicator", "integrate systems", "automation flows", "Email Deliverability", "openai", "openai api", "chatgpt api", "OpenAI Embeddings", "Software Integration", "automate", "automated", "sales report", "marketing report"]

    dashboard_kws = ["business analytics", "business analysis", "dashboard", "kpi", "financial reporting", "financial analysis", "looker", "google data", "data analytics", "business intelligence"]

    ai_kws = ["llama", "machine learning", "ocr", "optical character recognition", "document processing"]

    outbound_marketing = ["mass cold email", "cold emailing", "email campaign", "email marketing", "zoominfo", "linkedin sales navigator", "cold email", "email setup"]

    analytics_kws = ["marketing analytics", "media analytics", "sales analytics", "create reports"]

    ai_img_gen_list = ["image generation", "stable diffusion", "midjourney", "ai art", "generate image", "dall-e", "dalle", "ai image", "image creation", "generated image", "ai image editing"]

    sales_and_marketing_kw_list = ["sales", "marketing", "lead generation", "lead gen", "sales call", "mailchimp"]

    project_mgmt_software = ["trello", "notion", "lucidchart", "lucid chart"]

    base_kw_list = deduplicate_list(crm_automation_kw_list + outbound_marketing + sales_and_marketing_kw_list + project_mgmt_software + ai_kws + dashboard_kws + analytics_kws) #+ ai_img_gen_list

    skills_list = ["automation", "make.com", "data integration", "api integration", "gpt", "chatgpt", "langchain", "airtable", "google analytics", "automated workflow", "Marketing Operations & Workflow", "sales operations", "crm automation", "campaign management", "email campaign setup", "Marketing Automation Strategy", "AI Classifier", "AI Implementation", "AI-Generated Text", "Apollo.io", "CRM Development", "CRM Software",  "Lead Generation Analysis", "Google Sheets Automation", "Advertising Automation", "Automation Framework", "Build Automation", "ML Automation", "Robotic Process Automation", "task automation", "Growth Analytics", "Sales Analytics", "Sales Lead Lists", "Business Operations", "Insurance Agency Operations", "Operations Analytics", "Operations Management Software", "Application Integration Software", "Call Center Software", "Call Center Management", "Data Analytics", "Data Analytics &amp; Visualization Software", "Data Analytics Framework", "ERP Software", "Generative AI Software", "Low-Code Development", "Software Integration", "Sales Strategy", "Marketing Strategy", "Business Intelligence", "Customer Service Analytics", "data mining", "procedure development", "Analytics", "zapier", "uipath", "data extraction", "gpt-4",  "ai instruction"]

    ai_tasks = ["research"]

    skills_list = deduplicate_list(skills_list)

    general_searches = [f'("{kw}")' for kw in base_kw_list]
    no_quotes_general_searches = [f'{kw}' for kw in base_kw_list]
    title_searches = [f'title:("{kw}")' for kw in base_kw_list]
    description_searches = [f'description:("{kw}")' for kw in base_kw_list]
    details_searches = [f'details:("{kw}")' for kw in base_kw_list]
    skills_searches = [f'skills:("{kw}")' for kw in skills_list]

    all_searches = skills_searches + general_searches + no_quotes_general_searches  + title_searches + description_searches + details_searches + ai_tasks

    #ignore searches
    ignore_searches = ["PHP", "video editor", "blockchain", "trading automation", ".net", "appointment setter", "sales development rep", "wordpress", "android", "ios", "animator", "full time", "full-time", "join our .{1,15} team", "exciting", "Wix", "Copywriter", "Pay Per Click Expert", "pharma"]
    ignore_searches = [f'"{x}"' for x in ignore_searches]
    ignore_searches = " OR ".join(ignore_searches)
    ignore_searches = f" AND NOT ({ignore_searches})"
    # all_searches = [x + ignore_searches for x in all_searches]

    params_list = build_params_from_kws(all_searches)

    return params_list

if __name__ == "__main__":
    if sys.argv[1] == 'test':
        # Example usage
        # params = {"q": 'title:("zoho crm")', "sort": "recency", "paging": "0"}

        params_list = build_params_list()

        for x in params_list:
            print(x)

        result = concatenate_search_from_queries(params_list)

        result['description'] = result['description'].apply(lambda x: html.unescape(x))

        # create a field called word_count that counts the number of words in the description
        result['word_count'] = result['description'].apply(lambda x: len(x.split()))

        # creat a field called field_count that counts the number of characters in the description
        result['char_count'] = result['description'].apply(lambda x: len(x))

        #convert pub date to datetime
        result['pub_date'] = pd.to_datetime(result['pub_date'])
        result["pub_date"] = [x - pd.Timedelta(hours=5) for x in result["pub_date"]]
        #convert utc time to etc

        print('1', result.shape)

        result = filter_results(result)
        print('2', result.shape)
        result = remove_titles(result)
        print('3', result.shape)

        result.sort_values(by=['pub_date'], inplace=True, ascending=False)

        #sort by pub_date descending

        result.to_csv("upwork_search_results_new.csv", index=False)


        #unique jobs
        unique_jobs = result.drop_duplicates(subset=['job_url'])

        unique_jobs.to_csv("upwork_search_results_new_UNIQUE.csv", index=False)

        # # # # print(result)
        # print(result.shape)
        print(datetime.now())