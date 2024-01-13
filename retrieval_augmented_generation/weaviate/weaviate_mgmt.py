import sys, os
import re

import weaviate
import weaviate.classes as wvc

from docx import Document

import json
import traceback


if __name__ == "__main__":
    # reset vector database
    # python3 weaviate_mgmt.py 'create_schema_my_portfolio'
    # python3 weaviate_mgmt.py 'update_portfolio'
    # python3 weaviate_mgmt.py 'write_job_proposal'
    if sys.argv[1] == "create_schema_my_portfolio":
        client = weaviate.WeaviateClient(
            weaviate.ConnectionParams.from_url("http://localhost:8080", 50051)
        )
        client.collections.delete("my_portfolio")
        if not (client.collections.exists("my_portfolio")):
            upwork = client.collections.create(
                name="my_portfolio",
                # vectorizer_config=wvc.Configure.Vectorizer.text2vec_huggingface(),  # If set to "none" you must always provide vectors yourself. Could be any other "text2vec-*" also.
                vectorizer_config=wvc.Configure.Vectorizer.text2vec_openai(),
                generative_config=wvc.Configure.Generative.openai(),  # Ensure the `generative-openai` module is used for generative queries
                properties=[
                    wvc.Property(
                        name="project_title",
                        description="project_title",
                        data_type=wvc.DataType.TEXT,
                    ),
                    wvc.Property(
                        name="project_description",
                        description="project_description",
                        data_type=wvc.DataType.TEXT,
                    ),
                    wvc.Property(
                        name="skills_used",
                        description="skills_used",
                        data_type=wvc.DataType.TEXT,
                    ),
                ],
            )

    elif sys.argv[1] == "update_portfolio":
        client = weaviate.WeaviateClient(
            weaviate.ConnectionParams.from_url("http://localhost:8080", 50051)
        )
        portfolio_data_file = "data/portfolio.json"
        with open(portfolio_data_file) as f:
            portfolio_data = json.load(f)

        my_portfolio = client.collections.get("my_portfolio")

        client = weaviate.WeaviateClient(
            weaviate.ConnectionParams.from_url("http://localhost:8080", 50051)
        )
        class_name = "My_portfolio"
        class_properties = [
            "*"
        ]  # Replace with the specific properties you want to retrieve

        collection = client.collections.get(class_name)

        for project in portfolio_data:
            my_portfolio.data.insert(project)

    elif sys.argv[1] == "show_records":
        client = weaviate.WeaviateClient(
            weaviate.ConnectionParams.from_url("http://localhost:8080", 50051)
        )
        class_name = "My_portfolio"
        class_properties = [
            "*"
        ]  # Replace with the specific properties you want to retrieve

        collection = client.collections.get(class_name)

        jobs = [item.properties for item in collection.iterator()]

        for item in jobs:
            print(json.dumps(item, indent=4))

        # Print or process the retrieved objects
        # for obj in response['data']['Get']['YourClassName']:
        #     print(obj)

    elif sys.argv[1] == "write_job_proposal":
        from langchain.llms import OpenAI

        from langchain.vectorstores.weaviate import Weaviate
        from langchain.schema.runnable.passthrough import RunnablePassthrough
        import weaviate

        from langchain.prompts import PromptTemplate
        from langchain.chains import LLMChain

        from dotenv import dotenv_values

        config = dotenv_values(".env")

        client = weaviate.Client("http://localhost:8080")

        # vectorstore = Weaviate(client, "Upwork_jobs", "snippet")
        vectorstore = Weaviate(client, "My_portfolio", "project_description")

        retriever = vectorstore.as_retriever()

        os.environ["OPENAI_API_KEY"] = config["OPENAI_APIKEY"]

        openai_llm = OpenAI(
            verbose=True, temperature=0.3, model_name="gpt-4-1106-preview"
        )

        job_desc = """
        Job Description:
We are seeking a Zoho Suite Specialist to assist with our business's CRM and integration needs. This role will involve working with various applications within the Zoho suite, as well as integrating and managing other applications like Notion, make.com, Google Sheets, and QuickBooks.

Responsibilities:
Answer questions and provide insights on capabilities within the Zoho suite of apps, Notion, and make.com.
Help with minor customizations in Zoho applications, including configuring reports and performing mass-record updates.
Occasionally run and provide custom reports.
Manage and maintain integrations with other applications such as Notion, Google Sheets, make.com, and QuickBooks.
Assist with the implementation of a comprehensive work order for Zoho CRM, which includes lead management, reporting in Zoho Analytics, process review, and data migration.
Provide rapid communication and complete tasks efficiently (preferably within 24 hours).
Must be available to communicate during Central Standard Time business hours.

Requirements:
In-depth knowledge of Zoho suite applications.
Experience with managing and integrating applications like Notion, make.com, Google Sheets, and QuickBooks.
Ability to perform minor customizations and configurations within Zoho without coding.
Excellent problem-solving skills and ability to work independently.
Good communication skills and responsiveness for quick task turnaround.
Additional Information:
This position is part-time and on an as-needed basis.
The specialist will be paid on an hourly rate.
Work can be done remotely, with a preference for someone who can quickly respond and complete tasks within 24 hours.  
        """

        # I have almost no experience with airtable. Please do not say that I have much experience with air table - just say that I can learn it quickly based on previous experience.

        #####Split the proposal generation out into multiple parts
        # 1) Write a brief summary of their proposal.
        # 2) Find relevant projects in my portfolio.
        # 3) Write a more detailed summary of their proposal tying in my experience to show how I am a good fit for the job.

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # 1) Write a brief summary of their proposal.
        brief_job_summary_template = """
            Question: Please write a brief, one sentence summary of a job description. Start the proposal as if you are talking to someone, with something like 'so you need someone to do X'. Then summarize it.

            job description: {job_description}

            Answer: Here is the summary of the job description:
        """

        job_desc_summary_prompt = PromptTemplate(
            input_variables=["job_description"], template=brief_job_summary_template
        )

        brief_job_summary_chain = LLMChain(llm=openai_llm, prompt=job_desc_summary_prompt)
        brief_job_summary = brief_job_summary_chain.run(job_desc)

        print(brief_job_summary)

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # 2) Find relevant projects in my portfolio.
        relevant_projects_template = """
            Your task is to analyze a specific job description and then review a list of projects from my professional portfolio. Your objective is to identify and return only those projects that are highly relevant to the job description. Only select 2 or 3 projects at most.

            1) Understand the Job Description: Examine the skills, scope, and specific requirements highlighted in the job description.

            2) Review My Portfolio: Go through my project list, considering relevance based on skills, project scope, and industry.

            3) Assess Relevance: Select projects that closely match the job description, focusing on similar skills, project size, and industry.

            4) Justify Selections: For each chosen project, briefly explain its relevance to the job, linking specific project aspects to the job requirements.

            Aim for accuracy and relevance in your analysis to ensure the selected projects demonstrate my suitability for the job
            
            Job description:
            {job_description}

            portfolio projects:
            {portfolio_jobs}
        """

        prompt = PromptTemplate(
            input_variables=["job_description", "portfolio_jobs"],
            template=relevant_projects_template,
        )

        # Create llm chain
        llm_chain = LLMChain(llm=openai_llm, prompt=prompt)

        rag_chain = {
            "job_description": RunnablePassthrough(),
            "portfolio_jobs": retriever,
        } | llm_chain

        relevant_jobs_response = rag_chain.invoke(job_desc)

        relevant_jobs_text = relevant_jobs_response["text"]

        print(relevant_jobs_text)

        # # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # 3) Write a more detailed summary of their proposal tying in my experience to show how I am a good fit for the job.
        proposal_prompt_template = """
            Instruction: You are to assume the role of an expert business proposal writer. Your objective is to create a tailored proposal for a specific Upwork job. Initiate by dissecting the job description to understand the client's core needs. Then, pinpoint the project in my portfolio that best matches this job.

            Ensure the proposal language is professional, clear, and devoid of any extravagant or overly embellished phrases. Focus on delivering your message in a straightforward manner. Avoid using broad, grandiose statements. Avoid flowery language. The tone should be pragmatic and focused on delivering real-world value. Avoid words like 'quest, vision', 'journey.', or 'exciting' Instead, use words like 'project, goal, or objective.'

            Remember, the tone of the proposal should exude quiet confidence and a professional demeanor. Here are the streamlined instructions to ensure the proposal is impactful and concise:

            Client-Focused Summary: Start with a brief, client-centered overview of their project. Always place the emphasis on the client ('you') rather than yourself ('I'). For instance, instead of saying "I can accomplish this," opt for "You will gain this benefit."

            Avoid Clichés: Maintain a confident tone without resorting to overused phrases like 'I hope this finds you well.' The goal is to come across as competent yet reserved, committed to quality without seeming overly eager.

            Simple Language: Keep the language simple and conversational, suitable for a fourth-grade reading level. When discussing technical aspects, make them easy to grasp.

            Direct Experience Link: Only mention experiences in your portfolio that are directly relevant to the job. If an experience or skill is absent from your portfolio, acknowledge this honestly.

            Conciseness: Ensure the proposal is direct, eliminating any superfluous text. Focus solely on your skills and experiences that meet the client's needs and the job's requirements. The proposal should clearly showcase your expertise and confidence, minus any unnecessary details.

            I also want you to write in my style. I will include a very brief summary of how I write:

            my writing style:

            'So you need an automation expert to use xyz tool to do abc. I used xyz tool to do abc for this client. I can do the same for you.

            I have years of experience of software development, so I'm very comfortable integrating tools like this together.

            Let me know if you'd like to discuss this on a phone call.'

            You're drafting a proposal for the following job:

            Job description:
            {job_description}

            ### relevant portfolio projects:
            {portfolio_jobs}
        """
        # 5) No Introduction Needed: Dive straight into the specifics of the proposal, as the introduction has already been addressed.
        print(len(proposal_prompt_template))
        prompt_template = proposal_prompt_template.strip()
        prompt_template = re.sub(' {2,}', ' ', prompt_template)
        print(len(prompt_template))

        # Create prompt from prompt template

        # for x in response["text"].split("\n"):
        #     print(x)

        ##
        proposal_prompt = PromptTemplate(
            input_variables=["job_description", "portfolio_jobs"],
            template=proposal_prompt_template,
        )

        proposal_chain = LLMChain(llm=openai_llm, prompt=proposal_prompt)
        proposal = proposal_chain.run(
            job_description=job_desc, portfolio_jobs=relevant_jobs_text
        )
        ##

        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("proposal")
        print(proposal)
