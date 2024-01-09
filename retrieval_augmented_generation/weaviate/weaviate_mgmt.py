import sys, os

import weaviate
import weaviate.classes as wvc

from docx import Document

import json
import traceback


if __name__ == "__main__":
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
            verbose=True, temperature=0.1, model_name="gpt-4-1106-preview"
        )

        prompt_template = """
        Instruction: You are an expert proposal writer. You are writing a proposal for a job on Upwork. You will read the job description, find the most relevant job in your portfolio, and write a proposal for the job. Use absolutely ZERO generic language like 'I hope this finds you well.' Write like you are competent and slightly arrogant and aloof. You are the expert. You do good work and care about the results, but you don't care about winning any particular proposal. Convey this subtly - don't state it explicitly. Write at a fourth grade level - no fancy words unless absolutely necessary. You are writing a proposal for the following job:

        ### Job description:
        {job_description}

        ### portfolio jobs:
        {portfolio_jobs}

        """

        # Create prompt from prompt template
        prompt = PromptTemplate(
            input_variables=["job_description", "portfolio_jobs"],
            template=prompt_template,
        )

        # Create llm chain
        llm_chain = LLMChain(llm=openai_llm, prompt=prompt)

        rag_chain = {
            "job_description": retriever,
            "portfolio_jobs": RunnablePassthrough(),
        } | llm_chain

        job_desc = """
Need someone to help integrate and understand our business and how we could utilize open phone to work with other platforms like Airtable, zapier, etc. to automate our business."""

        response = rag_chain.invoke(job_desc)

        print(response)

        for x in response['text'].split('\n'):
            print(x)
