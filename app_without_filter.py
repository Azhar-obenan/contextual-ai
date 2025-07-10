
import os
import requests
from pathlib import Path
from typing import List, Optional, Dict
import pandas as pd
from contextual import ContextualAI

import os
# from google.colab import userdata

# Get the API key from Colab secrets
# API_KEY = userdata.get("CONTEXTUAL_API_KEY")
API_KEY = "key-mpOCeQabXBTDDJ6JpV16KEfo2N7fQ58jNiaeVtPZ57cPVUhMc"

client = ContextualAI(
    api_key=API_KEY,
)

"""## Step 1: Create your Datastore


You will need to first create a datastore for your agent using the  /datastores endpoint. A datastore is secure storage for data. Each agent will have it's own datastore for storing data securely.
"""

# Check if a datastore with the name already exists
datastore_name = "Restaurant-Reviews-Datastore"
datastore_exists = False
datastore_id = None

try:
    # List all datastores
    datastores = client.datastores.list()
    
    # Check if a datastore with our name exists
    for datastore in datastores.datastores:
        if datastore.name == datastore_name:
            datastore_id = datastore.id
            datastore_exists = True
            print(f"Using existing datastore '{datastore_name}' with ID: {datastore_id}")
            break
    
    # If no matching datastore found, create a new one
    if not datastore_exists:
        result = client.datastores.create(name=datastore_name)
        datastore_id = result.id
        print(f"Created new datastore '{datastore_name}' with ID: {datastore_id}")
        
except Exception as e:
    print(f"Error checking or creating datastore: {e}")
    raise

"""## Step 2: Ingest Documents into your Datastore

You can now ingest documents into your Agent's datastore using the /datastores endpoint. Documents must be in PDF or HTML format.


I am using an example PDF. You can also use your own documents here. If you have very large documents (hundreds of pages), processing can take longer.
"""

# Check if the reviews markdown file exists locally
reviews_md_path = "/Users/prom1/Documents/sql-agent/con_ai/data/reviews_for_rag.md"
reviews_md_filename = os.path.basename(reviews_md_path)

if not os.path.exists(reviews_md_path):
    print(f"ERROR: The reviews markdown file does not exist at {reviews_md_path}")
    print("Make sure you've run the CSV to Markdown conversion script first")
    raise FileNotFoundError(f"File not found: {reviews_md_path}")
else:
    print(f"Found reviews markdown file at {reviews_md_path}")

"""Check if the file already exists in the datastore"""
# List all documents in the datastore
try:
    documents = client.datastores.documents.list(datastore_id)
    
    # Check if a document with the same name exists
    file_exists = False
    for doc in documents.documents:
        if doc.name == reviews_md_filename:
            print(f"Document '{reviews_md_filename}' already exists in datastore with ID: {doc.id}")
            document_id = doc.id
            file_exists = True
            break
    
    if not file_exists:
        # File doesn't exist in datastore, upload it
        print(f"Uploading '{reviews_md_filename}' to datastore...")
        with open(reviews_md_path, 'rb') as f:
            ingestion_result = client.datastores.documents.ingest(datastore_id, file=f)
            document_id = ingestion_result.id
            print(f"Successfully uploaded reviews to datastore {datastore_id}")
    else:
        print(f"Using existing document: {document_id}")
        
except Exception as e:
    print(f"Error checking or uploading document: {e}")
    raise

"""Once ingested, you can view the list of documents, see their metadata, and also delete documents."""

# Get document metadata and check status
def check_document_status(datastore_id, document_id, max_attempts=10, wait_seconds=5):
    import time
    
    print(f"Checking document processing status...")
    for attempt in range(max_attempts):
        metadata = client.datastores.documents.metadata(datastore_id=datastore_id, document_id=document_id)
        print(f"Attempt {attempt+1}/{max_attempts}: Document status: {metadata.status}")
        
        if metadata.status == 'done':
            print(f"✅ Document processing completed successfully!")
            return True
        elif metadata.status == 'failed':
            print(f"❌ Document processing failed!")
            return False
        elif metadata.status == 'pending' and attempt < max_attempts - 1:
            print(f"⏳ Document still processing. Waiting {wait_seconds} seconds before checking again...")
            time.sleep(wait_seconds)
        else:
            if attempt == max_attempts - 1:
                print(f"⚠️ Maximum attempts reached. Document is still in '{metadata.status}' status.")
                print(f"The agent might not have access to the document content yet.")
                return False
            
    return False

# Check document status
metadata = client.datastores.documents.metadata(datastore_id=datastore_id, document_id=document_id)
print("Initial document metadata:", metadata)

if metadata.status != 'done':
    print("\nDocument is not yet fully processed. Waiting for processing to complete...")
    document_ready = check_document_status(datastore_id, document_id, max_attempts=3, wait_seconds=2)
    
    if not document_ready:
        print("\nWARNING: Document processing hasn't completed in the expected time.")
        print("The agent may not be able to access the document content for answering questions.")
        print("Options: 1) Continue anyway, 2) Wait longer manually, 3) Check the Contextual.ai dashboard")
        response = input("Do you want to continue anyway? (y/n): ")
        
        if response.lower() != 'y':
            print("Exiting script. Please check the document status in the Contextual.ai dashboard.")
            import sys
            sys.exit(0)
    else:
        print("\nDocument is ready for querying!")
else:
    print("\nDocument is already processed and ready for querying!")

"""## Step 3: Create your Agent

Next let's create the Agent and modify it to our needs.

Some additional parameters include setting a system prompt or using a previously tuned model.

`system_prompt` is used for the instructions that your RAG system references when generating responses. Note that we do not guarantee that the system will follow these instructions exactly.

`llm_model_id` is the optional model ID of a tuned model to use for generation. Contextual AI will use a default model if none is specified.
"""

system_prompt = '''
You are an AI assistant specialized in restaurant review analysis. Your responses should be precise, accurate, and sourced exclusively from the customer review data provided to you. Please follow these guidelines:

Review Analysis & Response Quality:
* Only use information explicitly stated in the provided review documentation
* Present sentiment analyses using structured formats with tables and bullet points where appropriate
* Identify trends in customer feedback across different time periods when relevant
* Highlight patterns in positive and negative reviews to identify strengths and areas for improvement
* Note any unusual or outlier reviews that may skew overall sentiment analysis

Analytical Capabilities:
* Provide accurate counts of positive, negative, and neutral reviews
* Identify common keywords and phrases in both positive and negative reviews
* Track sentiment trends over time when timestamp data is available
* Analyze customer satisfaction levels across different aspects (food, service, atmosphere)
* Suggest potential improvements based on negative feedback patterns
* Identify strengths based on positive feedback patterns

Response Format:
* Begin with a high-level summary of key findings when analyzing reviews
* Structure detailed analyses in clear, hierarchical formats
* Use markdown for lists, tables, and emphasized text
* Maintain a professional, analytical tone
* Present quantitative data in consistent formats

Critical Guidelines:
* Base analyses on factual review content, not speculation
* If information is unavailable or irrelevant, clearly state this without additional commentary
* Answer questions directly and completely
* Do not reference source document names or file types in responses
* Focus only on information that directly answers the query

For any analysis, provide comprehensive insights using all relevant available information while maintaining strict adherence to these guidelines and focusing on delivering clear, actionable information that can help improve customer experience and business performance.
'''

"""Let's create our agent."""

# Check if an agent with the name already exists
agent_name = "Restaurant-Reviews-Analyzer"
agent_exists = False
agent_id = None

try:
    # List all agents
    agents = client.agents.list()
    
    # Check if an agent with our name exists
    for agent in agents.agents:
        if agent.name == agent_name:
            agent_id = agent.id
            agent_exists = True
            print(f"Using existing agent '{agent_name}' with ID: {agent_id}")
            break
    
    # If no matching agent found, create a new one
    if not agent_exists:
        app_response = client.agents.create(
            name=agent_name,
            description="Research Agent for analyzing restaurant customer reviews",
            system_prompt=system_prompt,
            datastore_ids=[datastore_id]
        )
        agent_id = app_response.id
        print(f"Created new agent '{agent_name}' with ID: {agent_id}")
        
except Exception as e:
    print(f"Error checking or creating agent: {e}")
    raise

## (Optional) Step 3a: Connect to a different agent
# agent_id = ''

"""## Step 4: Query your Agent

Let's query our agent to see if its working. The required information is the agent_id and messages.  

Optional information includes parameters for streaming, conversation_id, and model_id if using a different fine tuned model.

**Note:** It may take a few minutes for the document to be ingested and processed. The Assistant will give a detailed answer once the documents are ingested.
"""

query_result = client.agents.query.create(
    agent_id=agent_id,
    messages=[{
        "content": "how we can increase our business based on negative reviews?",
        "role": "user"
    }]
)
print(query_result.message.content)