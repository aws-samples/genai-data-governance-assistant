# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
This module implements the ReMatch method from this paper: https://arxiv.org/html/2403.01567v1.

1. [Prerequisite] Create embedding for each schema document
2. Retrieve similar schemas
3. Ask Claude to evaluate potential matches
"""

import os
import traceback
import boto3
from urllib.parse import urlparse
import json
from requests_aws4auth import AWS4Auth
from langchain.vectorstores import OpenSearchVectorSearch
from opensearchpy import RequestsHttpConnection
from langchain_community.embeddings import BedrockEmbeddings

claudeModelId = 'anthropic.claude-3-sonnet-20240229-v1:0' 
embedModelId = 'amazon.titan-embed-text-v1'
bedrock_runtime = boto3.client('bedrock-runtime')
credentials = boto3.Session().get_credentials()
region = boto3.Session().region_name
awsauth = AWS4Auth(region=region, service='aoss', refreshable_credentials=credentials)
embeddings = BedrockEmbeddings()

def get_context_from_opensearch(query, opensearch_domain_endpoint, opensearch_index):
    opensearch_endpoint = opensearch_domain_endpoint
    docsearch = OpenSearchVectorSearch(
        index_name=opensearch_index,
        embedding_function=embeddings,
        opensearch_url=opensearch_endpoint,
        http_auth=awsauth,
        timeout=300,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )
    docs_with_scores = docsearch.similarity_search_with_score(query, k=3, vector_field="embedding", text_field="passage")
    docs = [doc[0] for doc in docs_with_scores]
    print(f"docs received from opensearch:\n{docs}")
    return docs # return list of matching docs

def get_embedding(text):
    accept = 'application/json'
    contentType = 'application/json'
    inp = json.dumps({"inputText": text})
    response = bedrock_runtime.invoke_model(body=inp, modelId=embedModelId, accept=accept, contentType=contentType)
    response_body = json.loads(response.get('body').read())
    embedding = response_body.get('embedding')
    return embedding

def call_claude(query):

    claudePayload = json.dumps({ 
        "anthropic_version": "bedrock-2023-05-31",
        'max_tokens': 2048,
    	"messages": [
          {
            "role": "user",
            "content": [
              {
                "type": "text",
                "text": query
              }
            ]
          }
        ]
    })
    

    response = bedrock_runtime.invoke_model(
        body=claudePayload, 
        modelId=claudeModelId, 
        accept='application/json', 
        contentType='application/json'
    )

    body = response.get('body').read().decode('utf-8')

    response_body = json.loads(body)
    final_answer = response_body['content'][0]['text']
    print(f"final_answer: {final_answer}")
    return final_answer

def parse_claude_json(r):
    idx = r.find('```json') + len('```json')
    if idx == -1:
        return json.loads(r)
    s = r[idx:]
    idx = s.find('```')
    t = s[:idx]
    return json.loads(t)

def parse_claude_adjustment_json(r):
    idx = r.find('<response_format>') + len('<response_format>')
    if idx == -1:
        return json.loads(r)
    s = r[idx:]
    idx = s.find('</response_format>')
    t = s[:idx]
    return json.loads(t)

# Inputs: 
#   location of JSON schema document
#   opensearch domain endpoint
#   opensearch index name
#   location for output files
def main():

    print("Task starting")
    s3_input = os.environ['S3_INPUT']
    print(f"s3_input1: {s3_input}")
    s3_output_matches = os.environ['S3_OUTPUT']
    print(f"s3_output: {s3_output_matches}")
    s3_output_desc = os.environ['S3_OUTPUT_DESC']
    print(f"s3_output_desc: {s3_output_desc}")
    os_domain_endpoint = os.environ['OSS_DOMAIN_ENDPOINT']
    print(f"os_domain_endpoint: {os_domain_endpoint}")
    os_index_name = os.environ['OSS_INDEX_NAME']
    print(f"os_index_name: {os_index_name}")

    base_dir = "/opt"

    try:
        s3_parts = urlparse(s3_input, allow_fragments=False)
        bucket = s3_parts.netloc
        input_name = s3_parts.path.lstrip('/')

        s3 = boto3.client('s3')
        input_path = os.path.join(base_dir, "data1.csv")
        print(f"Downloading s3://{bucket}/{input_name} to {input_path}")
        s3.download_file(bucket, input_name, input_path)

        print(f"Reading {input_path}")
        with open(input_path, 'r') as F:
            input_lines = F.readlines()

        print(f"Convert to document")
        prompt1 = """Here's a table schema in JSON format. Please write a paragraph describing this table. Include a description of the table's purpose and a brief description of each column.

<schema>
"""
        prompt1 = prompt1 + "\n".join(input_lines) + "\n</schema>"
        first_answer = call_claude(prompt1)
        print(f"Got schema document: {first_answer}")

        # Write output to disk
        output_path = os.path.join(base_dir, "description.txt")
        with open(output_path, 'w') as F:
            F.write(first_answer)
        output_parts = urlparse(s3_output_desc, allow_fragments=False)
        output_name = output_parts.path.lstrip('/')
        print(f"Uploading {output_path} to s3://{bucket}/{output_name}")
        s3.upload_file(output_path, bucket, output_name)

        print(f"Get context from opensearch")
        context = get_context_from_opensearch(first_answer, os_domain_endpoint, os_index_name)
        context_formatted =  [{"page_content": doc.page_content} for doc in context]
        print(f"context_formatted: {str(context_formatted)}")

        if len(context_formatted) == 0:
            print("No context found")
            return
        
        print("Getting possible schema matches")
        prompt2 = """You are an expert in databases, and duplicate schema detection. Your task is to look at the following new schema 
and the top-3 possible similar schemas. It's unlikely that a duplicate would match exactly. Rather, a person may have accidentally created
a duplicate schema with very similar column names, possibly in a different order.

For each of the similar schemas, provide a confidence score ranging from 0.0 (no confidence) to 1.0 (certainty) showing whether the similar
schema is a duplicate of the original schema. Also provide a reason for the score. 

Provide this output format:
<response_format>
{
  "possible_matches": [
  {
    "table": "other_table_1",
    "confidence": 0.0,
    "reason": "reason"
  },
  {
    "table": "other_table_2",
    "confidence": 0.75,
    "reason": "reason"
  }
  ]
}
</response_format>

<new_schema>
"""
        prompt2 = prompt2 + first_answer + "\n</new_schema>"
        if len(context_formatted) > 0:
            prompt2 = prompt2 + "\n<possible_match_1>\n" + context_formatted[0]['page_content'] + "\n</possible_match_1>"
        if len(context_formatted) > 1:
            prompt2 = prompt2 + "\n<possible_match_2>\n" + context_formatted[1]['page_content'] + "\n</possible_match_2>"
        if len(context_formatted) > 2:
            prompt2 = prompt2 + "\n<possible_match_3>\n" + context_formatted[2]['page_content'] + "\n</possible_match_3>"
        second_answer = call_claude(prompt2)
        print(f"Got schema match response: {second_answer}")

        # Write output to disk
        output_path = os.path.join(base_dir, "matches.json")
        with open(output_path, 'w') as F:
            #json.dump(first_answer, F)
            F.write(second_answer)

        output_parts = urlparse(s3_output_matches, allow_fragments=False)
        output_name = output_parts.path.lstrip('/')
        print(f"Uploading {output_path} to s3://{bucket}/{output_name}")
        s3.upload_file(output_path, bucket, output_name)

    except Exception as e:
        trc = traceback.format_exc()
        print(trc)
        print(str(e))
        raise e

if __name__ == "__main__":
    main()