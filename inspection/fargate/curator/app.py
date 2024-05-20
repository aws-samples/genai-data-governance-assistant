# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import uuid
import traceback
import boto3
from urllib.parse import urlparse
import pandas as pd
import json
from opensearchpy.helpers import bulk
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

claudeModelId = 'anthropic.claude-3-sonnet-20240229-v1:0' 
embedModelId = 'amazon.titan-embed-text-v1'
bedrock_runtime = boto3.client('bedrock-runtime')
glueclient = boto3.client('glue')

credentials = boto3.Session().get_credentials()
region = boto3.Session().region_name
awsauth = AWS4Auth(region=region, service='aoss', refreshable_credentials=credentials)

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
    idx2 = r.find('<sample_json>') + len('<sample_json>')
    if idx != -1:
        s = r[idx:]
        idx = s.find('```')
        t = s[:idx]
        return json.loads(t)
    if idx2 != -1:
        s = r[idx:]
        idx = s.find('</sample_json>')
        t = s[:idx]
        return json.loads(t)
    else:
        return json.loads(r)

def get_glue_type(t):
    if t == 'string':
        return 'string'
    elif t == 'integer':
        return 'int'
    elif t == 'float' or t == 'number':
        return 'double'
    else:
        raise Exception(f"Unknown type {t}")

def truncate_desc(desc):
    idx = desc.find('1.')
    if idx != -1:
        return desc[:idx]
    else:
        return desc

def create_glue_catalog_entry(schema_json, table_desc, glue_catalog, table_name, s3_path):
    print(f"Creating Glue catalog entry for {table_name}")
    columns = []
    for c in schema_json['columns']:
        glue_c = {
            'Name': c['name'],
            'Type': get_glue_type(c['type']),
            'Comment': c['description']
        }
        columns.append(glue_c)
    glueclient.create_table(
        DatabaseName=glue_catalog,
        TableInput={
            'Name': table_name,
            'Description': truncate_desc(table_desc),
            'StorageDescriptor': {
                'Columns': columns,
                'Location': s3_path,
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                "SerdeInfo": {
                    "Parameters": {
                        "classification": "csv"
                    }
                },
            },
            "Parameters": {
                "classification": "csv",
            },
            'TableType': 'EXTERNAL_TABLE'
        }
    )

def get_embedding(text):
    accept = 'application/json'
    contentType = 'application/json'
    inp = json.dumps({"inputText": text})
    response = bedrock_runtime.invoke_model(body=inp, modelId=embedModelId, accept=accept, contentType=contentType)
    response_body = json.loads(response.get('body').read())
    embedding = response_body.get('embedding')
    return embedding

# Inputs: 
#   location of original data set in S3
#   location of schema JSON in S3
#   location of table description in S3
#   location of glue dqdl rules in S3
#   name of glue database
#   name of table
#   number of rows to sample for DQDL
def main():

    print("Task starting")
    s3_input_data = os.environ['S3_INPUT_DATA']
    print(f"s3_input_data: {s3_input_data}")
    s3_input_schema = os.environ['S3_INPUT_SCHEMA']
    print(f"s3_input_schema: {s3_input_schema}")
    s3_input_desc = os.environ['S3_INPUT_DESC']
    print(f"s3_input_desc: {s3_input_desc}")
    s3_input_dqdl = os.environ['S3_INPUT_DQDL']
    print(f"s3_input_dqdl: {s3_input_dqdl}")
    s3_output_dqdl = os.environ['S3_OUTPUT_DQDL']
    print(f"s3_output_dqdl: {s3_output_dqdl}")
    glue_catalog = os.environ['GLUE_CATALOG']
    print(f"glue_catalog: {glue_catalog}")
    table_name = os.environ['TABLE_NAME']
    print(f"table_name: {table_name}")
    num_rows = os.environ['NUM_ROWS']
    print(f"num_rows: {num_rows}")
    os_domain_endpoint = os.environ['OSS_DOMAIN_ENDPOINT']
    print(f"os_domain_endpoint: {os_domain_endpoint}")
    os_index_name = os.environ['OSS_INDEX_NAME']
    print(f"os_index_name: {os_index_name}")

    base_dir = "/opt"

    try:
        # Download from S3
        s3_parts = urlparse(s3_input_data, allow_fragments=False)
        bucket = s3_parts.netloc
        input_name_data = s3_parts.path.lstrip('/')
        s3_parts = urlparse(s3_input_schema, allow_fragments=False)
        input_name_schema = s3_parts.path.lstrip('/')
        s3_parts = urlparse(s3_input_desc, allow_fragments=False)
        input_name_desc = s3_parts.path.lstrip('/')
        s3_parts = urlparse(s3_input_dqdl, allow_fragments=False)
        input_name_dqdl = s3_parts.path.lstrip('/')
        s3 = boto3.client('s3')
        input_path_data = os.path.join(base_dir, "data.csv")
        print(f"Downloading s3://{bucket}/{input_name_data} to {input_path_data}")
        s3.download_file(bucket, input_name_data, input_path_data)
        input_path_schema = os.path.join(base_dir, "schema.json")
        print(f"Downloading s3://{bucket}/{input_name_schema} to {input_path_schema}")
        s3.download_file(bucket, input_name_schema, input_path_schema)
        input_path_desc = os.path.join(base_dir, "description.txt")
        print(f"Downloading s3://{bucket}/{input_name_desc} to {input_path_desc}")
        s3.download_file(bucket, input_name_desc, input_path_desc)
        input_path_dqdl = os.path.join(base_dir, "dqdl.txt")
        print(f"Downloading s3://{bucket}/{input_name_dqdl} to {input_path_dqdl}")
        s3.download_file(bucket, input_name_dqdl, input_path_dqdl)

        # Read from disk
        print(f"Reading {input_path_data}")
        with open(input_path_data, 'r') as F:
            input_lines_data = F.readlines()
        print(f"Reading {input_path_schema}")
        with open(input_path_schema, 'r') as F:
            input_lines_schema = F.readlines()
        print(f"Reading {input_path_desc}")
        with open(input_path_desc, 'r') as F:
            input_lines_desc = F.readlines()
        print(f"Reading {input_path_dqdl}")
        with open(input_path_dqdl, 'r') as F:
            input_lines_dqdl = F.readlines()

        # glue catalog
        print("Creating Glue catalog entry")
        create_glue_catalog_entry(json.loads("\n".join(input_lines_schema)), "\n".join(input_lines_desc), glue_catalog, table_name, s3_input_data)

        # send embeddings to OpenSearch
        print(f"Sending embeddings to OpenSearch")
        opensearch = OpenSearch(
            hosts=f"{os_domain_endpoint}:443",
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=300
        )
        requests = []
        content = "\n".join(input_lines_desc)
        embedding = get_embedding(content)
        _id = str(uuid.uuid4())
        request = {
            "_op_type": "index",
            "_index": os_index_name,
            "embedding": embedding,
            "passage": content,
            "doc_id": _id,
        }
        requests.append(request)
        bulk(opensearch, requests)

        # subsample
        print(f"Reading {input_path_data} and sampling")
        df = pd.read_csv(input_path_data)
        sample = df.sample(n=int(num_rows))
        sample_data = sample.to_csv(index=False)

        print("Creating DQDL rules")
        prompt1 = """Below is a set of 100 rows of sample data. For this dataset, I already have the schema and a description of the table. I want to write a set of data quality rules for AWS Glue that help me enforce data quality. I have the data quality language definition available as well. Using these inputs, write a set of Glue DQDL rules for the dataset.

Provide the output in a JSON structure in this format.

<sample_json>
Rules = [
    rule 1,
   rule 2
]
</sample_json>

<data>
"""
        prompt1 = prompt1 + sample_data + "\n</data>"
        prompt1 = prompt1 + "\n<schema>" + "\n".join(input_lines_schema) + "\n</schema>"
        prompt1 = prompt1 + "\n<table_description>" + "\n".join(input_lines_desc) + "\n</table_description>"
        prompt1 = prompt1 + "\n<dqdl>" + "\n".join(input_lines_dqdl) + "\n</dqdl>"
        dqdl_rules = call_claude(prompt1)
        print(f"Got DQDL: {dqdl_rules}")

        # Write rules to disk
        output_path = os.path.join(base_dir, "rules.txt")
        with open(output_path, 'w') as F:
            F.write(dqdl_rules)

        output_parts = urlparse(s3_output_dqdl, allow_fragments=False)
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