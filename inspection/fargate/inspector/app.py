# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import traceback
import boto3
from urllib.parse import urlparse
import pandas as pd
import json

claudeModelId = 'anthropic.claude-3-sonnet-20240229-v1:0' 
bedrock_runtime = boto3.client('bedrock-runtime')

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

def merge_schema(original, adjustments):
    merged = original.copy()
    for m in merged['columns']:
        for a in adjustments:
            if m['name'] == a['name']:
                print(f"Got adjustment {a} for field {m['name']}")
                m['type'] = a['new_type']
                m['description'] = a['new_description']
    return merged 

# Inputs: 
#   locations for subsamples of data
def main():

    print("Task starting")
    s3_input1 = os.environ['S3_INPUT_1']
    print(f"s3_input1: {s3_input1}")
    s3_input2 = os.environ['S3_INPUT_2']
    print(f"s3_input2: {s3_input2}")
    s3_output = os.environ['S3_OUTPUT_1']
    print(f"s3_output: {s3_output}")
    s3_output_final = os.environ['S3_OUTPUT_2']
    print(f"s3_output_final: {s3_output_final}")

    base_dir = "/opt"

    try:
        s3_parts = urlparse(s3_input1, allow_fragments=False)
        bucket = s3_parts.netloc
        input_name1 = s3_parts.path.lstrip('/')
        s3_parts = urlparse(s3_input2, allow_fragments=False)
        input_name2 = s3_parts.path.lstrip('/')

        s3 = boto3.client('s3')
        input_path1 = os.path.join(base_dir, "data1.csv")
        input_path2 = os.path.join(base_dir, "data2.csv")
        print(f"Downloading s3://{bucket}/{input_name1} to {input_path1}")
        s3.download_file(bucket, input_name1, input_path1)
        print(f"Downloading s3://{bucket}/{input_name2} to {input_path2}")
        s3.download_file(bucket, input_name2, input_path2)

        print(f"Reading {input_path1}")
        with open(input_path1, 'r') as F:
            input_lines1 = F.readlines()
        print(f"Reading {input_path2}")
        with open(input_path2, 'r') as F:
            input_lines2 = F.readlines()

        print(f"First-pass inspection")
        prompt1 = """Below is a set of 100 rows of sample data. Help me create a business catalog entry for this data set, including the column names, data types, and column descriptions. 

Provide the output in a JSON structure in this format.

<sample_json>
{
columns: [
  {
    "name": "DOB",
    "type": "date",
    "description": "date of birth"
  },
  {
    "name": "first_name",
    "type": "string",
    "description": "first name"
  }
]
}
</sample_json>

<data>
"""
        prompt1 = prompt1 + "\n".join(input_lines1) + "\n</data>"
        first_answer = parse_claude_json(call_claude(prompt1))
        print(f"Got first-pass schema: {first_answer}")

        # Write JSON to disk
        output_path = os.path.join(base_dir, "schema1.json")
        with open(output_path, 'w') as F:
            json.dump(first_answer, F)

        output_parts = urlparse(s3_output, allow_fragments=False)
        output_name = output_parts.path.lstrip('/')
        print(f"Uploading {output_path} to s3://{bucket}/{output_name}")
        s3.upload_file(output_path, bucket, output_name)
        
        prompt2 = """The last time we chatted, I sent you a set of 100 rows of sample data. I asked you to help me create a business catalog entry for this data set, including the column names, data types, and column descriptions. 

Here's what you sent me:

<catalog>
"""
        prompt2 = prompt2 + json.dumps(first_answer) + """
</catalog>

I'm sending you a different sample of another 100 rows. Look at that, and see if you'd suggest any changes to the original business catalog entries you provided.

<data>
"""
        prompt2 = prompt2 + "\n".join(input_lines2) + """
</data>

Provide your response in this JSON format:

<response_format>
{
  "Changes": [
  {
    "name": "units_sold",
    "original_type": "string",
    "original_description": "Quantity of units sold",
    "new_type": "int",
    "new_description": "Quantity of units sold"
  }
   ]
}
</response_format>

If there are no changes necessary, you can return an empty list in JSON format.
"""
        second_answer = call_claude(prompt2)
        print(f"Got schema adjustments: {second_answer}")
        output_parts = urlparse(s3_output_final, allow_fragments=False)
        output_name = output_parts.path.lstrip('/')
        adjustments = parse_claude_adjustment_json(second_answer)['Changes']
        if len(adjustments) == 0:
            print(f"No adjustments")
        else:
            print(f"Got {len(adjustments)} changes to make")
            adjusted = merge_schema(first_answer, adjustments)
            output_path = os.path.join(base_dir, "schema2.json")
            with open(output_path, 'w') as F:
                json.dump(adjusted, F)
        print(f"Uploading {output_path} to s3://{bucket}/{output_name}")
        s3.upload_file(output_path, bucket, output_name)
        

    except Exception as e:
        trc = traceback.format_exc()
        print(trc)
        print(str(e))
        raise e

if __name__ == "__main__":
    main()