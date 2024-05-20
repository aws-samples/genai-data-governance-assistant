# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import traceback
import boto3
from urllib.parse import urlparse
import pandas as pd

# Inputs: 
#   location of data set in S3 (assumed to be single file)
#   location for subsample of data
#   number of rows to subsample
def main():

    print("Task starting")
    s3_input = os.environ['S3_INPUT']
    print(f"s3_input: {s3_input}")
    s3_output = os.environ['S3_OUTPUT']
    print(f"s3_output: {s3_output}")
    num_rows = os.environ['NUM_ROWS']
    print(f"num_rows: {num_rows}")

    base_dir = "/opt"

    try:
        s3_parts = urlparse(s3_input, allow_fragments=False)
        bucket = s3_parts.netloc
        input_name = s3_parts.path.lstrip('/')

        s3 = boto3.client('s3')
        input_path = os.path.join(base_dir, "data.csv")
        print(f"Downloading s3://{bucket}/{input_name} to {input_path}")
        s3.download_file(bucket, input_name, input_path)

        print(f"Reading {input_path} and sampling")
        df = pd.read_csv(input_path)
        sample = df.sample(n=int(num_rows))

        output_path = os.path.join(base_dir, "sample.csv")
        print(f"Writing sample to {output_path}")
        sample.to_csv(output_path, index=False)

        output_parts = urlparse(s3_output, allow_fragments=False)
        output_name = output_parts.path.lstrip('/')
        print(f"Uploading {output_path} to s3://{bucket}/{output_name}")
        s3.upload_file(output_path, bucket, output_name)

    except Exception as e:
        trc = traceback.format_exc()
        print(trc)
        print(str(e))

if __name__ == "__main__":
    main()