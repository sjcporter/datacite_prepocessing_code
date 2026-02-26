import json
import os
import argparse
import boto3
from smart_open import open
from urllib.parse import urlparse
from botocore.exceptions import ClientError

def load_schema(schema_path):
    """Loads the BigQuery schema from a JSON file."""
    try:
        with open(schema_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading schema from {schema_path}: {e}")
        exit(1)

def s3_key_exists(s3_uri):
    """Checks if a key exists in S3 without downloading it."""
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        # If it's another error (e.g. 403 Forbidden), re-raise or log it
        print(f"Error checking S3 key {s3_uri}: {e}")
        return False

def fix_record(data, schema_fields):
    """
    Recursively traverses a dictionary and ensures REPEATED fields are lists, not None.
    """
    if not isinstance(data, dict):
        return

    for field in schema_fields:
        key = field['name']
        mode = field.get('mode', 'NULLABLE')
        dtype = field.get('type', 'STRING')
        sub_fields = field.get('fields', [])

        if mode == 'REPEATED':
            if key not in data or data[key] is None:
                data[key] = []
            if dtype == 'RECORD' and isinstance(data[key], list):
                for item in data[key]:
                    fix_record(item, sub_fields)
        elif dtype == 'RECORD':
            if key in data and isinstance(data[key], dict):
                fix_record(data[key], sub_fields)

def process_file(input_path, output_path, schema):
    """Reads local JSONL, fixes it, and writes to S3."""
    print(f"Processing: {input_path} -> {output_path}")
    
    try:
        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    fix_record(record, schema)
                    outfile.write(json.dumps(record) + '\n')
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON in {input_path} line {line_num}: {e}")
    except Exception as e:
        print(f"Error processing file {input_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Fix BigQuery JSONL & Upload to S3 (Incremental).")
    parser.add_argument("--input_dir", required=True, help="Local directory containing raw .jsonl files")
    parser.add_argument("--output_dir", required=True, help="Target S3 bucket URI")
    parser.add_argument("--schema", default="bigquery_schema.json", help="Path to schema file")
    
    args = parser.parse_args()
    schema = load_schema(args.schema)
    base_output_dir = args.output_dir.rstrip('/')
    
    files_processed = 0
    files_skipped = 0

    # Walk the LOCAL directory
    for root, dirs, files in os.walk(args.input_dir):
        for filename in files:
            if filename.endswith(".jsonl"):
                local_input_path = os.path.join(root, filename)
                
                # Calculate relative path (e.g., 2024-01-01/records.jsonl)
                relative_path = os.path.relpath(local_input_path, args.input_dir)
                
                # Construct S3 destination path
                s3_output_path = f"{base_output_dir}/{relative_path.replace(os.sep, '/')}"
                
                # CHECK IF EXISTS
                if s3_output_path.startswith("s3://") and s3_key_exists(s3_output_path):
                    print(f"Skipping (Already Exists): {s3_output_path}")
                    files_skipped += 1
                    continue
                
                process_file(local_input_path, s3_output_path, schema)
                files_processed += 1

    print(f"Done. Uploaded: {files_processed}, Skipped: {files_skipped}.")

if __name__ == "__main__":
    main()
