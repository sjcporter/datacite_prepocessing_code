Here is the `README.md` file for the S3-to-S3 cleaning pipeline.

```markdown
# DataCite S3 Cleaning Pipeline

This pipeline automates the process of fetching a compressed DataCite archive from an S3 bucket, processing it to fix BigQuery schema compatibility issues (specifically nulls in `REPEATED` fields), and streaming the cleaned JSONL files back to a destination S3 bucket.

## Features

* **Ephemeral Workspace**: Automatically creates a temporary directory for processing and deletes it upon completion or failure, ensuring no local storage is wasted.
* **S3 Integration**: Downloads input archives directly from S3 and streams output directly to S3.
* **Schema Validation**: Uses a BigQuery schema definition to strictly ensure `REPEATED` fields are never `null`.
* **Incremental Processing**: The Python processor checks if a file already exists in the destination S3 bucket before uploading, skipping duplicates to save time.

## Prerequisites

### System Tools
* **Bash**: A standard Unix shell.
* **AWS CLI**: Required for downloading the input archive from S3.
  * *Installation*: `pip install awscli` (or via your system package manager).
  * *Configuration*: Run `aws configure` to set your credentials.

### Python Environment
* **Python 3.6+**
* **Dependencies**:
  ```bash
  pip install smart_open boto3

```

## File Structure

Ensure the following files are present in your directory:

* `run_pipeline.sh`: The main orchestration script.
* `fix_repeated_fields.py`: The logic for cleaning JSONL records.
* `bigquery_schema.json`: The BigQuery schema definition used for validation.

## Usage

Run the pipeline using the shell script. It requires three arguments:

```bash
./run_pipeline.sh <python_script> <s3_input_archive> <s3_output_bucket>

```

### Arguments

1. **`<python_script>`**: Path to `fix_repeated_fields.py`.
2. **`<s3_input_archive>`**: The S3 URI of the source `.tar.gz` file (e.g., `s3://my-bucket/raw/data.tar.gz`).
3. **`<s3_output_bucket>`**: The destination S3 URI where cleaned files will be saved (e.g., `s3://my-bucket/cleaned/`).

### Example

```bash
chmod +x run_pipeline.sh

./run_pipeline.sh \
  ./fix_repeated_fields.py \
  s3://ds-opendatasets/raw/datacite.tar.gz \
  s3://ds-opendatasets/datacite/2024/

```

## How It Works

1. **Workspace Creation**: The script generates a unique temporary directory (e.g., `/tmp/tmp.xYz123`).
2. **Download**: It uses `aws s3 cp` to download the specified `.tar.gz` archive into the temporary workspace.
3. **Extraction**: The archive is unzipped. The script assumes the archive contains a folder named `dois`.
4. **Processing**: `fix_repeated_fields.py` is executed:
* It recursively scans the extracted files.
* It reads `bigquery_schema.json` to identify repeated fields.
* It fixes records where repeated fields are `null` (replacing them with `[]`).
* It streams the fixed records directly to the output S3 bucket.


5. **Cleanup**: A `trap` ensures the temporary directory is deleted immediately after the script exits, regardless of success or failure.

```

```
