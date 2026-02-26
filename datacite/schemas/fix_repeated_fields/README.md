# BigQuery JSONL Schema Fixer

This utility preprocesses JSONL files for Google BigQuery ingestion. It resolves the common `Field is repeated but input has null` error by ensuring that all fields defined as `REPEATED` in your BigQuery schema are correctly initialized as empty lists (`[]`) instead of `null`.

## Features

* **Schema-Aware**: Reads your specific BigQuery schema to identify exactly which fields require correction.
* **Deep Recursion**: Correctly handles deeply nested records and arrays of records.
* **Direct-to-S3 Streaming**: Reads from a local directory and streams cleaned data directly to an Amazon S3 bucket, avoiding large local intermediate files.
* **Structure Preservation**: Maintains the subdirectory structure of your input folder when writing to the destination.

## Prerequisites

* **Python 3.6+**
* **AWS Credentials**: Configured via `~/.aws/credentials`, environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), or IAM role.

### Installation

Install the required dependencies:

```bash
pip install smart_open boto3
