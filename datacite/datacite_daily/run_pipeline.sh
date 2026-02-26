#!/bin/bash

# Configuration
START_DATE="2024-01-01"  # Adjust this date as needed
S3_TARGET="s3://ds-opendatasets/datacite/2024/"
SCHEMA_FILE="bigquery_schema.json"

echo "=================================================="
echo "Starting DataCite Harvest & Fix Pipeline"
echo "Start Date: $START_DATE"
echo "Target S3:  $S3_TARGET"
echo "=================================================="

# 1. Install Dependencies
echo "[1/3] Checking dependencies..."
pip install requests smart_open boto3 --quiet
if [ $? -ne 0 ]; then
    echo "Error: Failed to install Python dependencies."
    exit 1
fi

# 2. Run Data Harvest
# This downloads data to local folders (e.g., ./2024-01-01/)
# It is smart enough to skip local folders that already exist.
echo "[2/3] Running DataCite Harvest..."
python dataCiteHarvest.py --start-date "$START_DATE"

if [ $? -ne 0 ]; then
    echo "Error: dataCiteHarvest.py failed."
    exit 1
fi

# 3. Run Fix & Upload
# This scans the current directory (.) for JSONL files, 
# checks if they exist in S3, and uploads only the new ones.
echo "[3/3] Running Fix & Upload..."
python fix_repeated_fields.py \
    --input_dir . \
    --output_dir "$S3_TARGET" \
    --schema "$SCHEMA_FILE"

if [ $? -eq 0 ]; then
    echo "=================================================="
    echo "Pipeline Completed Successfully."
    echo "=================================================="
else
    echo "Error: fix_repeated_fields.py failed."
    exit 1
fi
