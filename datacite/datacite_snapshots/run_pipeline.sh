#!/bin/bash

# Usage: ./run_pipeline.sh <python_script> <s3_input_archive> <s3_output_bucket>

# 1. Read parameters
PYTHON_SCRIPT_PATH="$1"
S3_INPUT_ARCHIVE="$2"  # e.g., s3://my-source-bucket/datacite.tar.gz
S3_OUTPUT_URI="$3"     # e.g., s3://my-dest-bucket/datacite-clean/

# Check if parameters are provided
if [[ -z "$PYTHON_SCRIPT_PATH" || -z "$S3_INPUT_ARCHIVE" || -z "$S3_OUTPUT_URI" ]]; then
    echo "Usage: $0 <python_script> <s3_input_archive> <s3_output_bucket>"
    echo "Example: $0 ./fix_repeated_fields.py s3://ds-opendatasets/datacite.tar.gz s3://ds-opendatasets/clean/"
    exit 1
fi

# 2. Validate inputs
if [[ ! -f "$PYTHON_SCRIPT_PATH" ]]; then
    echo "Error: Python script not found at '$PYTHON_SCRIPT_PATH'"
    exit 1
fi

# 3. Create Temporary Directory
# mktemp -d creates a unique temporary directory
WORKING_DIR=$(mktemp -d)
echo "Created temporary workspace: $WORKING_DIR"

# Ensure cleanup happens on exit (success, error, or interrupt)
cleanup() {
    echo "Cleaning up workspace..."
    rm -rf "$WORKING_DIR"
    echo "Done."
}
trap cleanup EXIT

# 4. Get absolute path of Python script
# (We need the absolute path because we are about to change directories)
PYTHON_SCRIPT_ABS_PATH="$(cd "$(dirname "$PYTHON_SCRIPT_PATH")" && pwd)/$(basename "$PYTHON_SCRIPT_PATH")"

echo "------------------------------------------------"
echo "Pipeline Configuration:"
echo "Python Script:   $PYTHON_SCRIPT_ABS_PATH"
echo "S3 Input:        $S3_INPUT_ARCHIVE"
echo "S3 Output:       $S3_OUTPUT_URI"
echo "Workspace:       $WORKING_DIR"
echo "------------------------------------------------"

# 5. Navigate to Working Directory
cd "$WORKING_DIR" || exit 1

# 6. Download Archive from S3
ARCHIVE_NAME=$(basename "$S3_INPUT_ARCHIVE")

echo "[Step 1/3] Downloading $ARCHIVE_NAME from S3..."
aws s3 cp "$S3_INPUT_ARCHIVE" .

if [ $? -ne 0 ]; then
    echo "Error: Failed to download file from S3."
    exit 1
fi

# 7. Unzip the Archive
echo "[Step 2/3] Extracting $ARCHIVE_NAME..."
tar -xzvf "$ARCHIVE_NAME"

if [ $? -ne 0 ]; then
    echo "Error: Failed to extract archive."
    exit 1
fi

# 8. Run the Python Cleaning Script
# We assume the tarball extracts into a folder named 'dois'
# If your tarball structure is different, update INPUT_REL below.
INPUT_REL="./dois"

echo "[Step 3/3] Running Python cleaning script..."
python "$PYTHON_SCRIPT_ABS_PATH" --input_dir "$INPUT_REL" --output_dir "$S3_OUTPUT_URI"

if [ $? -eq 0 ]; then
    echo "------------------------------------------------"
    echo "Pipeline completed successfully."
    echo "Cleaned files uploaded to: $S3_OUTPUT_URI"
    echo "------------------------------------------------"
else
    echo "Error: Python script encountered an issue."
    exit 1
fi

# The 'trap' function will automatically run here to delete $WORKING_DIR
