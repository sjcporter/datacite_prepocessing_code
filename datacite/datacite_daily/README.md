```markdown
# DataCite S3 Harvester & Schema Fixer

This pipeline harvests DOI records from the DataCite API, validates and fixes them against a BigQuery schema (specifically sanitizing `null` values in repeated fields), and streams the clean data directly to Amazon S3.

It is designed for high-throughput environments, using parallel workers and a bounded memory buffer to process millions of records without exhausting system RAM or local disk space.

## Key Features

* **Schema-Aware Fixing**: Automatically fixes BigQuery compatibility issues by converting `null` values in `REPEATED` fields (arrays) to empty lists `[]` before upload.
* **Direct-to-S3 Streaming**: Data flows from the API -> RAM -> S3. No large files are ever written to the local disk.
* **Memory Safe**: Uses a "Bounded Buffer" strategy to hold a maximum of 100,000 records in memory at any time.
* **Parallel Fetching**: Splits each day into time shards and fetches them concurrently using multiple threads.
* **Automatic Partitioning**: Output files are automatically split (e.g., `records_part00.jsonl`, `records_part01.jsonl`) and organized by date.

## Prerequisites

* **Python 3.6+**
* **Dependencies**:
    ```bash
    pip install requests boto3
    ```
* **Configuration Files**:
    * `bigquery_schema.json`: A JSON file defining the BigQuery schema. This is required for the script to know which fields are `REPEATED` and need fixing.

## Usage

```bash
python dataCiteHarvest.py --start-date <YYYY-MM-DD> [options]

```

### Arguments

| Argument | Default | Description |
| --- | --- | --- |
| `--start-date` | **Required** | The date to begin harvesting (Format: `YYYY-MM-DD`). Runs until "yesterday" (UTC). |
| `--s3-uri` | `s3://.../dois` | The base S3 destination URI. Date folders will be created under this path. |
| `--schema` | `bigquery_schema.json` | Path to the BigQuery schema file used for data validation. |
| `--threads` | `6` | Number of parallel threads to use for fetching data for a single day. |
| `--verbose`, `-v` | `False` | Enable detailed logging (page-level fetches and shard timings). |

### Example Run

Harvest data from January 1, 2024, fix it using `my_schema.json`, and upload to a specific bucket:

```bash
python dataCiteHarvest.py \
  --start-date 2024-01-01 \
  --schema ./schemas/datacite_v4.json \
  --s3-uri s3://ds-opendatasets/datacite/clean/ \
  --threads 8 \
  --verbose

```

## Output Structure

The script creates a hierarchical structure in S3 optimized for partitioning:

```text
s3://<bucket>/<base_prefix>/
├── updated-2024-01/               # Partition by Month
│   ├── 2024-01-01/                # Partition by Day
│   │   ├── records_part00.jsonl   # First 100k fixed records
│   │   ├── records_part01.jsonl   # Next 100k fixed records
│   │   └── ...
│   └── 2024-01-02/
│       └── records_part00.jsonl
└── ...

```

## How It Works

1. **Schema Loading**: The script loads the provided JSON schema to identify all fields marked as `REPEATED`.
2. **Fetching**: Worker threads query the DataCite API for specific time slices of a given day.
3. **Sanitization**: Before any record is buffered, the `fix_record` function recursively traverses it. If a field defined as `REPEATED` in the schema is missing or `null`, it is replaced with `[]`.
4. **Buffering**: Clean records are pushed to a thread-safe buffer.
5. **Streaming**: Once the buffer hits 100,000 records, it is flushed directly to S3 as a new `.jsonl` part file.

```

```
