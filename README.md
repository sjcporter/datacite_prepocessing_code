
```markdown
# DataCite Preprocessing Code for ds-open-datasets

This repository contains pipelines and utilities designed to harvest, clean, and preprocess DataCite metadata for ingestion into Google BigQuery. The core focus of these tools is to resolve common BigQuery schema compatibility issues and stream large volumes of processed data directly to Amazon S3.

## 📖 Overview

The project is divided into three main components, handling both daily incremental updates and bulk historical snapshots:

1. **`datacite_daily/`**: An API harvester for daily DataCite records.
2. **`datacite_snapshots/`**: An S3-to-S3 processing pipeline for bulk historical archives.
3. **`schemas/fix_repeated_fields/`**: The core BigQuery schema fixing utility used across the pipelines.

---

## ✨ Core Features

* **Schema-Aware Fixing**: All tools read a provided BigQuery JSON schema to identify `REPEATED` fields. They automatically prevent ingestion errors by converting any missing or `null` values in these arrays to empty lists (`[]`). 
* **Deep Recursion**: The schema fixer correctly handles deeply nested records and arrays of records.
* **Direct-to-S3 Streaming**: Data flows directly into Amazon S3, avoiding the need to write large intermediate files to local disk.
* **Memory Safe & Parallel Processing**: The daily harvester uses a bounded buffer strategy (holding a maximum of 100,000 records in RAM) and splits fetching into concurrent time shards using multiple threads.

---

## 🗂️ Project Modules

### 1. DataCite Daily Harvester (`/datacite_daily`)
Harvests new DOI records directly from the DataCite API.
* Runs concurrently using specified thread counts.
* Automatically partitions output `.jsonl` files in S3 by date (e.g., `updated-YYYY-MM/YYYY-MM-DD/`).

### 2. DataCite Snapshots Pipeline (`/datacite_snapshots`)
A shell-orchestrated pipeline for cleaning large `.tar.gz` data dumps.
* Fetches a compressed DataCite archive from an S3 bucket.
* Creates an ephemeral, temporary workspace that is automatically deleted upon completion or failure.
* Extracts the archive, fixes the schema recursively, and streams the cleaned JSONL records back to a destination S3 bucket.

### 3. BigQuery JSONL Schema Fixer (`/schemas/fix_repeated_fields`)
The underlying Python utility that performs the data manipulation.
* Reads from a local directory, preserves subdirectory structures, and streams cleaned data directly to an S3 destination.

---

## 🚀 Getting Started

### Prerequisites

* **Python:** 3.6+
* **AWS CLI:** Required for the snapshots pipeline to download input archives. Ensure your credentials are configured via `~/.aws/credentials`, environment variables, or an IAM role.
* **System Tools:** A standard Unix shell (Bash).

### Installation

Install the required Python dependencies:

```bash
pip install requests boto3 smart_open

```

**

### Quick Usage Examples

**Running the Daily Harvester:**

```bash
cd datacite_daily
python dataCiteHarvest.py \
  --start-date 2024-01-01 \
  --schema ../schemas/bigquery_schema.json \
  --s3-uri s3://my-bucket/datacite/clean/ \
  --threads 8

```

**

**Running the Snapshot Pipeline:**

```bash
cd datacite_snapshots
chmod +x run_pipeline.sh
./run_pipeline.sh \
  ../schemas/fix_repeated_fields/fix_repeated_fields.py \
  s3://my-bucket/raw/datacite.tar.gz \
  s3://my-bucket/datacite/2024/

```

**

For detailed configuration options and arguments, refer to the individual `README.md` files within the `datacite_daily/`, `datacite_snapshots/`, and `schemas/fix_repeated_fields/` directories.

```

```
