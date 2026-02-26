import requests
import json
import os
import time
import threading
import queue
import argparse
import boto3
import io
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

# Configuration
API_URL = "https://api.datacite.org/dois"
BATCH_SIZES = [1000, 100, 10]
BACKOFF_REPEATS = 10
RECORDS_PER_FILE = 100000

# Global Verbose Flag
VERBOSE = False

def vprint(*args, **kwargs):
    """Thread-safe verbose print."""
    if VERBOSE:
        print(*args, **kwargs)

# --- S3 UTILS ---

def s3_folder_exists(bucket, prefix):
    """
    Checks if an S3 'folder' (prefix) exists by listing objects with that prefix.
    Returns True if at least one object exists.
    """
    s3 = boto3.client('s3')
    # Ensure prefix ends with / to match folder behavior
    if not prefix.endswith('/'):
        prefix += '/'
        
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in response

# --- S3 BUFFER LOGIC ---

class S3MultipartWriter:
    """
    Manages writing records to S3, splitting files when they reach a record limit.
    """
    def __init__(self, bucket, base_prefix):
        self.bucket = bucket
        self.base_prefix = base_prefix
        self.s3_client = boto3.client('s3')
        
        self.part_number = 0
        self.current_record_count = 0
        self.current_buffer = io.StringIO()
        self.total_records_written = 0
        self.lock = threading.Lock()

    def _get_s3_key(self):
        filename = f"records_part{self.part_number:02d}.jsonl"
        # Ensure no double slashes
        return f"{self.base_prefix.rstrip('/')}/{filename}"

    def _flush_buffer(self):
        """Uploads the current buffer to S3 as a single file."""
        if self.current_buffer.tell() == 0:
            return

        key = self._get_s3_key()
        # Always print uploads to show progress
        print(f"Uploading {key} ({self.current_record_count} records)...")
        
        # Reset pointer to start of buffer
        self.current_buffer.seek(0)
        
        # Upload
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=self.current_buffer.getvalue().encode('utf-8')
            )
        except Exception as e:
            print(f"ERROR: Failed to upload to S3: {e}")
            raise e

        # Reset state for next file
        self.current_buffer.close()
        self.current_buffer = io.StringIO()
        self.current_record_count = 0
        self.part_number += 1

    def write_records(self, records):
        """Thread-safe write of a list of records."""
        with self.lock:
            for record in records:
                self.current_buffer.write(json.dumps(record) + '\n')
                self.current_record_count += 1
                self.total_records_written += 1
                
                # Check if we need to split
                if self.current_record_count >= RECORDS_PER_FILE:
                    self._flush_buffer()

    def close(self):
        """Finalizes any remaining data in the buffer."""
        with self.lock:
            if self.current_record_count > 0:
                self._flush_buffer()
            self.current_buffer.close()

# --- WORKER THREADS ---

def writer_worker(record_queue, s3_writer):
    """
    Consumer thread: gets records and sends to S3 writer.
    """
    seen_ids = set()
    
    while True:
        item = record_queue.get()
        if item is None:
            record_queue.task_done()
            break
        
        records = item
        unique_records = []
        
        for record in records:
            doi = record.get('id')
            if doi and doi not in seen_ids:
                seen_ids.add(doi)
                unique_records.append(record)
        
        if unique_records:
            s3_writer.write_records(unique_records)
            
        record_queue.task_done()
    
    s3_writer.close()
    vprint(f"Writer Thread: Finished. Total written: {s3_writer.total_records_written}")

def fetch_worker(worker_id, time_range, record_queue, abort_event):
    start_time, end_time = time_range
    query = f"updated:[{start_time} TO {end_time}]"
    
    vprint(f"[Thread-{worker_id}] Starting shard: {start_time} -> {end_time}")
    
    current_cursor = "1"
    batch_size_index = 0
    backoff_countdown = 0
    page_count = 0
    
    while True:
        if abort_event.is_set(): break

        if backoff_countdown == 0 and batch_size_index != 0:
            batch_size_index = 0
        
        current_size = BATCH_SIZES[batch_size_index]
        params = {"query": query, "page[size]": current_size, "page[cursor]": current_cursor}
        
        try:
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"[Thread-{worker_id}] Error: {e}")
            if batch_size_index < len(BATCH_SIZES) - 1:
                batch_size_index += 1
                backoff_countdown = BACKOFF_REPEATS
                print(f"[Thread-{worker_id}] Backing off to size {BATCH_SIZES[batch_size_index]}")
                time.sleep(2)
                continue
            else:
                print(f"[Thread-{worker_id}] Failed all retries. Aborting.")
                abort_event.set()
                break

        records = data.get('data', [])
        count = len(records)
        
        if count > 0:
            record_queue.put(records)
            if backoff_countdown > 0: backoff_countdown -= 1
        
        page_count += 1
        vprint(f"[Thread-{worker_id}] Page {page_count}: Fetched {count}")

        next_link = data.get('links', {}).get('next')
        if not next_link or count == 0:
            break
        
        try:
            parsed = urlparse(next_link)
            qs = parse_qs(parsed.query)
            if 'page[cursor]' in qs:
                current_cursor = qs['page[cursor]'][0]
            else:
                break
        except:
            break
        
        time.sleep(0.1)
    
    vprint(f"[Thread-{worker_id}] Finished.")

# --- ORCHESTRATION ---

def get_date_range_for_day(date_obj):
    if date_obj.tzinfo is None:
        date_obj = date_obj.replace(tzinfo=timezone.utc)
    start_dt = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_dt, end_dt

def split_time_range(start_dt, end_dt, chunks):
    total_seconds = (end_dt - start_dt).total_seconds()
    if total_seconds <= 0: return []
    
    chunk_seconds = total_seconds / chunks
    ranges = []
    current = start_dt
    
    for i in range(chunks):
        next_time = current + timedelta(seconds=chunk_seconds)
        if i == chunks - 1:
            next_time = end_dt
        start_str = current.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str = next_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        ranges.append((start_str, end_str))
        current = next_time
    return ranges

def process_date(target_date, num_threads, s3_base_uri):
    """Orchestrates fetching for a single day and uploading to S3."""
    start_dt, end_dt = get_date_range_for_day(target_date)
    
    # Construct S3 Prefix: .../YYYY-MM-DD/
    day_str = start_dt.strftime('%Y-%m-%d')
    
    parsed_s3 = urlparse(s3_base_uri)
    bucket_name = parsed_s3.netloc
    base_prefix = parsed_s3.path.strip('/')
    
    full_prefix = f"{base_prefix}/{day_str}"
    
    # --- CHECK IF DAY FOLDER EXISTS ---
    if s3_folder_exists(bucket_name, full_prefix):
        print(f"\nSkipping {day_str} (Folder Exists): s3://{bucket_name}/{full_prefix}/")
        return
    # ----------------------------------

    print(f"\n=== Processing {day_str} ===")
    vprint(f"Target S3: s3://{bucket_name}/{full_prefix}")

    # Initialize S3 Writer & Queue
    s3_writer = S3MultipartWriter(bucket_name, full_prefix)
    record_queue = queue.Queue()
    
    # Start Writer Thread (No schema needed anymore)
    writer_thread = threading.Thread(target=writer_worker, args=(record_queue, s3_writer))
    writer_thread.start()

    abort_event = threading.Event()
    time_shards = split_time_range(start_dt, end_dt, num_threads)
    
    threads = []
    for i, tr in enumerate(time_shards):
        t = threading.Thread(target=fetch_worker, args=(i+1, tr, record_queue, abort_event))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    record_queue.put(None)
    writer_thread.join()

    if abort_event.is_set():
        print(f"CRITICAL ERROR: Failed to process {day_str} completely.")
    else:
        print(f"=== Completed {day_str} ===")

def main():
    parser = argparse.ArgumentParser(description="Fetch DataCite to S3.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--s3-uri", default="s3://ds-opendatasets/datacite/Harvest/dois", 
                        help="Base S3 URI (without date folders)")
    # Removed --schema argument
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    
    args = parser.parse_args()

    # Set Global Verbose Flag
    global VERBOSE
    VERBOSE = args.verbose

    try:
        current_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print("Invalid date format.")
        return

    end_date = datetime.now(timezone.utc) - timedelta(days=1)
    
    while current_date.date() <= end_date.date():
        process_date(current_date, args.threads, args.s3_uri)
        current_date += timedelta(days=1)
    
    print("All operations complete.")

if __name__ == "__main__":
    main()
