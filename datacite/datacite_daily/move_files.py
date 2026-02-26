import boto3
import re
from concurrent.futures import ThreadPoolExecutor
import threading

# --- CONFIGURATION ---
SOURCE_BUCKET = 'your-source-bucket-name'
DEST_BUCKET = 'your-dest-bucket-name' # Can be the same as source
SOURCE_PREFIX = 'datacite/Harvest/dois/updated-' # Narrow down the search
MAX_THREADS = 100 # Increase if running on EC2, decrease if on local wifi

s3 = boto3.client('s3')
counter = 0
lock = threading.Lock()

def copy_object(obj):
    global counter
    old_key = obj['Key']
    
    # Regex to remove 'updated-YYYY-MM/' from the path
    # Matches: /updated-2026-01/ and replaces with /
    new_key = re.sub(r'/updated-\d{4}-\d{2}/', '/', old_key)

    if new_key == old_key:
        print(f"Skipping (pattern not found): {old_key}")
        return

    try:
        s3.copy_object(
            CopySource={'Bucket': SOURCE_BUCKET, 'Key': old_key},
            Bucket=DEST_BUCKET,
            Key=new_key
        )
        with lock:
            counter += 1
            if counter % 1000 == 0:
                print(f"Copied {counter} files...")
    except Exception as e:
        print(f"Error copying {old_key}: {e}")

def main():
    print("Listing objects...")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)

    # Flatten the iterator to get a list of all objects
    # (For massive lists >1M, you might want to process page-by-page instead)
    all_objects = [obj for page in pages for obj in page.get('Contents', [])]
    
    print(f"Found {len(all_objects)} objects. Starting copy...")
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(copy_object, all_objects)
        
    print(f"Done! Copied {counter} files.")

if __name__ == '__main__':
    main()
