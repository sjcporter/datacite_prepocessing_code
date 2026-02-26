import zipfile
import boto3
import os
import argparse
import mimetypes

def stream_unzip_to_s3(zip_file_path, bucket_name, s3_prefix=""):
    """
    Reads a local zip file and streams extracted files directly to S3.
    """
    s3_client = boto3.client('s3')
    
    # Verify zip file exists
    if not os.path.exists(zip_file_path):
        print(f"Error: File not found: {zip_file_path}")
        return

    print(f"Opening zip file: {zip_file_path}")
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            # List all files in the zip
            file_list = zf.namelist()
            print(f"Found {len(file_list)} items in archive.")

            for file_name in file_list:
                # Skip directories (entries ending in /)
                if file_name.endswith('/'):
                    continue

                # Construct the destination S3 key
                # We combine the prefix with the file name from the zip
                s3_key = os.path.join(s3_prefix, file_name)
                
                # Normalize slashes for S3 (S3 uses forward slashes)
                s3_key = s3_key.replace(os.sep, '/')

                print(f"Streaming {file_name} -> s3://{bucket_name}/{s3_key}")

                # Open the file object from the zip archive
                with zf.open(file_name) as source_stream:
                    # Guess the content type (mime type) based on file extension
                    content_type, _ = mimetypes.guess_type(file_name)
                    if content_type is None:
                        content_type = 'application/octet-stream'

                    # Upload the stream directly to S3
                    s3_client.upload_fileobj(
                        Fileobj=source_stream,
                        Bucket=bucket_name,
                        Key=s3_key,
                        ExtraArgs={'ContentType': content_type}
                    )

    except zipfile.BadZipFile:
        print("Error: The file is not a valid zip archive.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream unzip content to S3")
    parser.add_argument("--zip_file", required=True, help="Path to the local zip file")
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name")
    parser.add_argument("--prefix", default="", help="Optional S3 folder prefix (e.g., 'extracted_data/')")

    args = parser.parse_args()

    stream_unzip_to_s3(args.zip_file, args.bucket, args.prefix)
