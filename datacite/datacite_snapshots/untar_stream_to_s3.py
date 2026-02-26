import tarfile
import boto3
import os
import argparse
import mimetypes

def stream_targz_to_s3(tar_file_path, bucket_name, s3_prefix=""):
    """
    Reads a local .tar.gz file and streams extracted files directly to S3.
    """
    s3_client = boto3.client('s3')
    
    if not os.path.exists(tar_file_path):
        print(f"Error: File not found: {tar_file_path}")
        return

    print(f"Opening archive: {tar_file_path}")
    
    try:
        # 'r:gz' opens the file for reading with gzip compression
        with tarfile.open(tar_file_path, 'r:gz') as tar:
            
            # Iterate over the members of the tar file as a stream
            # This is memory efficient (doesn't load all filenames into RAM)
            for member in tar:
                
                # We only want to upload actual files, not directory entries
                if not member.isfile():
                    continue

                # Construct the destination S3 key
                # member.name contains the path inside the tar (e.g., "folder/file.txt")
                s3_key = os.path.join(s3_prefix, member.name)
                
                # Normalize slashes for S3
                s3_key = s3_key.replace(os.sep, '/')

                print(f"Streaming {member.name} -> s3://{bucket_name}/{s3_key}")

                # Extract the file object as a stream
                f_stream = tar.extractfile(member)
                
                if f_stream:
                    # Guess the content type
                    content_type, _ = mimetypes.guess_type(member.name)
                    if content_type is None:
                        content_type = 'application/octet-stream'

                    # Upload stream to S3
                    try:
                        s3_client.upload_fileobj(
                            Fileobj=f_stream,
                            Bucket=bucket_name,
                            Key=s3_key,
                            ExtraArgs={'ContentType': content_type}
                        )
                    finally:
                        f_stream.close()

    except tarfile.TarError as e:
        print(f"Error: The file is not a valid tar archive. {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream tar.gz content to S3")
    parser.add_argument("--tar_file", required=True, help="Path to the local .tar.gz file")
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name")
    parser.add_argument("--prefix", default="", help="Optional S3 folder prefix")

    args = parser.parse_args()

    stream_targz_to_s3(args.tar_file, args.bucket, args.prefix)
