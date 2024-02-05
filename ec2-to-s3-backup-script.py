import boto3
import os
import datetime

# Config
SOURCE_DIR = "/path/to/directory"
DEST_BUCKET = "s3bucketname"

def upload_files_to_s3(source_dir, dest_bucket):
    s3_client = boto3.client("s3")

    # Create a timestamp for the backup with hours and minutes
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")

    # List all files in the source directory
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            file_path = os.path.join(root, file)
            s3_key = f"backups/{timestamp}/{file}"  # Use a subdirectory for each backup

            # Upload each file to S3 (These are individual Files)
            s3_client.upload_file(file_path, dest_bucket, s3_key)
            print(f"Uploaded {file} to S3 as {s3_key}")

            # Verify upload by attempting to get the object
            try:
                s3_client.get_object(Bucket=dest_bucket, Key=s3_key)
                print(f"Verified upload of {file} to S3.")

                # Remove the local file after successful upload
                os.remove(file_path)
                print(f"Removed local file: {file_path}")

            except Exception as e:
                print(f"Upload verification failed for {file}: {e}")


if __name__ == "__main__":
    upload_files_to_s3(SOURCE_DIR, DEST_BUCKET)
