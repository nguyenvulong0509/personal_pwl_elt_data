import os
import sys
from pathlib import Path
from dotenv import load_dotenv

import boto3
from botocore.exceptions import ClientError

# --- 1. PATHS & CONFIGURATION ---
root_dir = Path(__file__).resolve().parent.parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

load_dotenv(dotenv_path=root_dir / '.env')

MINIO_ENDPOINT = f"http://localhost:{os.getenv('MINIO_API_PORT', '9000')}"
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')

BUCKETS_TO_CREATE = [
    os.getenv("MINIO_BUCKET_NAME", "staging"),
    "lakehouse"
]

def initialize_buckets():
    """Connects to MinIO and creates necessary buckets if they don't exist."""
    print("--- Initializing MinIO Buckets ---")
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    for bucket in BUCKETS_TO_CREATE:
        try:
            s3_client.head_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' already exists. Skipping.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Bucket '{bucket}' not found. Creating it...")
                s3_client.create_bucket(Bucket=bucket)
                print(f" -> Successfully created bucket '{bucket}'.")
            else:
                print(f"Error checking bucket '{bucket}': {e}")

if __name__ == "__main__":
    initialize_buckets()