import os
import sys
import requests
import zipfile
import boto3
from botocore.client import Config
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# 1. PATHS & GLOBAL CONFIGURATION
root_dir = Path(__file__).resolve().parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

load_dotenv(dotenv_path=root_dir / '.env')
from src.utils.state_tracker import init_db, should_process_file, log_file_state

DATA_URL = os.getenv("OPENPWL_GITLAB_LINK")
FILE_ID = "openpwl_master_db"

def fetch_openpowerlifting():
    # 0. Ensure the state tracking table exists
    init_db()

    bucket_name = os.getenv("MINIO_BUCKET_NAME", "staging")
    current_batch_date = datetime.today().strftime('%Y-%m-%d')
    print(f"=== Initializing Ingestion Batch Window: {current_batch_date} ===")

    s3_client = boto3.client(
        's3',
        endpoint_url=f"http://localhost:{os.getenv('MINIO_API_PORT', '9000')}",
        aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'minioadmin'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin'),
        config=Config(signature_version='s3v4')
    )

    print("Pinging OpenPowerlifting servers...")
    
    # 1. The HEAD Request: Get the metadata without downloading the whole file
    try:
        head_response = requests.head(DATA_URL, allow_redirects=True)
        head_response.raise_for_status()
        
        # We use their Last-Modified server timestamp as our hash
        # If it's missing for some reason, we fallback to the ETag
        server_hash = head_response.headers.get('Last-Modified') or head_response.headers.get('ETag')
        
        if not server_hash:
            raise ValueError("Server didn't return a Last-Modified or ETag header.")
            
    except Exception as e:
        print(f"Failed to ping server: {e}")
        return

    # 2. Check Postgres
    print(f"latest version's from: {server_hash}")
    if not should_process_file(FILE_ID, server_hash):
        print("Skipping. We already have this exact dataset in the Lakehouse.")
        return

    # 3. We have new data! Let's log PENDING and start the heavy lifting
    print("New data found! Starting download...")
    log_file_state(
        FILE_ID, "openpowerlifting-latest.zip", "openpowerlifting", server_hash, "PENDING", 
        batch_date=current_batch_date, folder_path="External/GitLab"
    )
    
    staging_dir = str(root_dir / "data" / "staging")
    os.makedirs(staging_dir, exist_ok=True)
    temp_zip_path = os.path.join(staging_dir, "temp_opl.zip")
    extracted_csv_path = None

    try:
        # Download the giant zip in chunks so we don't blow up our RAM
        with requests.get(DATA_URL, stream=True) as r:
            r.raise_for_status()
            with open(temp_zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        print("Download complete. Extracting CSV...")
        
        # Extract only the CSV file directly into our staging folder
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    # The zip contains a folder with the CSV inside. We just want the CSV.
                    file_info.filename = "openpowerlifting.csv" 
                    zip_ref.extract(file_info, staging_dir)
                    extracted_csv_path = os.path.join(staging_dir, "openpowerlifting.csv")
                    break
        
        if not extracted_csv_path:
            raise FileNotFoundError("Could not find a CSV file inside the downloaded ZIP.")

        csv_filename = f"openpowerlifting/batch_date={current_batch_date}/openpowerlifting.csv"
        
        print(f"Uploading extracted CSV to MinIO at {csv_filename}...")
        s3_client.upload_file(extracted_csv_path, bucket_name, csv_filename)

        # Clean up the giant temp files to save hard drive space
        os.remove(temp_zip_path)
        os.remove(extracted_csv_path)
        
        # Log SUCCESS
        log_file_state(
            FILE_ID, "openpowerlifting.csv", "openpowerlifting", server_hash, "SUCCESS", 
            batch_date=current_batch_date, folder_path="External/GitLab"
        )
        print(f" -> Successfully safely landed data at s3://{bucket_name}/{csv_filename}")

    except Exception as e:
        # If any error/failure happens, log the crash
        log_file_state(
            FILE_ID, "openpowerlifting-latest.zip", "openpowerlifting", server_hash, "FAILED", 
            batch_date=current_batch_date, folder_path="External/GitLab", error_log=str(e)
        )
        print(f"FAILED to process data: {e}")
        
        # Clean up the broken zip file if it exists
        if os.path.exists(temp_zip_path):
            os.remove(temp_zip_path)
        if extracted_csv_path and os.path.exists(extracted_csv_path):
            os.remove(extracted_csv_path)

if __name__ == "__main__":
    fetch_openpowerlifting()
