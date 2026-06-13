import os
import io
import sys
import csv
import re
import time
import random
from tenacity import retry, stop_after_attempt, retry_if_exception
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

import boto3
from botocore.client import Config
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# 1. PATHS & GLOBAL CONFIGURATION
root_dir = Path(__file__).resolve().parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

# Explicitly load .env first so os.getenv works immediately below
load_dotenv(dotenv_path=root_dir / '.env')
from src.utils.state_tracker import should_process_file, log_file_state

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]

FOLDER_ID = os.getenv("drive_folder_id")


# 2. HELPER FUNCTIONS
# Tenacity-based retry logic for handling 429 errors with exponential backoff and jitter
def custom_wait_strategy(retry_state):
    # retry_state.attempt_number bắt đầu từ 1 (tương ứng với n + 1)
    n = retry_state.attempt_number - 1 
    max_wait = 60.0
    
    base_wait = min(max_wait, ((2 ** n) + (n * 0.5)) * 2)
    jitter = random.uniform(0, 1.5)
    return base_wait + jitter

def is_429_error(exception):
    """Bộ lọc chỉ bắt lỗi HttpError có status là 429."""
    if isinstance(exception, HttpError):
        return exception.resp.status == 429
    return False

def log_before_sleep(retry_state):
    """Hàm tự động chạy để print thông báo trước khi hệ thống ngủ (sleep)."""
    # Lấy số giây chuẩn bị chờ từ chiến lược tính toán phía trên
    wait_time = retry_state.next_action.sleep
    print(f"      ! Quota hit (429). Waiting {wait_time:.1f}s before retry...")

# Định nghĩa cấu hình Retry bằng Decorator
@retry(
    retry=retry_if_exception(is_429_error), 
    stop=stop_after_attempt(6),            
    wait=custom_wait_strategy,             
    before_sleep=log_before_sleep,         
    reraise=True                           
)
def execute_with_retry(request):
    """Executes an API request. Tenacity handles all 429 retry logic automatically."""
    return request.execute()

# 2.1 Authentication helper function
def authenticate_gdrive():
    # Path to the JSON key you downloaded from GCP
    key_path = root_dir / sa = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    
    # Define the same scopes
    scopes = [
        os.getenv("GOOGLE_DRIVE_SCOPES"),
        os.getenv("GOOGLE_SHEET_SCOPES")
    ]
    
    # This automatically handles the credentials without user interaction
    creds = service_account.Credentials.from_service_account_file(
        key_path, scopes=scopes
    )
    
    return creds

def get_all_files_in_folder_recursive(service, current_folder_id, current_path="Root"):
    all_spreadsheets = []
    file_query = f"'{current_folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    file_results = execute_with_retry(service.files().list(q=file_query, fields="files(id, name, modifiedTime)"))
    
    for item in file_results.get('files', []):
        item['folder_path'] = current_path
        all_spreadsheets.append(item)
    
    folder_query = f"'{current_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folder_results = execute_with_retry(service.files().list(q=folder_query, fields="files(id, name)"))
    
    for folder in folder_results.get('files', []):
        new_path = f"{current_path}/{folder['name']}" if current_path != "Root" else folder['name']
        all_spreadsheets.extend(get_all_files_in_folder_recursive(service, folder['id'], new_path))
    return all_spreadsheets

# ==========================================
# 3. EXTRACTION & LOADING WORKER
# ==========================================
def download_and_upload_to_minio(s3_client, bucket_name, sheets_service, file_id, file_name, folder_path, current_batch_date):
    sheet_metadata = execute_with_retry(sheets_service.spreadsheets().get(spreadsheetId=file_id))
    sheets = sheet_metadata.get('sheets', [])
    
    clean_folder = re.sub(r'[^a-zA-Z0-9 \-_]', '', folder_path).strip()
    clean_file = re.sub(r'[^a-zA-Z0-9 \-_]', '', file_name).strip()

    for sheet in sheets:
        sheet_title = sheet.get("properties", {}).get("title", "")
        if any(x in sheet_title.upper() for x in ["TEMPLATE", "SETUP", "NOTE"]):
            continue
            
        print(f"    -> Processing Tab: '{sheet_title}'")
        
        result = execute_with_retry(sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, 
            range=f"'{sheet_title}'"
        ))
        
        values = result.get('values', [])
        if not values: 
            continue

        clean_sheet = re.sub(r'[^a-zA-Z0-9 \-_]', '', sheet_title).strip()
        csv_filename = f"training_logs/batch_date={current_batch_date}/{clean_folder}___{clean_file}___{clean_sheet}.csv"
        
        # Write to memory stream
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(values)
        
        # Extract payload and close stream cleanly
        raw_csv_string = csv_buffer.getvalue()
        csv_buffer.close()
        
        payload_bytes = raw_csv_string.encode('utf-8-sig')
        
        # Ship payload using explicit metadata bounds
        s3_client.put_object(
            Bucket=bucket_name,
            Key=csv_filename,
            Body=payload_bytes,
            ContentType='text/csv',
            ContentLength=len(payload_bytes)
        )
        time.sleep(0.2) 
            
    return True

# ==========================================
# 4. MAIN ORCHESTRATION PIPELINE
# ==========================================
def fetch_logs():
    if not FOLDER_ID:
        raise ValueError("CRITICAL ERROR: 'drive_folder_id' is missing from your .env file.")

    bucket_name = os.getenv("MINIO_BUCKET_NAME", "staging")
    
    # Generate the global batch execution date for this run
    current_batch_date = datetime.today().strftime('%Y-%m-%d')
    print(f"=== Initializing Ingestion Batch Window: {current_batch_date} ===")

    # Initialize Google Auth and Drive/Sheets Services
    creds = authenticate_gdrive()
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    # Initialize MinIO S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=f"http://localhost:{os.getenv('MINIO_API_PORT', '9000')}",
        aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'minioadmin'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin'),
        config=Config(signature_version='s3v4')
    )
    
    # Retrieve items to process
    items = get_all_files_in_folder_recursive(drive_service, FOLDER_ID)

    for item in items:
        file_id, file_name, modified_time, folder_path = item['id'], item['name'], item['modifiedTime'], item['folder_path']
    
        if should_process_file(file_id, modified_time):  
            print(f"\nIngesting updated file: [{folder_path}] {file_name}")
            try:
                log_file_state(file_id, file_name, "gdrive_coach", modified_time, "PENDING")
                
                # Pass current_batch_date down
                download_and_upload_to_minio(
                    s3_client, bucket_name, sheets_service, 
                    file_id, file_name, folder_path, current_batch_date
                )
                
                log_file_state(file_id, file_name, "gdrive_coach", modified_time, "SUCCESS")
            except Exception as e:
                print(f"Error: {e}")
                log_file_state(file_id, file_name, "gdrive_coach", modified_time, "FAILED", str(e))
        else:
            print(f"Skipping: {file_name} (No new changes)")

    print(f"\nExtraction & Loading complete for Batch {current_batch_date}.")

if __name__ == '__main__':
    fetch_logs()