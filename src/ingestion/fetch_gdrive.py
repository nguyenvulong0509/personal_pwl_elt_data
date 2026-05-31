import os
import io
import sys
import csv
import glob
import re
import time
from pathlib import Path
from dotenv import load_dotenv

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 1. Path Configurations
root_dir = Path(__file__).resolve().parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

load_dotenv(dotenv_path=root_dir / '.env')
from src.utils.state_tracker import should_process_file, log_file_state

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]
FOLDER_ID = os.getenv("drive_folder_id")
STAGING_DIR = root_dir / "data" / "staging" / "training_logs"

# ==========================================
# HELPER: RATE LIMIT HANDLER
# ==========================================

def execute_with_retry(request, max_retries=5):
    """Executes an API request with exponential backoff if a 429 error is hit."""
    for n in range(max_retries):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status == 429:
                wait_time = (2 ** n) + (n * 0.5) 
                print(f"      ! Quota hit (429). Waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception("Max retries exceeded for API request")

# ==========================================
# PART 1: EXTRACTION
# ==========================================

def authenticate_gdrive():
    creds = None
    token_path = root_dir / 'token.json'
    creds_path = root_dir / 'credentials.json'
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
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

def download_all_tabs_as_csvs(sheets_service, file_id, file_name, folder_path):
    sheet_metadata = execute_with_retry(sheets_service.spreadsheets().get(spreadsheetId=file_id))
    sheets = sheet_metadata.get('sheets', [])
    
    # Standardize the folder/file names for Windows paths
    safe_folder = re.sub(r'[\\/*?:"<>|]', "-", folder_path)
    safe_file = re.sub(r'[\\/*?:"<>|]', "-", file_name)

    for sheet in sheets:
        sheet_title = sheet.get("properties", {}).get("title", "")
        if any(x in sheet_title.upper() for x in ["TEMPLATE", "SETUP", "NOTE"]):
            continue
            
        print(f"    -> Saving Tab: '{sheet_title}'")
        
        result = execute_with_retry(sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, 
            range=f"'{sheet_title}'"
        ))
        
        values = result.get('values', [])
        if not values: continue

        # The Triple-Underscore format is key for the Spark script to know the context later
        csv_filename = f"{safe_folder}___{safe_file}___{sheet_title}.csv"
        file_path = STAGING_DIR / csv_filename
        
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerows(values)
        
        time.sleep(0.4) # Respect the quota
            
    return True

# ==========================================
# MAIN EXECUTION (INGESTION ONLY)
# ==========================================

def fetch_logs():
    os.makedirs(STAGING_DIR, exist_ok=True)
    creds = authenticate_gdrive()
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    print(f"Scanning Folder: {FOLDER_ID}...")
    items = get_all_files_in_folder_recursive(drive_service, FOLDER_ID)

    for item in items:
        file_id, file_name, modified_time, folder_path = item['id'], item['name'], item['modifiedTime'], item['folder_path']
        
        if should_process_file(file_id, modified_time):
            print(f"\nIngesting updated file: [{folder_path}] {file_name}")
            try:
                log_file_state(file_id, file_name, "gdrive_coach", modified_time, "PENDING")
                download_all_tabs_as_csvs(sheets_service, file_id, file_name, folder_path)
                log_file_state(file_id, file_name, "gdrive_coach", modified_time, "SUCCESS")
            except Exception as e:
                print(f"Error: {e}")
                log_file_state(file_id, file_name, "gdrive_coach", modified_time, "FAILED", str(e))
        else:
            print(f"Skipping: {file_name} (No new changes)")

    print("\nExtraction phase complete. All raw files are in data/staging/training_logs/")

if __name__ == '__main__':
    fetch_logs()