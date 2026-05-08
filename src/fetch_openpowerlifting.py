import os
import requests
import zipfile
from src.utils.state_tracker import should_process_file, log_file_state

DATA_URL = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
STAGING_DIR = "data/staging"
FILE_ID = "openpwl_master_db"

def fetch_openpowerlifting():
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
    log_file_state(FILE_ID, "openpowerlifting-latest.zip", "openpowerlifting", server_hash, "PENDING")
    
    os.makedirs(STAGING_DIR, exist_ok=True)
    temp_zip_path = os.path.join(STAGING_DIR, "temp_opl.zip")

    try:
        # Download the giant zip in chunks so we don't blow up our RAM
        with requests.get(DATA_URL, stream=True) as r:
            r.raise_for_status()
            with open(temp_zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        print("Download complete. Extracting CSV...")
        
        # Extract only the CSV file directly into our staging folder
        extracted_csv_path = None
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    # The zip contains a folder with the CSV inside. We just want the CSV.
                    file_info.filename = "openpowerlifting.csv" 
                    zip_ref.extract(file_info, STAGING_DIR)
                    extracted_csv_path = os.path.join(STAGING_DIR, "openpowerlifting.csv")
                    break
        
        if not extracted_csv_path:
            raise FileNotFoundError("Could not find a CSV file inside the downloaded ZIP.")

        # Clean up the giant temp zip file to save hard drive space
        os.remove(temp_zip_path)
        
        # Log SUCCESS
        log_file_state(FILE_ID, "openpowerlifting.csv", "openpowerlifting", server_hash, "SUCCESS")
        print(f" -> Successfully safely landed data at {extracted_csv_path}")

    except Exception as e:
        # If any error/failure happens, log the crash
        log_file_state(FILE_ID, "openpowerlifting-latest.zip", "openpowerlifting", server_hash, "FAILED", error_log=str(e))
        print(f"FAILED to process data: {e}")
        
        # Clean up the broken zip file if it exists
        if os.path.exists(temp_zip_path):
            os.remove(temp_zip_path)

if __name__ == "__main__":
    fetch_openpowerlifting()
