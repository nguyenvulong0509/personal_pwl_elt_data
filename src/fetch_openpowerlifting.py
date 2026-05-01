import os
import requests
import zipfile
import duckdb
from io import BytesIO

# The official bulk data URL often redirects or updates, but the project
# maintains their static data dumps at data.openpowerlifting.org
DATA_URL = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
EXTRACT_DIR = "data/bronze/openpowerlifting"
DB_PATH = "state/etl_state.duckdb"

def download_openpowerlifting_data(url: str, extract_to: str):
    """Downloads and extracts the latest OpenPowerlifting dataset."""
    print(f"Downloading dataset from {url}...")
    response = requests.get(url)
    response.raise_for_status()  # Check for download errors

    print("Download complete. Extracting files...")
    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        zip_ref.extractall(extract_to)
        
        # The zip typically extracts into a subfolder named with a date hash
        # Let's find the exact path to the CSV file
        for root, dirs, files in os.walk(extract_to):
            for file in files:
                if file.endswith('.csv'):
                    csv_path = os.path.join(root, file)
                    print(f"Dataset extracted successfully to: {csv_path}")
                    return csv_path
                    
    raise FileNotFoundError("Could not find a CSV file in the extracted archive.")

def load_to_duckdb(csv_file_path: str, db_path: str):
    """Loads the CSV data into DuckDB."""
    print(f"Loading {csv_file_path} into DuckDB ({db_path})...")
    conn = duckdb.connect(db_path)
    
    # Create or replace a table with the OpenPowerlifting data
    conn.execute(f"""
        CREATE OR REPLACE TABLE openpowerlifting AS 
        SELECT * FROM read_csv_auto('{csv_file_path}');
    """)
    
    # Verify the load
    row_count = conn.execute("SELECT COUNT(*) FROM openpowerlifting").fetchone()[0]
    print(f"Successfully loaded {row_count} rows into the 'openpowerlifting' table.")
    conn.close()

if __name__ == "__main__":
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    try:
        csv_path = download_openpowerlifting_data(DATA_URL, EXTRACT_DIR)
        load_to_duckdb(csv_path, DB_PATH)
    except requests.exceptions.RequestException as e:
        print(f"Failed to download the data. Error: {e}")