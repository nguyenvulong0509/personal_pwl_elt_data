import os
import requests
import zipfile
import csv
from io import BytesIO
from google.cloud import bigquery
from dotenv import load_dotenv

# The official bulk data URL often redirects or updates, but the project
# maintains their static data dumps at data.openpowerlifting.org
DATA_URL = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
EXTRACT_DIR = "data/bronze/openpowerlifting"

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

def load_to_bigquery(csv_file_path: str, project_id: str, dataset_id: str, table_name: str):
    """Loads the CSV data into Google BigQuery."""
    print(f"Loading {csv_file_path} into BigQuery ({project_id}.{dataset_id}.{table_name})...")
    client = bigquery.Client(project=project_id)
    # The client is initialized with the project, so we only need to provide
    # "dataset_id.table_name". This prevents duplicating the project ID if it's
    # already part of the dataset_id environment variable.
    table_id = f"{dataset_id}.{table_name}"

    # Read the header to dynamically generate a STRING schema for all columns.
    # This prevents BigQuery autodetect from failing on type mismatches (e.g. floats in integer columns).
    with open(csv_file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    schema = [bigquery.SchemaField(col_name, "STRING") for col_name in header]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(csv_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete

    destination_table = client.get_table(table_id)
    print(f"Successfully loaded {destination_table.num_rows} rows into the '{table_name}' table in BigQuery.")

if __name__ == "__main__":
    load_dotenv()
    
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    DATASET_ID = os.getenv("BQ_RAW_DATASET_ID")
    TABLE_NAME = "raw_openpowerlifting"

    if not PROJECT_ID or not DATASET_ID:
        raise ValueError("Please set GCP_PROJECT_ID and BQ_RAW_DATASET_ID in your environment variables or .env file.")

    os.makedirs(EXTRACT_DIR, exist_ok=True)
    
    try:
        csv_path = download_openpowerlifting_data(DATA_URL, EXTRACT_DIR)
        load_to_bigquery(csv_path, PROJECT_ID, DATASET_ID, TABLE_NAME)
    except requests.exceptions.RequestException as e:
        print(f"Failed to download the data. Error: {e}")