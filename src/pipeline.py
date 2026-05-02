import os
import io
import csv
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

load_dotenv()
folder_id = os.getenv('drive_folder_id')

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SILVER_DIR = 'data/silver/workouts/'

def authenticate_gdrive():
    """Handles Google Drive OAuth 2.0 authentication."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def load_to_bigquery(csv_file_path: str, project_id: str, dataset_id: str, table_name: str):
    """Loads the CSV data into Google BigQuery."""
    print(f"Loading {csv_file_path} into BigQuery ({project_id}.{dataset_id}.{table_name})...")
    client = bigquery.Client(project=project_id)
    # The client is initialized with the project, so we only need to provide
    # "dataset_id.table_name". This prevents duplicating the project ID if it's
    # already part of the dataset_id environment variable.
    table_id = f"{dataset_id}.{table_name}"

    # Read the header to dynamically generate a STRING schema for all columns.
    # This prevents BigQuery autodetect from failing on type mismatches.
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
    print(f"Successfully loaded data into '{table_name}' table in BigQuery.")