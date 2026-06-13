import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# grab db creds from the environment, fallback to the docker defaults
DB_HOST = os.getenv("POSTGRES_HOST") # 'postgres' is the container name
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("POSTGRES_PORT")

if not DB_PASS or not DB_USER:
    raise ValueError("ERROR: Database credentials are not set in the environment. Halting execution.")

def get_conn():
    # spins up a quick connection to our metadata db
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

def init_db():
    """Initializes the tracking table with support for partitioned batch dates and folder paths."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS file_state_tracker (
                    file_id VARCHAR PRIMARY KEY,
                    file_name VARCHAR,
                    folder_path VARCHAR,     -- Differentiates files with same names
                    source_system VARCHAR,
                    batch_date VARCHAR,     -- Tracks the partition date (YYYY-MM-DD)
                    md5_hash VARCHAR,
                    status VARCHAR CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED')), 
                    error_log TEXT,
                    last_processed TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP 
                );
            """)
    finally:
        conn.close()

def should_process_file(file_id, current_hash):
    """Checks if a file needs processing based on its hash and previous status."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT md5_hash, status FROM file_state_tracker WHERE file_id = %s;", (file_id,))
                result = cur.fetchone()
    finally:
        conn.close()

    if not result:
        return True
        
    db_hash, db_status = result
    if db_status != 'SUCCESS' or db_hash != current_hash:
        return True
        
    return False

def log_file_state(file_id, file_name, source_system, current_hash, status, batch_date=None, folder_path=None, error_log=None):
    """Logs or updates the processing state of a file in PostgreSQL using an Upsert."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                    INSERT INTO file_state_tracker (
                        file_id, file_name, folder_path, source_system, 
                        batch_date, md5_hash, status, error_log, last_processed
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (file_id) 
                    DO UPDATE SET 
                        file_name = EXCLUDED.file_name,
                        folder_path = EXCLUDED.folder_path,
                        source_system = EXCLUDED.source_system,
                        batch_date = EXCLUDED.batch_date,
                        md5_hash = EXCLUDED.md5_hash,
                        status = EXCLUDED.status,
                        error_log = EXCLUDED.error_log,
                        last_processed = CURRENT_TIMESTAMP;
                """, (file_id, file_name, folder_path, source_system, batch_date, current_hash, status, error_log))
                except Exception as e:
                    print(f"Database error while logging state: {e}")
                    conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    # run this directly to set up the table initially
    print("initializing the postgres state tracker...")
    init_db()
    print("done.")