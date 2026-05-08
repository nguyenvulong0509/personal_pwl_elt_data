import os
import psycopg2

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
    # makes sure the tracking table actually exists before we try to use it
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS file_state_tracker (
            file_id VARCHAR PRIMARY KEY,
            file_name VARCHAR,
            source_system VARCHAR,  -- e.g., 'gdrive_coach', 'openpowerlifting'
            md5_hash VARCHAR,
            status VARCHAR CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED')), 
            error_log TEXT,         -- dump the python error here if it crashes
            last_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def should_process_file(file_id, current_hash):
    # checks if we've seen this exact version of the file before
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT md5_hash, status FROM file_state_tracker WHERE file_id = %s;", (file_id,))
    result = cur.fetchone()
    
    cur.close()
    conn.close()

    # if it's brand new, definitely process it
    if not result:
        return True
        
    db_hash, db_status = result
    
    # if it previously failed, or if the coach updated the sheet, process it again
    if db_status != 'SUCCESS' or db_hash != current_hash:
        return True
        
    # otherwise, skip it
    return False

def log_file_state(file_id, file_name, source_system, current_hash, status, error_log=None):
    # "upserts" the record so we know we successfully handled this version
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO file_state_tracker (file_id, file_name, source_system, md5_hash, status, error_log, last_processed)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (file_id) 
        DO UPDATE SET 
            file_name = EXCLUDED.file_name,
            source_system = EXCLUDED.source_system,
            md5_hash = EXCLUDED.md5_hash,
            status = EXCLUDED.status,
            error_log = EXCLUDED.error_log,
            last_processed = CURRENT_TIMESTAMP;
    """, (file_id, file_name, source_system, current_hash, status, error_log))
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    # run this directly to set up the table initially
    print("initializing the postgres state tracker...")
    init_db()
    print("done.")
