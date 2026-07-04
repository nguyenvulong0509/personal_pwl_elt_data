import io
import csv
import os
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import urllib.request
import sys

# ==========================================
# 1. DEFINE THE STRICT BRONZE SCHEMA
# ==========================================
bronze_schema = StructType([
    # Client Metadata
    StructField("client_name", StringType(), True),
    StructField("client_age", StringType(), True),
    StructField("week_num", StringType(), True),
    StructField("week_start_date", StringType(), True),
    StructField("week_end_date", StringType(), True),
    
    # Workout Data
    StructField("day_of_week", StringType(), True),
    StructField("exercise_name", StringType(), True),
    StructField("sets", StringType(), True),
    StructField("reps", StringType(), True),
    StructField("rpe_target", StringType(), True),
    StructField("rpe_actual", StringType(), True),
    StructField("pct_rm", StringType(), True),
    StructField("load_raw", StringType(), True),
    StructField("tempo", StringType(), True),
    StructField("method", StringType(), True),
    StructField("rest_time", StringType(), True),
    StructField("notes_coach", StringType(), True),
    StructField("notes_client", StringType(), True),
    StructField("notes_sleep", StringType(), True),
    StructField("notes_meal", StringType(), True),
    
    # Lineage (For debugging & dbt validation)
    StructField("source_file", StringType(), True),       # e.g., "Week 1.csv"
    StructField("source_folder", StringType(), True),     # e.g., "nguyen_vu_long"
    StructField("source_full_path", StringType(), True)   # e.g., "s3a://.../Week 1.csv"
])

# Get a list of expected column names for pandas alignment
EXPECTED_COLUMNS = [field.name for field in bronze_schema.fields]

# ==========================================
# 2. THE DISTRIBUTED PYTHON PARSER 
# ==========================================
def parse_vn_powerlifting_csv(iterator):
    """
    Runs on Spark workers to parse binary CSVs using Polars via mapInArrow.
    """
    for batch in iterator:
        # Convert Arrow Batch directly to Polars (Zero-copy)
        input_df = pl.from_arrow(batch)
        all_extracted_tables = []
        
        for row in input_df.iter_rows(named=True):
            full_path = row['path']
            file_name = os.path.basename(full_path)
            folder_path = os.path.dirname(full_path)
            parent_folder = os.path.basename(folder_path)
            binary_content = row['content']
            
            try:
                raw_csv_text = binary_content.decode('utf-8')
                reader = csv.reader(io.StringIO(raw_csv_text))
                
                client_name, age, week, start_date, end_date = None, None, None, None, None
                current_day = None
                headers = []
                in_data_block = False
                parsed_rows = []
                
                for csv_row in reader:
                    if not csv_row: 
                        continue
                    
                    # A. EXTRACT METADATA
                    if len(csv_row) >= 3:
                        if "Học viên" in csv_row[1]: client_name = csv_row[2]
                        elif "Tuổi" in csv_row[1]: age = csv_row[2]
                        elif "Tuần" in csv_row[1]: week = csv_row[2]
                    if len(csv_row) >= 4 and "Ngày" in csv_row[1]:
                        start_date = csv_row[2]
                        end_date = csv_row[3]

                    # B. DETECT THE DAY
                    if str(csv_row[0]).strip().startswith("Thứ "):
                        current_day = str(csv_row[0]).strip()
                        in_data_block = False
                        continue

                    # C. DETECT THE TABLE ANCHOR
                    if str(csv_row[0]).strip() == "Exercise" and "Sets" in csv_row:
                        headers = [h.strip() for h in csv_row]
                        if len(headers) > 1 and headers[1] == "":
                            headers[1] = "Exercise_Name" # Prevent collision with column 0
                        in_data_block = True
                        continue

                    # D. DETECT END OF TABLE
                    if in_data_block and ("Total sets" in str(csv_row[0]) or "Warm-up" in str(csv_row[0])):
                        in_data_block = False
                        continue

                    # E. EXTRACT DATA ROWS
                    if in_data_block and any(csv_row) and len(csv_row) == len(headers):
                        row_dict = dict(zip(headers, csv_row))
                        row_dict.update({
                            'client_name': client_name,
                            'client_age': age,
                            'week_num': week,
                            'week_start_date': start_date,
                            'week_end_date': end_date,
                            'day_of_week': current_day
                        })
                        parsed_rows.append(row_dict)

                # F. CONVERT TO PANDAS & CLEAN UP
                if parsed_rows:
                    df = pl.from_dicts(parsed_rows).cast(pl.String)
                    
                    # Clean empty strings and Forward Fill exercise names
                    fill_cols = [c for c in ["Exercise", "Exercise_Name"] if c in df.columns]
                    if fill_cols:
                        df = df.with_columns([
                            pl.col(c).cast(pl.Utf8).replace(["", "None", "nan"], None).forward_fill() 
                            for c in fill_cols
                        ]).cast(pl.String)

                    # Standardize Headers
                    column_mapping = {
                        "Exercise": "exercise_order",
                        "Exercise_Name": "exercise_name",
                        "Sets": "sets",
                        "Reps": "reps",
                        "RPE": "rpe_target",
                        "RPE thực cảm nhận": "rpe_actual",
                        "%RM": "pct_rm",
                        "Load": "load_raw",
                        "Tempo": "tempo",
                        "Method": "method",
                        "Rest": "rest_time"
                    }
                    
                    # Rename existing columns
                    existing_renames = {k: v for k, v in column_mapping.items() if k in df.columns}
                    df = df.rename(existing_renames)
                    
                    # Handle Note variations
                    note_coach_col = next((col for col in df.columns if "Note (Coach)" in col or col == "Note"), None)
                    note_client_col = next((col for col in df.columns if "Note (Client)" in col), None)
                    
                    if note_coach_col:
                        df = df.with_columns(pl.col(note_coach_col).cast(pl.String).alias("notes_coach"))
                    if note_client_col:
                        df = df.with_columns(pl.col(note_client_col).cast(pl.String).alias("notes_client"))
                    
                    # Add Enhanced Lineage
                    df = df.with_columns([
                        pl.lit(str(file_name)).alias("source_file"),
                        pl.lit(str(parent_folder)).alias("source_folder"),
                        pl.lit(full_path).alias("source_full_path")
                    ])
                    
                    # Standardize schema in one pass for better performance
                    missing_cols = [pl.lit(None).cast(pl.Utf8).alias(c) for c in EXPECTED_COLUMNS if c not in df.columns]
                    if missing_cols:
                        df = df.with_columns(missing_cols)
                            
                    all_extracted_tables.append(df.select(EXPECTED_COLUMNS))

            except Exception as e:
                # Log failures to console (or redirect to a DLQ later)
                print(f"Failed to parse {full_path}: {e}")
                
        # G. YIELD TO SPARK
        if all_extracted_tables:
            # mapInArrow strictly expects an iterator of RecordBatches
            table = pl.concat(all_extracted_tables).to_arrow()
            yield from table.to_batches()


# ==========================================
# 3. SPARK EXECUTION 
# ==========================================
def main():
    print("Checking infrastructure container connectivity...")
    
    # Health check 1: REST Catalog
    try:
        urllib.request.urlopen("http://rest-catalog:8181/v1/config", timeout=5)
        print(" -> REST Catalog container detected successfully.")
    except Exception as e:
        print("\n[CRITICAL ERROR] Cannot connect to Iceberg REST Catalog container!")
        print("Ensure your Docker containers are running (`docker-compose up`).")
        print(f"Error details: {e}")
        sys.exit(1)

    # Health check 2: MinIO Storage API
    try:
        urllib.request.urlopen("http://minio:9000/minio/health/ready", timeout=3)
        print(" -> MinIO container detected successfully.")
    except Exception as e:
        print("\n[CRITICAL ERROR] Cannot connect to MinIO storage APIcontainer!")
        print("Ensure MinIO is running and port 9000 is mapped to localhost.")
        print(f"Error details: {e}")
        sys.exit(1)
        
    print("Initializing Spark & Iceberg Connection...")

    # 1. CORE DEPENDENCIES MAPPING
    # Pull storage credentials dynamically from your .env context
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

    # Spark builder is now agnostic: package versions are managed by your container environment
    spark = SparkSession.builder \
        .appName("Powerlifting_Bronze_Ingestion") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Define paths
    LANDING_ZONE_PATH = "s3a://staging/training_logs/batch_date=*/*.csv"
    TARGET_ICEBERG_TABLE = "my_catalog.bronze.workout_logs"
    
    print(f"Reading messy CSVs from {LANDING_ZONE_PATH}...")
    
    print("Ensuring target Iceberg bronze schema and table structures exist...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_ICEBERG_TABLE} (
            client_name STRING, client_age STRING, week_num STRING, 
            week_start_date STRING, week_end_date STRING, day_of_week STRING, 
            exercise_name STRING, sets STRING, reps STRING, rpe_target STRING, 
            rpe_actual STRING, pct_rm STRING, load_raw STRING, tempo STRING, 
            method STRING, rest_time STRING, notes_coach STRING, notes_client STRING, 
            notes_sleep STRING, notes_meal STRING, source_file STRING, 
            source_folder STRING, source_full_path STRING
        ) USING iceberg
        TBLPROPERTIES ('write.format.default'='parquet')
    """)
    
    print(f"Reading messy CSVs from {LANDING_ZONE_PATH}...")
    
    # 1. Read files as pure binary blobs
    raw_binary_df = spark.read.format("binaryFile").load(LANDING_ZONE_PATH)
        
    # CRITICAL VALIDATION: Check if any files were found to process.
    if raw_binary_df.isEmpty():
        print("\n[INFO] No new files found to process in the landing zone path:")
        print(f" -> {LANDING_ZONE_PATH}")
        print("\nThis is normal if no new data has been ingested via the Google Drive script.")
        print("To process data, please ensure you have first run: `python src/ingestion/fetch_gdrive.py`")
        print("Aborting Spark job as there is no work to do.")
        spark.stop()
        sys.exit(0) # Exit with success code 0, as this is an expected state.

    # 2. Distribute the Python Parser across the cluster
    parsed_bronze_df = raw_binary_df.mapInArrow(
        parse_vn_powerlifting_csv, 
        schema=bronze_schema
    )
    
    # 3. Write securely to Iceberg
    print(f"Writing structured data to {TARGET_ICEBERG_TABLE}...")
    parsed_bronze_df.writeTo(TARGET_ICEBERG_TABLE).append()
        
    print("Pipeline Execution Complete!")

if __name__ == "__main__":
    main()