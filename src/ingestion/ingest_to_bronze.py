import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 1. Path Configurations
root_dir = Path(__file__).resolve().parent.parent.parent
staging_path = str(root_dir / "data" / "staging" / "training_logs" / "*.csv")
bronze_output = str(root_dir / "data" / "bronze" / "training_logs")

# 2. Initialize Spark
spark = SparkSession.builder \
    .appName("PowerliftingBronzeDynamicIngestion") \
    .getOrCreate()

print(f"Archiving raw logs with DYNAMIC column detection...")

# 3. Read raw CSVs
df_raw = spark.read.option("header", "false").csv(staging_path) \
    .withColumn("raw_filepath", F.input_file_name())

# 4. IDENTIFY ALL DATA COLUMNS DYNAMICALLY
# Spark names columns _c0, _c1, _c2... by default. 
# We find every column that starts with '_c' so we don't miss anything.
data_cols = [c for c in df_raw.columns if c.startswith("_c")]

# 5. BUILD THE SELECTION
# We create our metadata columns first...
metadata_selection = [
    F.current_timestamp().alias("ingestion_timestamp"),
    F.element_at(F.split(F.element_at(F.split("raw_filepath", "/"), -1), "___"), 1).alias("meta_folder"),
    F.element_at(F.split(F.element_at(F.split("raw_filepath", "/"), -1), "___"), 2).alias("meta_filename"),
    F.regexp_replace(F.element_at(F.split(F.element_at(F.split("raw_filepath", "/"), -1), "___"), 3), ".csv", "").alias("meta_tab_name")
]

# ...then we dynamically add every data column found, casting them to String
# This turns [_c0, _c1, _c2...] into [raw_col0, raw_col1, raw_col2...]
dynamic_data_selection = [F.col(c).cast(StringType()).alias(c.replace("_c", "raw_col")) for c in data_cols]

# Combine both lists
final_selection = metadata_selection + dynamic_data_selection

# Apply the selection
df_standardized = df_raw.select(*final_selection)

# 6. Push to Bronze (Parquet)
df_standardized.write.mode("overwrite").parquet(bronze_output)

print(f"Success! Captured {len(data_cols)} columns dynamically.")
print(f"Archived in Bronze at: {bronze_output}")

# Verify the count of columns captured
df_standardized.printSchema()