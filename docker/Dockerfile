# Use the official Apache Spark image as the base
FROM apache/spark:latest

# Switch to root to install dependencies
USER root

# Install the Python libraries needed for your ingestion script
# 👇 ADDED PANDAS COMPONENT STRINGS REQUIRED BY PYSPARK MAPINARROW
RUN pip install --no-cache-dir polars pyarrow pandas>=2.2.0

# 👇 CRITICAL: Stay as root or drop back to root so volumes map safely
USER root