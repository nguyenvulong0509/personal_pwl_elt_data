# Gemini Code Assist - Project Conventions

This document outlines the established architecture, technology stack, and coding conventions for the `personal_pwl_elt_data` project. It serves as a guide for Gemini Code Assist to ensure that any future code generation, refactoring, or analysis aligns with the project's design principles.

## 1. Project Architecture

The project implements a modern ELT (Extract-Load-Transform) data pipeline using a Lakehouse architecture.

### 1.1. Data Flow

1.  **Extract**: Raw data is pulled from two primary sources:
    *   **Google Drive**: Personal training logs stored in Google Sheets are fetched via the Google Drive & Sheets APIs.
    *   **OpenPowerlifting**: The bulk public dataset is downloaded from its official source.
    *   **Xiaomi API**: The logged information from xiaomi band about daily body metric (sleep score, heart rate, etc) - To be updated later.
    *   **Stance fitness bar-speed tracker**: The data of SBD lift velocity - to be updated later.

2.  **Load (Staging)**: Extracted files are loaded into a **MinIO** S3-compatible object storage bucket, which acts as the staging area (Landing Zone) of the data lake. Data is partitioned by `batch_date=YYYY-MM-DD` for Google Drive/Google Sheet data. For OpenPowerlifting, data will be overwritten each time the pipeline runs. For "To be updated later" data sources, the ingestion method will be different and decided later.

3.  **State Tracking**: A **PostgreSQL** database contains a `file_state_tracker` table. This table is crucial for idempotency, preventing the reprocessing of unchanged files by tracking file hashes (`modifiedTime` for Google Drive, `Last-Modified` header for web files) and their ingestion status (`PENDING`, `SUCCESS`, `FAILED`). More types of state tracking/log auditting tables will be implemented later based on the needs of the project.


4.  **Transform**: An **Apache Spark** job reads the raw, staged files (e.g., CSVs) from MinIO, parses and cleans them, and transforms them into a structured format. This job runs in a distributed manner.

5.  **Serve (Lakehouse)**: The transformed, structured data is written into an **Apache Iceberg** table, creating the "Bronze" layer of the lakehouse. The Iceberg catalog metadata is managed by the **Iceberg REST Catalog**, which itself uses the PostgreSQL database as its backend.

### 1.2. Orchestration

**Apache Airflow** is used to orchestrate the entire pipeline, scheduling and managing the dependencies between the extraction and transformation tasks.

### 1.3. Infrastructure

The entire environment is containerized using **Docker and Docker Compose**. This provides a reproducible and isolated development environment for all services (Postgres, MinIO, Airflow, Spark, etc.).

## 2. Technology Stack

When generating code, prioritize using the following technologies and libraries:

*   **Containerization**: Docker, Docker Compose
*   **Orchestration**: Apache Airflow
*   **Data Processing**:
    *   **Apache Spark**: For distributed transformations. Use PySpark for development.
    *   **Polars**: For high-performance, single-node parsing and manipulation, especially within Spark UDFs (`mapInArrow`) for efficiency gains.
*   **Storage**:
    *   **MinIO**: As the S3-compatible object store for the data lake.
    *   **PostgreSQL**: For metadata (Airflow backend, Iceberg catalog, custom state tracking).
*   **Data Lake Format**: **Apache Iceberg**. All Spark jobs should read from and write to Iceberg tables via the REST catalog.
*   **Primary Language**: **Python 3**.
*   **Key Python Libraries**:
    *   `pyspark`: For Spark jobs.
    *   `polars`: For data manipulation.
    *   `boto3`: For interacting with MinIO.
    *   `psycopg2-binary`: For connecting to PostgreSQL.
    *   `google-api-python-client`, `google-auth-oauthlib`: For Google Drive/Sheets API access.
    *   `requests`: For HTTP requests.
    *   `python-dotenv`: For managing environment variables.
    *   `tenacity`: For implementing robust retry logic on API calls.

## 3. Coding Conventions & Patterns

Adhere to the following conventions when writing or modifying code.

### 3.1. Configuration

*   **Use Environment Variables**: All configuration (credentials, ports, hostnames, file IDs) **must** be managed via environment variables. Load them from a `.env` file using `python-dotenv`.
*   **Centralized Paths**: Use `pathlib` to define root directories and construct paths to avoid OS-specific issues.

### 3.2. Idempotency and State Management

*   All ingestion scripts **must** interact with the `src/utils/state_tracker.py` utility.
*   Before processing a file, call `should_process_file(file_id, current_hash)` to check if it's new or has been updated.
*   Use `log_file_state()` to record the status (`PENDING`, `SUCCESS`, `FAILED`) of the operation. This is critical for observability and debugging.

### 3.3. Error Handling

*   Wrap critical operations (API calls, file I/O, database transactions) in `try...except` blocks.
*   When an error occurs during ingestion, log the state as `FAILED` in the state tracker, including the error message in the `error_log` column.
*   For API interactions prone to rate limiting (like Google Drive), use the **`tenacity`** library to implement an exponential backoff with jitter retry strategy, specifically targeting transient errors like HTTP 429.

### 3.4. Spark Jobs

*   **Schema Definition**: Define a strict schema (`StructType`) for all data being ingested into the Bronze layer. This prevents schema drift and ensures data quality.
*   **Efficient Parsing**: For complex, non-standard file formats (like the multi-table CSVs), use `spark.read.format("binaryFile")` and apply a Python parsing function with `mapInArrow`. This leverages libraries like **Polars** for fast parsing on worker nodes.
*   **Iceberg Integration**: Configure the Spark session to use the Iceberg Spark extensions and the REST catalog. All writes to the lakehouse should use the `.writeTo(...).append()` or `.merge()` syntax.

### 3.5. Code Style

*   Follow **PEP 8** guidelines for Python code.
*   Use clear, descriptive names for variables and functions.
*   Add comments to explain complex logic, business rules, or non-obvious code sections.
*   Structure code into logical sections using comments (e.g., `# 1. CONFIGURATION`, `# 2. HELPER FUNCTIONS`).

### 3.6. File Structure

*   **`src/ingestion/`**: Scripts for extracting data from external sources.
*   **`src/transformation/`**: Spark jobs for data transformation, organized by data quality layer (e.g., `bronze`, `silver`).
*   **`src/utils/`**: Common utilities shared across the project (e.g., `state_tracker.py`).
*   **`docker/`**: Contains Docker-related files, primarily `docker-compose.yml`.
*   **`dags/`**: (If applicable) Airflow DAG definitions.

By following these guidelines, we can maintain a high-quality, consistent, and maintainable codebase.

---
*This document was generated by Gemini Code Assist based on the existing project structure and code.*