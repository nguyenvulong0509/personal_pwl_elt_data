# personal_pwl_elt_data
a project where I extract and structure my pwl training data for storage and analysis purposes
practicing with some DE's tech stacks here

## Getting Started

Follow these instructions to set up and run the data pipeline environment on your local machine.

### Prerequisites

*   **Docker & Docker Compose**: Ensure you have Docker and Docker Compose installed. This project uses them to manage all services in a containerized environment.
*   **Git**: To clone the repository.
*   **Google Cloud Account**: Required for fetching data from Google Drive and Google Sheets.

### 1. Clone the Repository

First, clone this repository to your local machine.

```bash
git clone <your-repository-url>
cd personal_pwl_elt_data
```

### 2. Configure Environment Variables

The project uses a `.env` file to manage all configuration, such as credentials, ports, and API keys. An example file named `env.example` is provided in the root of the project.

Copy this file to `.env` and replace the placeholder values with your actual configuration details.

### Docker Environment Overview

This project uses Docker Compose to orchestrate a multi-container environment, creating a self-contained data platform on your local machine. Each service has a specific role:

<details>
<summary><strong>Click to expand service details</strong></summary>

*   **`postgres`**
    *   **Purpose**: This is the central metadata database for the entire platform. It serves three critical functions:
        1.  **Airflow Backend**: Stores DAGs, task instances, and other Airflow metadata.
        2.  **Iceberg REST Catalog**: Manages pointers to the data files and schema information for your Iceberg tables.
        3.  **State Tracker**: Holds the `file_state_tracker` table, which prevents the re-processing of unchanged source files, making the ingestion pipelines idempotent.
    *   **Configuration**: Managed by the `POSTGRES_*` variables in the `.env` file.

*   **`minio`**
    *   **Purpose**: An S3-compatible object storage system that acts as the project's data lake. Raw files extracted from Google Drive and OpenPowerlifting are loaded into a bucket here (the "staging" or "landing" zone) before being transformed.
    *   **Configuration**: The `MINIO_*` variables control the access credentials, ports, and the initial bucket name.

*   **`airflow` (Webserver, Scheduler, etc.)**
    *   **Purpose**: The pipeline orchestrator. Airflow is responsible for scheduling and running the data ingestion and transformation tasks in the correct sequence. The Airflow UI allows you to monitor and manage these pipelines.
    *   **Configuration**: The `AIRFLOW_*` variables set up the webserver port and the default UI login. It connects to the `postgres` container for its backend.

*   **`spark` (Master and Worker)**
    *   **Purpose**: The distributed data processing engine. The Spark job (`training_bronze_ingestion.py`) is responsible for reading the raw, messy CSV files from MinIO, parsing them into a structured format, and writing them into the final Iceberg table.
    *   **Configuration**: The `SPARK_*` variables control the UI and Thrift server ports. The Spark job itself is configured to connect to MinIO and the Iceberg catalog.

*   **`rest-catalog`**
    *   **Purpose**: The Apache Iceberg REST Catalog service. It provides a standardized API for Spark to interact with the Iceberg table metadata stored in PostgreSQL. This decouples the data processing from the metadata storage.
    *   **Configuration**: This service is configured in the `docker-compose.yml` file to use the `postgres` service as its backend.

</details>

**Important**:
*   Place your Google Cloud service account JSON key file (e.g., `your-service-account-key.json`) in the root of the project directory.
*   Share the Google Drive folder you specified in `drive_folder_id` with the service account's email address.

### 3. Build and Run the Services

Once your `.env` file is configured, you can start all the services using Docker Compose.

```bash
# Build and start all services in detached mode
docker-compose -f docker/docker-compose.yml up --build -d
```

### 4. Initialize Databases and Buckets

After the containers are running, you need to run a few one-time initialization scripts.

```bash
# Initialize the PostgreSQL table for state tracking
python src/utils/state_tracker.py

# Initialize the buckets in MinIO
python src/transformation/bronze/init_minio.py
```

### 5. Accessing Services

You can now access the various user interfaces for each service:

*   **Airflow UI**: `http://localhost:8080`
*   **MinIO Console**: `http://localhost:9001`
*   **Spark Master UI**: `http://localhost:8081`
