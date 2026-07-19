# London Bike & Weather Data Pipeline

An automated data engineering pipeline that ingests real-time bike station data from TfL and weather data from OpenWeatherMap, processes it using Apache Spark, and loads it into a Google Cloud BigQuery data warehouse for analysis.

## Project Architecture

The architecture diagram below illustrates how components interact, highlighting the separation of orchestration (Airflow), compute (Spark), and storage (GCP).

**Important Architecture Note:** This pipeline is optimized to run locally on Docker container. The configurations below ensure that Airflow and Spark operate efficiently on this specific chip architecture.

### London Bike & Weather Pipeline Architecture
<img width="2816" height="1536" alt="London Bike Architecture" src="https://github.com/user-attachments/assets/840d7d4a-297f-4a97-a616-4c7234fc18a2" />



### Key Components

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.10+ | DAG scheduling, task management, and failure monitoring. |
| **Compute** | Apache Spark 3.5.3 (PySpark) | Data extraction from APIs, transformation (Parquet conversion), and BigQuery loading. |
| **Data Lake** | Google Cloud Storage (GCS) | Staging area: Raw data (Bronze) and cleaned Parquet data (Silver). |
| **Data Warehouse** | Google BigQuery | Final Analytical storage (Gold) for dashboards and reporting. |
| **Environment** | Docker / Docker Compose | Containerized execution of Airflow (Webserver, Scheduler, worker) and a healthy PostgreSQL metadata DB. |

---

## Local Setup Instructions

These steps ensure a stable environment on an Intel-based machine, resolving common Docker memory and architecture bottlenecks.

### Prerequisites

1.  **Docker Desktop** (Installed on Windows).
2.  **Git** (Verified via terminal).
3.  **Google Cloud Project**: You must have a service account with **BigQuery Admin** and **Storage Admin** roles and have the JSON keyfile available.
4.  **APIs**: Weather API from https://openweathermap.org/api and Bike data API from https://tfl.gov.uk/info-for/open-data-users/api-documentation

### Installation & Initialization

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/kokojnr/london-bike-weather
    cd london-bike-weather
    ```

2.  **Configure Environment Details:**
    *   Confirm your `docker-compose.yaml` has the correct settings:

3.  **Reset and Rebuild (Scorched Earth):**
    If you encountered previous setup issues, run this sequence to perform a clean image build from scratch:
    ```bash
    # 1. Wipe previous containers, networks, and volumes
    docker compose down -v --remove-orphans

    # 2. Rebuild the custom Airflow/Spark image without cache
    docker compose build --no-cache

    # 3. Initialize the database and create the admin user
    docker compose up airflow-init
    # (Wait until you see "exited with code 0")
    ```

4.  **Start the Full Pipeline:**
    Spin up all containers in the background:
    ```bash
    docker compose up -d
    ```

5.  **Configure Google Cloud Connection:**
    *   Open Airflow: `http://localhost:8080` (Log in with `airflow`/`airflow`).
    *   Go to **Admin** -> **Connections**.
    *   Edit/Create connection Id: `google_cloud_default`.
    *   Connection Type: `Google Cloud`.
    *   Paste your service account **JSON** keyfile content into the `Keyfile JSON` box. *Do not hardcode paths.*
    *   Save the connection.

---
