from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Configuration
GCS_BUCKET_NAME = "london_bike_data_bronze"
GCP_PROJECT_ID = "london-bike-502619"

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'process_gold_layer',
    default_args=default_args,
    description='Load Silver data to BigQuery Gold layer',
    schedule_interval=None,
    catchup=False,
    tags=['gold', 'bigquery', 'analytics'],
) as dag:

    # Trigger the Gold processing job
    run_gold_spark_job = BashOperator(
        task_id='run_gold_spark_job',
        bash_command=(
            "export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp_key.json && "
            f"python /opt/airflow/jobs/gold_layer_spark_job.py {GCS_BUCKET_NAME} {GCP_PROJECT_ID}"
        )
    )
