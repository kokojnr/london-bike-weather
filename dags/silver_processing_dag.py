import json
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

GCS_BUCKET_NAME = "london_bike_data_bronze"

def setup_gcp_credentials(**kwargs):
    """
    PySpark cannot read Airflow's internal UI connections database.
    It requires a physical JSON key file to authenticate with GCP.
    This task securely extracts the key from Airflow and writes it to a temporary file for Spark to use.
    """
    print("Extracting GCP Service Account key from Airflow Connections...")
    conn = BaseHook.get_connection('google_cloud_default')
    extra = json.loads(conn.extra)
    
    # Depending on how it was pasted, it might be nested or a string
    keyfile_dict = extra.get('keyfile_dict', extra)
    if isinstance(keyfile_dict, str):
        keyfile_dict = json.loads(keyfile_dict)
        
    key_path = "/tmp/gcp_key.json"
    with open(key_path, "w") as f:
        json.dump(keyfile_dict, f)
        
    print(f"Credentials saved temporarily to {key_path}")

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    'process_silver_layer',
    default_args=default_args,
    description='Run PySpark job to process Bronze data into Silver Parquet files',
    schedule_interval=None, # For now, we will trigger this manually
    catchup=False,
    tags=['silver', 'processing', 'spark'],
) as dag:

    # Task 1: Prepare the JSON Key
    prepare_key_task = PythonOperator(
        task_id='prepare_gcp_key',
        python_callable=setup_gcp_credentials
    )

    # We use GOOGLE_APPLICATION_CREDENTIALS to tell the Spark GCS Connector where to find the key we just made
    run_spark_task = BashOperator(
        task_id='run_silver_spark_job',
        bash_command=(
            "export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp_key.json && "
            f"python /opt/airflow/jobs/silver_layer_spark_job.py {GCS_BUCKET_NAME}"
        )
    )

    # Set dependencies (Extract Key -> Run Spark)
prepare_key_task >> run_spark_task