import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable


GCS_BUCKET_NAME = "london_bike_data_bronze"

def fetch_bike_data(ts_nodash, **kwargs):
    url = "https://api.tfl.gov.uk/BikePoint/"
    response = requests.get(url)
    response.raise_for_status()
    
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    path = f"bronze/bike/{ts_nodash[:8]}/data_{ts_nodash}.json"
    
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=path,
        data=json.dumps(response.json()),
        mime_type='application/json',
        timeout=120
    )

def fetch_weather_data(ts_nodash, **kwargs):
    api_key = Variable.get("openweather_api_key")
    url = f"https://api.openweathermap.org/data/2.5/weather?q=London&appid={api_key}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    path = f"bronze/weather/{ts_nodash[:8]}/data_{ts_nodash}.json"
    
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_NAME,
        object_name=path,
        data=json.dumps(response.json()),
        mime_type='application/json'
    )

with DAG(
    'ingest_bronze_layer',
    start_date=datetime(2026, 7, 19),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    PythonOperator(task_id='bike_ingest', python_callable=fetch_bike_data)
    PythonOperator(task_id='weather_ingest', python_callable=fetch_weather_data)
