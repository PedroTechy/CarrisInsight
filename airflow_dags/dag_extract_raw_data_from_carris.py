import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import requests
import os
 
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

BASE_API_URL = "https://api.carrismetropolitana.pt/"
ENDPOINTS = ["municipalities", "stops", "lines", "routes"] # this 4 endpoints were tested and are working properly, see if need smth else later 
GCS_BUCKET_NAME = "your-gcs-bucket-name"

def extract_and_store_data(endpoint): 

    url = f"{BASE_API_URL}{endpoint}"
    response = requests.get(url)
    
    if response.status_code == 200:
        gcs_hook = GCSHook()
        file_path = f"raw_data/{endpoint}.json"
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=file_path,
            data=response.content,
            mime_type='application/json'
        )
        print(f"Data from {endpoint} uploaded to {file_path}")
    else:
        raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")

# Define the DAG
with DAG(
    dag_id='extract_and_upload_gcs',
    start_date=datetime(2025, 1, 3),
    schedule_interval='@hourly',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)}
) as dag:
    
    tasks = []
    for endpoint in ENDPOINTS:
        task = PythonOperator(
            task_id=f'extract_{endpoint}',
            python_callable=extract_and_store_data,
            op_args=[endpoint]
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
