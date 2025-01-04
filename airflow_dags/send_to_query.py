import json
import pandas as pd
from google.cloud import storage, bigquery
import logging
import datetime
# Configuration variables
BUCKET_NAME = 'edit-data-eng-project-group3'  # Replace with your bucket name
SOURCE_JSON_FILE = 'municipalities1.json'  # Path to the JSON file in the bucket
BIGQUERY_PROJECT = 'data-eng-dev-437916'  # Replace with your project ID
BIGQUERY_DATASET = 'data_eng_project_group3'  # Replace with your BigQuery dataset name
BIGQUERY_TABLE = 'municipalities_test'  # Replace with your BigQuery table name

def convert_json_to_dataframe(json_content):
    """Converts JSON content to a CSV DataFrame."""
    # Assuming the JSON is an array of records (list of dictionaries)
    data = json.loads(json_content)
    df = pd.DataFrame(data)
    logging.info("Processed to df.")
    return df

def read_json_from_gcs(bucket_name: str, source_file_name: str) -> pd.DataFrame:
    """Reads a JSON file from GCS, converts it to CSV, and uploads to BigQuery."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    json_content = blob.download_as_text()
    # Convert JSON content to DataFrame
    df = convert_json_to_dataframe(json_content)
    logging.info("Read json from bucket and converted to csv.")
    with_date = df.assign(ingested_at = datetime.datetime.now())
    return with_date


def load_to_bigquery(dataframe, project, dataset, table):

    # Upload dataframe 
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Automatically infers schema
    )

    load_job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Loaded data into {table_id}.")
    return 'success'


if __name__ == "__main__":
    #load_json_from_gcs_and_upload_to_bigquery(BUCKET_NAME, SOURCE_JSON_FILE, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
    # with open('municipalities.json', 'r') as source:
    dataframe = read_json_from_gcs(BUCKET_NAME, SOURCE_JSON_FILE)
    load_to_bigquery(dataframe, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)

    #     json_data = source.read()
    # df = convert_json_to_csv_in_memory(json_data)
    print("Process completed successfully.")
