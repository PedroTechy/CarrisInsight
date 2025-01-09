import json
import pandas as pd
from google.cloud import storage, bigquery
import logging
import numpy as np
import datetime
import sys
from google.api_core.exceptions import NotFound
# Configuration variables

logging.basicConfig(level=logging.INFO,  # Set the default logging level
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
                    handlers=[logging.StreamHandler()])  # Output to stdout (default)

BUCKET_NAME = 'edit-data-eng-project-group3'  # Replace with your bucket name
SOURCE_JSON_FILE = ''  # Path to the JSON file in the bucket
BIGQUERY_PROJECT = 'data-eng-dev-437916'  # Replace with your project ID
BIGQUERY_DATASET = 'data_eng_project_group3'  # Replace with your BigQuery dataset name
BIGQUERY_TABLE = ''  # Replace with your BigQuery table name
##  Util functions
def clean_none_list(col):
    if type(col[0]) == list:
        return  col.apply( lambda row: [item for item in row if item != None])
    else: 
        return col
    
def unhashable_types_to_text(content):

    if type(content) in ( list, json):
        return str(content)
    elif type(content) == np.ndarray:
        return str(list(content))
    else:
        return content

def restore_unhashable_types(content):
    try: 
        evaluated= eval(content)
        if type(evaluated) in ( np.ndarray, list, json):
            return evaluated
        else:
            return content
    except:
        return content


#processing functions
def convert_json_to_dataframe(json_content):
    """Converts JSON content to a clean DataFrame."""
    # Assuming the JSON is an array of records (list of dictionaries)
    data = json.loads(json_content)
    df = (
        pd.DataFrame(data)
          .apply(clean_none_list, axis=0)
          )
    logging.info("Processed to df.")
    print(df.head())
    return df

def read_json_from_gcs(bucket_name: str, source_file_name: str) -> pd.DataFrame:
    """Reads a JSON file from GCS, converts it to DataFrame."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    json_content = blob.download_as_text()
    # Convert JSON content to DataFrame
    df = convert_json_to_dataframe(json_content)
    logging.info("Read json from bucket and converted to DataFrame.")
    return df


def load_to_bigquery(dataframe, project, dataset, table):

    # Upload dataframe 
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    # Verify is a table exists and if yes add only new lines 
    upload_dataframe = get_diff(client, dataframe, table_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Automatically infers schema
    )

    load_job = client.load_table_from_dataframe(upload_dataframe, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    logging.info(f"Loaded data into {table_id}.")
    return 'success'


def get_diff(client, bucket_df, table_id):

    # Read existing table from BigQuery into a DataFrame
    try:
        existing_df = client.list_rows(table_id).to_dataframe() 
    except NotFound as error:
        logging.info("Table does not exist yet, proceeding with sending all data.")


    # Find the diff by merging and getting the rows that exist only on bucket_df
    merged_df = (
        pd.merge(
            existing_df.map(unhashable_types_to_text),
            bucket_df.map(unhashable_types_to_text),
            on=bucket_df.columns.to_list(),
            how='right', # important to get values only on second df
            indicator=True
            )
            )
        
    #Restoring the un hashable values
    upload_df=(
        merged_df
        .query('_merge == "right_only"')
        .drop(columns='_merge')
        .map(restore_unhashable_types)
        )
   
    logging.info(f"Found {len(upload_df)} new or modified rows to upload.")
    return upload_df



if __name__ == "__main__":

    dataframe = read_json_from_gcs(BUCKET_NAME, SOURCE_JSON_FILE)
    load_to_bigquery(dataframe, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)

    logging.info("Process completed successfully.")
