"""
Carris Data Pipeline DAG
=======================

An Airflow DAG that extracts data from the Carris API, stores it in Google Cloud Storage (GCS),
and loads it into BigQuery as tables. This pipeline handles both JSON and ZIP file formats,
performing the minimum necessary transformations during the process.

Tasks
-----
1. extract_and_store_json_data
    - Extracts data from specified API endpoints
    - Transforms response into JSON format
    - Uploads resulting files to GCS with .json extension

2. extract_and_upload_zip_task
    - Downloads and processes ZIP files from API
    - Extracts relevant .txt files from the ZIP archives
    - Converts data to CSV format
    - Uploads processed files to GCS with .csv extension

3. load_to_bigquery_task
    - Creates or updates BigQuery tables based on GCS content
    - Performs incremental loading of new data
    - Preprocesses data for consistency (avoid errors when creating tables)

"""
import io
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from io import StringIO
from typing import List, Optional
from zipfile import ZipFile
import numpy as np
import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

############################################ Configurations ############################################

BASE_API_URL = "https://api.carrismetropolitana.pt/"
ZIP_FILES = ['calendar_dates.txt', 'trips.txt', 'stop_times.txt', "data.txt", "shapes.txt"] #data, shapes
ENDPOINTS = ["municipalities", "stops", "lines", "routes"]
BUCKET_NAME= "edit-data-eng-project-group3"
BIGQUERY_PROJECT = 'data-eng-dev-437916'  
BIGQUERY_DATASET = 'data_eng_project_group3'   

SOURCE_FILE = 'periods.csv'  # Path to the JSON file in the bucket
BIGQUERY_TABLE = 'periods_dates_test'  # Replace with your BigQuery table name
############################################ Configurations ############################################

logging.basicConfig(level=logging.INFO,  # Set the default logging level
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
                    handlers=[logging.StreamHandler()])  # Output to stdout (default)

# Utils functions 

# ToDo: review this func with team, not sure if there is a smarter way like going throw all the files in the bucket (however we may not want/need all)
# dont know, check with them
def get_all_file_names(endpoints: List[str], zip_files: List[str]) -> List[str]:
    """
    Generates a list of filenames by combining endpoints and zip files with appropriate extensions.
    
    Args:
        endpoints (List[str]): List of endpoint names without extensions (e.g., ['users', 'products']).
            Each endpoint will have '.json' appended.
        zip_files (List[str]): List of zip file names with '.txt' extension (e.g., ['data.txt', 'info.txt']).
            Each .txt extension will be replaced with '.csv'.
    
    Returns:
        List[str]: Combined list of filenames with proper extensions:
            - Endpoints will have '.json' extension
            - Zip files will have '.txt' replaced with '.csv'
    
    Examples:
        >>> get_all_file_names(['users', 'orders'], ['data.txt', 'logs.txt'])
        ['data.csv', 'logs.csv', 'users.json', 'orders.json']
        
    Notes:
        - Input zip_files must have '.txt' extension
        - Function will maintain the original filename without extension
    """
    # Initialize empty list for all filenames
    filename_list = []
    
    # Process zip files: replace .txt with .csv
    for zip_file in zip_files:
        if zip_file.endswith('.txt'):
            csv_filename = zip_file.replace('.txt', '.csv')
            filename_list.append(csv_filename)
        else:
            raise ValueError(f"Zip file {zip_file} must have '.txt' extension")
    
    # Process endpoints: append .json
    for endpoint in endpoints:
        filename_list.append(f"{endpoint}.json")
    
    return filename_list

def upload_blob_from_memory(bucket_name: str,
                            contents: str,
                            destination_blob_name: str) -> None:
    """
    Uploads a file directly to a Google Cloud Storage bucket from memory.

    Args:
        bucket_name (str): The name of the GCS bucket.
        contents (str): The file contents to upload as a string.
        destination_blob_name (str): The destination path and filename in the GCS bucket.

    Returns:
        None
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    logging.info(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )

def clean_none_list(col: pd.Series) -> pd.Series:
    """
    Cleans a pandas Series by removing None values from lists within each element.

    Args:
        col (pd.Series): A pandas Series where each element may be a list.

    Returns:
        pd.Series: A cleaned pandas Series with None values removed from lists.
    """
    if isinstance(col.iloc[0], list):
        return col.apply(lambda row: [item for item in row if item is not None])
    return col
    
def unhashable_types_to_text(content) -> str:
    """
    Converts unhashable types (list, dict, ndarray) to text for processing.

    Args:
        content (Any): The input content to be converted.

    Returns:
        content str: A string representation of the content.
    """
    if type(content) in (list, json):
        return str(content)
    elif type(content) == np.ndarray:
        return str(list(content))
    else:
        return content

def restore_unhashable_types(content: str):
    """
    Restores original data types from stringified content, if possible.

    Args:
        content (str): The string representation of the original content.

    Returns:
        Any: The restored content or the original input if evaluation fails.
    """
    try: 
        evaluated= eval(content)
        if type(evaluated) in ( np.ndarray, list, json):
            return evaluated
        else:
            return content
    except:
        return content

def convert_to_dataframe(content,
                        input_type: str) -> pd.DataFrame:
    """
    Converts JSON content to a clean DataFrame. Assumes JSON as
    an array of records (list of dictionaries)

    Args:
        content : The file content.
        input_type (str): The file type ('json' or 'csv').

    Returns:
        pd.DataFrame: A processed pandas DataFrame.
    """   
    if input_type=='json':
        logging.info("Handling as json file")
        data = json.loads(content)
        df = pd.DataFrame(data)
    elif input_type == 'csv':
        # Read the CSV data into a pandas DataFrame
        logging.info("Handling as csv file")
        csv_buffer = StringIO(content)
        df = pd.read_csv(csv_buffer)
    else:
        logging.error("Make sure you are passing a .json or .csv file")

    clean_df = df.apply(clean_none_list, axis=0) 
    logging.info("Processed to df.")
    
    return clean_df

def read_files_from_gcs(bucket_name: str,
                        source_file_name: str) -> pd.DataFrame:
    """
    Reads a file from GCS and converts it to a pandas DataFrame.

    Args:
        bucket_name (str): The name of the GCS bucket.
        source_file_name (str): The path and filename of the file in GCS.

    Returns:
        pd.DataFrame: The DataFrame representation of the file content.
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    file_content = blob.download_as_text()
    file_type=source_file_name.split(".")[-1:][0]
    # Convert file content to DataFrame
    df = convert_to_dataframe(file_content ,file_type)
    logging.info("Read json from bucket and converted to DataFrame.")
    return df

def get_diff(client,
             bucket_df: pd.DataFrame,
             table_id: str) -> pd.DataFrame:
    """
    Compares a DataFrame with an existing BigQuery table and returns the difference.

    Args:
        client: The BigQuery client.
        bucket_df (pd.DataFrame): The DataFrame representing new data.
        table_id (str): The BigQuery table ID.

    Returns:
        pd.DataFrame: A DataFrame containing new or modified rows.
    """
    try:
        existing_df = client.list_rows(table_id).to_dataframe() 
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
    
    except NotFound as error:
        logging.info("Table does not exist yet, proceeding with sending all data.")
        return bucket_df

def load_dataframe_to_bigquery(dataframe: pd.DataFrame,
                               project: str,
                               dataset: str,
                               table: str):
    """
    Loads a pandas DataFrame into a BigQuery table. If the table exists, only new rows are added.

    Args:
        dataframe (pd.DataFrame): The data to be uploaded.
        project (str): The GCP project ID.
        dataset (str): The BigQuery dataset name.
        table (str): The BigQuery table name.

    Returns:
        bool: True if load was successful, False otherwise
    """
    try:

        client = bigquery.Client(project=project)
        table_id = f"{project}.{dataset}.{table}"

        # Verify is a table exists and if yes add only new lines 
        upload_dataframe = get_diff(client, dataframe, table_id)

        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Automatically infers schema
        )

        # As we load the table as dataframes objects, we can use this method
        load_job = client.load_table_from_dataframe(upload_dataframe, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info(f"Successfully loaded data into {table_id} with success") 
        return True
        
    except Exception as e:
        logging.error(f"Error loading data to BigQuery table {table_id}: {str(e)}")
        return False
    
# DAG Functions 
def extract_and_store_json_data(base_url: str,
                                endpoints_list: list) -> None:
    """
    Fetches data from a list of JSON API endpoints and uploads it to Google Cloud Storage.

    Args:
        base_url (str): The base URL for the API.
        endpoints_list (list): A list of endpoint paths to fetch data from.

    Returns:
        None
    """
    for endpoint in endpoints_list:
        try:
            url = f"{base_url}{endpoint}"
            response = requests.get(url) 
            
            if response.status_code == 200: 
                file_content=response.content
                logging.info(f"Fetched data from {endpoint}")
            else:
                raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")
                
            target_filename = f"{endpoint}.json"
            upload_blob_from_memory(BUCKET_NAME,file_content,target_filename)
        except:
            # added a generic exception to catch any error for a specific file without stopping the loop.
            logging.error(f"Error when trying to handle {target_filename}. File has been skipped")
            continue

def extract_and_store_zip_files(base_url: str,
                                zip_endpoint: str = "gtfs",
                                file_list: list = []):
    """
    Fetches a ZIP file from a specified endpoint, extracts specific files, and uploads them to GCS.

    Args:
        base_url (str): The base URL for the API.
        zip_endpoint (str): The endpoint returning a ZIP file (default is 'gtfs').
        file_list (list): A list of filenames to extract and upload.

    Returns:
        None
    """ 
    url = f"{base_url}/{zip_endpoint}"
    response = requests.get(url)

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        # List the files in the zip
        zip_file_list = zip_ref.namelist()
        logging.info(f"Files in the zip archive: {zip_file_list}")

        for target_filename in file_list:
            try:
                if target_filename in zip_file_list:
                    with zip_ref.open(target_filename) as target_file:
                        # Example: Read the file and parse its content
                        file_content = target_file.read().decode('utf-8')  # Decode the bytes to string if it's a text file
                        logging.info(f"Content of {target_filename}:\n")
                        logging.info(f"Received file with len: {len(file_content)}")

                    # Sending file to the bucket
                    bucket_filename= f"{target_filename.split(".")[0]}.csv"
                    upload_blob_from_memory(BUCKET_NAME,file_content,bucket_filename)

                else:
                    raise Exception(f"{target_filename} not found in the zip archive.")
            except:
                # Added a generic exception to catch any error for a specific file without stopping the loop.
                logging.error(f"Error when trying to handle {target_filename}. File has been skipped")
                continue

def load_tables_from_bucket_to_bigquery(bucket_name: str,
                                      project_id: str,
                                      dataset_id: str,
                                      file_names: List[str]):
    """
    Loads specified files from GCS bucket to BigQuery tables.
    
    Args:
        bucket_name (str): Name of the GCS bucket
        project_id (str): GCP project ID
        dataset_id (str): BigQuery dataset ID
        file_names List(str): List of file names to process
    
    Returns:
        None
    """ 
    
    for source_file in file_names:
        logging.info(f"Processing file: {source_file}")
        
        # Read file from GCS as dataframe
        dataframe = read_files_from_gcs(bucket_name, source_file)
        if dataframe is None:
            logging.info(f"Couldn't load {source_file} on bucket{bucket_name}, please be sure the file exists. Skipping it.")
            continue
            
        # create table name from file name (remove extension) 
        table_name, ext = os.path.splitext(source_file)
        
        # Load to bigQuery
        success = load_dataframe_to_bigquery(
            df=dataframe,
            project_id=project_id,
            dataset_id=dataset_id,
            table_name=table_name
        ) 

# Define the DAG
with DAG(

    dag_id='extract_and_upload_gcs',
    start_date=datetime(2025, 1, 9),
    schedule_interval='@hourly',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)}
) as dag:

    tasks = []

    # JSON endpoints
    extract_and_upload_json_task = PythonOperator(
        task_id='extract_and_store_json_data',
        python_callable=extract_and_store_json_data,
        op_args=[BASE_API_URL, ENDPOINTS],  # pass all endpoints as a list
    )
    tasks.append(extract_and_upload_json_task)

    # Zip file extraction and storage 
    extract_and_upload_zip_task = PythonOperator(
        task_id='extract_and_store_zip_files',
        python_callable=extract_and_store_zip_files,
        op_args=[BASE_API_URL, "gtfs", ZIP_FILES],  # "gtfs" is the default endpoint that contains the zip
    )
    tasks.append(extract_and_upload_zip_task)

    # Add data from GCS to Bigquery 
    bucket_filenames = get_all_file_names(ENDPOINTS, ZIP_FILES)
    # Zip file extraction and storage 
    load_to_bigquery_task = PythonOperator(
        task_id='load_tables_from_bucket_to_bigquery',
        python_callable=load_tables_from_bucket_to_bigquery,
        op_args=[BASE_API_URL, "gtfs", ZIP_FILES],  # "gtfs" is the default endpoint that contains the zip
    ) 

    tasks.append(load_to_bigquery_task)

    # usefull to test if each task is doing what is should, but the commented line should be the final version 
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]

    #extract_and_upload_json_task >> extract_and_upload_zip_task >> load_to_bigquery_task