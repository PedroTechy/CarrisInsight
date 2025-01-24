"""
Carris Data Pipeline DAG
=======================

An Airflow DAG that extracts data from the Carris API, stores it in Google Cloud Storage (GCS),
and loads it into BigQuery as tables. This pipeline handles both JSON and ZIP file formats,
performing the minimum necessary transformations during the process. Also copies the historical_stop_times
from teachers BigQuery.

=======================
"""
import io
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import List, Optional
from zipfile import ZipFile
import numpy as np
import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
 

############################################ Configurations ############################################

BASE_API_URL = "https://api.carrismetropolitana.pt/"
ZIP_FILES = ['stop_times.txt','calendar_dates.txt', 'trips.txt', "dates.txt", "shapes.txt", "periods.txt"]
ENDPOINTS = ["municipalities", "stops", "lines", "routes"]
BUCKET_NAME= "edit-data-eng-project-group3"
BIGQUERY_PROJECT = 'data-eng-dev-437916'  
BIGQUERY_DATASET = 'data_eng_project_group3'   
MAX_TABLE_CREATED_HOURS= 23 #used to drop large tables before starting imports
MAX_TABLE_COMPARE_SIZE=300000
MAX_FILE_CHUNK_SIZE=150000000
############################################ Configurations ############################################

logging.basicConfig(level=logging.INFO,  # Set the default logging level
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
                    handlers=[logging.StreamHandler()])  # Output to stdout (default)

# Utils functions 
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
                        input_type: str,
                        dtype:str =None) -> pd.DataFrame:
    """
    Converts JSON content to a clean DataFrame. Assumes JSON as
    an array of records (list of dictionaries)

    Args:
        content : The file content.
        input_type (str): The file type ('json' or 'csv').
        dtype (str) None: Can be used to bind specific columns to datatypes in pandas. Default is none. 
    Returns:
        pd.DataFrame: A processed pandas DataFrame.
    """   
    if input_type=='json':
        logging.info("Handling as json file")
        data = json.loads(content)
        df = pd.DataFrame(data,dtype=dtype)
    elif input_type == 'csv':
        # Read the CSV data into a pandas DataFrame

        logging.info("Handling as csv file")
        csv_buffer = StringIO(content)
        df = pd.read_csv(csv_buffer, dtype=dtype)
    else:
        logging.error("Make sure you are passing a .json or .csv file")

    clean_df = df.apply(clean_none_list, axis=0) 
    logging.info("Processed to df.")
    
    return clean_df

def find_file_replicas(bucket_name, filename, filepath):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_list = [blob for blob in bucket.list_blobs(prefix=filepath) if filename in blob.name]
    logging.info(f"Found blobs: {blob_list}")
    return blob_list

def read_files_from_gcs(blob,
                        file_type: str,
                        dtype:str=None) -> pd.DataFrame:
    """
    Reads a file from GCS and converts it to a pandas DataFrame.

    Args:
        bucket_name (str): The name of the GCS bucket.
        source_file_name (str): The path and filename of the file in GCS.

    Returns:
        pd.DataFrame: The DataFrame representation of the file content.
    """

    file_content = blob.download_as_text()

    # Convert file content to DataFrame
    df = convert_to_dataframe(file_content , file_type, dtype=dtype)
    logging.info("Read file from bucket and converted to DataFrame.")
    return df

def get_diff(client,
             bucket_df: pd.DataFrame,
             table_id: str) -> pd.DataFrame:
    """
    Compares a DataFrame with an existing BigQuery table and returns the new or modified rows.

    Args:
        client: The BigQuery client to interact with the database.
        bucket_df (pd.DataFrame): The DataFrame containing new or modified data to compare.
        table_id (str): The ID of the BigQuery table to compare against.

    Returns:
        pd.DataFrame: A DataFrame containing new or modified rows from `bucket_df` that are not in the BigQuery table.

    Notes:
        - If the BigQuery table is too large (over 300,000 rows) and older than 23hours, the function will drop the existing table and load new data.
        - If the BigQuery table is too large (over 300,000 rows) and younger than 23hours, the function will append all new data.
        - If the table does not exist yet, all data in `bucket_df` will be returned.
        - Rows are compared based on exact matches of column values; unhashable types are handled before comparison.
    """
    try:
        iter_row = client.query(f"select count(*) from {table_id}").result()
        table_lines = [i for i in iter_row][0][0]

        assert table_lines < MAX_TABLE_COMPARE_SIZE, "Table too large to compare. Will drop existing and create a new one."

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
    except AssertionError:
        logging.info("Checking table created age")
        table = client.get_table(table_id)
        now_minus_23_hours = (datetime.now(tz=timezone.utc) - timedelta(hours=MAX_TABLE_CREATED_HOURS))
        if table.created < now_minus_23_hours:
            # Only added this logic to prevent creating a "forever appending table"
            logging.warning(f"Found table older that 23 hours: {table.created} Dropping {table_id} to load new data")
            iter_row = client.query(f"DROP TABLE {table_id}").result()
        else:
            logging.info('Appending to existing query without checking for duplicates.')
        return bucket_df

def load_dataframe_to_bigquery(dataframe: pd.DataFrame,
                               project: str,
                               dataset: str,
                               table: str) -> bool:
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
def extract_and_store_weather_data(base_url: str,
                                endpoint: str, 
                                **context) -> None:
    """
    Fetches data from the API endpoint and uploads it to Google Cloud Storage.

    Args:
        base_url (str): The base URL for the API.
        endpoint (str): The endpoint path to fetch data from.

    Returns:
        None
    """
    try:
        url = f"{base_url}{endpoint}"
        response = requests.get(url) 
        
        if response.status_code == 200: 
            file_content=response.content
            logging.info(f"Fetched data from {endpoint}")
        else:
            raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        
        # we dont need the country and owner, so geting ride of that before uploading to bq 
        # Decode and parse bytes content into a dictionary
        content_dict = json.loads(file_content.decode('utf-8'))

        # Convert the 'data' field back to a string to be able to use the blob_from_memory func (maybe fin func that upload as json)
        file_content = json.dumps(content_dict.get('data', {}))

        execution_date = context['execution_date'].strftime('%Y-%m-%d')

        # here we sub the endpoint (that was a numeric ID representing Lisbon) to the name that better represents the data
        endpoint = "weather_lisbon_last_fivedays"
        
        target_file_path = f"raw_data/{execution_date}/{endpoint}.json"
        upload_blob_from_memory(BUCKET_NAME,file_content,target_file_path)
    except:
        # added a generic exception to catch any error for a specific file without stopping the loop.
        logging.error(f"Error when trying to handle {target_file_path}. File has been skipped")
          
def extract_and_store_zip_files(base_url: str,
                                zip_endpoint: str = "gtfs",
                                file_list: list = [],
                                **context):
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

    with ZipFile(io.BytesIO(response.content)) as zip_ref:
        # List the files in the zip
        zip_file_list = zip_ref.namelist()
        logging.info(f"Files in the zip archive: {zip_file_list}")

        for target_filename in file_list:
            try:
                if target_filename in zip_file_list:
                    with zip_ref.open(target_filename) as target_file:
                        logging.info(f"Reading {target_filename} in chunks...")

                        # Initialize the chunk reader (using BufferedReader for better performance)
                        buffer = io.BufferedReader(target_file)
            
                        # Process file in chunks
                        total_bytes_read = 0
                        # Saving the header so it can be prepended to all chunks creating valid csv
                        header = buffer.readlines(1)[0].decode('utf-8')
                        iter_count = 0
                        while True: # Get a better apporach here than while true!!
                            
                            chunk = buffer.readlines(MAX_FILE_CHUNK_SIZE)  # Consume chunk of data
                            if not chunk:
                                break  # EOF reached
                            
                            total_bytes_read += len(chunk)
                            
                            logging.info(f"Size of {iter_count} chunk {sys.getsizeof(chunk)}:\n")
                            string_content= list(map(lambda x: x.decode('utf-8'),chunk))
                            file_content = header+ "".join(string_content)
                            #bucket_filename= f"{target_filename.split(".")[0]}{iter_count if iter_count != 0 else ''}.csv"
                            bucket_filepath=f"raw_data/{context['execution_date'].strftime('%Y-%m-%d')}/{bucket_filename}" 
                            upload_blob_from_memory(BUCKET_NAME, file_content, bucket_filepath)
                            logging.info(f"Saved {bucket_filename} with {len(chunk)} bytes.")
                            iter_count+=1
                            
                        logging.info(f"Total bytes read from {target_filename}: {total_bytes_read}")

                else:
                    raise Exception(f"{target_filename} not found in the zip archive.")
            except:
                # Added a generic exception to catch any error for a specific file without stopping the loop.
                logging.error(f"Error when trying to handle {target_filename}. File has been skipped")
                continue

def get_table_dtypes(table_name: str) -> dict:
    """
    Returns the specific data types for columns in a BigQuery table based on the table name.

    Args:
        table_name (str): The name of the table to get data types for.

    Returns:
        dict or None: A dictionary with column names as keys and data types as values,
                      or None if no specific types are needed for the table.
    """

    if "stop_times" in table_name:
        return {'shape_dist_traveled': 'float64'}
    # Add more table-specific logic here as needed
    else:
        return None

def load_tables_from_bucket_to_bigquery(bucket_name: str,
                                        project_id: str,
                                        dataset_id: str,
                                        filename: str,
                                        extension_type,
                                        **context) -> bool:
    """
    Loads a file from a GCS bucket into a BigQuery table.

    Args:
        bucket_name (str): GCS bucket name.
        project_id (str): GCP project ID.
        dataset_id (str): BigQuery dataset ID.
        filename (str): Name of the file to load.
        extension_type (str): File extension (e.g., 'csv', 'json').
        context (dict): Airflow context containing metadata like execution date.

    Returns:
        bool: True if data is loaded, False if an error occurs.

    Notes:
        - Dynamically generates the file path based on the `execution_date`.
        - Handles specific files (e.g., 'stop_times') with custom data types.
    """
        
    execution_date =  '2025-01-17' 
    file_path = f"raw_data/{execution_date}/"
    
    logging.info(f"Processing file: {filename}")

    matching_blobs = find_file_replicas(bucket_name, filename, file_path)

    for blob in matching_blobs:

        dtype = get_table_dtypes(blob.name)
        dataframe = read_files_from_gcs(blob, extension_type, dtype=dtype)
        if dataframe is None:
            logging.info(f"Couldn't load {blob.name} on bucket{bucket_name}, please be sure the file exists. Skipping it.")
            return "Failure"

        # define bigquery table name    
        table_name = f"raw_{filename}"
    
        # Load to bigQuery
        success = load_dataframe_to_bigquery(
            dataframe=dataframe,
            project=project_id,
            dataset=dataset_id,
            table=table_name
        ) 
    return "success"

def recreate_historical_stop_times_table_from_teachers(project_id: str,
                                                       dataset_id: str,
                                                       is_weekly: bool = True,
                                                       **context: dict) -> bool:
    """
    Recreates a table from teachers project (historical_stop_times) with option for weekly execution.
    
    Args:
        project_id: Destination project ID
        dataset_id: Destination dataset ID
        is_weekly: If True, only runs on Mondays
        context: Airflow context
    """
    # check if it's a weekly task and if it's not Wednesday (could be any other day, the idea is that we don't need to run this every day)
    if is_weekly and context['execution_date'].weekday() != 2:
        logging.info("Not Wednesday, skipping weekly task of loading historical stop times")
        return False

    try:
        client = bigquery.Client(project=project_id)
        
        source_table = f"{project_id}.de_project_teachers.historical_stop_times"
        destination_table = f"{project_id}.{dataset_id}.raw_historical_stop_times"

        # Delete if exists
        try:
            client.delete_table(destination_table)
            logging.info(f"Existing table {destination_table} deleted")
        except Exception:
            logging.info(f"Table {destination_table} does not exist")

        # Create and copy data
        source_table_ref = client.get_table(source_table)
        new_table = bigquery.Table(destination_table, schema=source_table_ref.schema)
        client.create_table(new_table)

        query = f"INSERT INTO {destination_table} SELECT * FROM {source_table}"
        query_job = client.query(query)
        query_job.result()

        logging.info (f"Successfully recreated table {destination_table}") 
        return True

    except Exception as e:
        error_msg = f"Failed to recreate table historical_stop_times: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return False

 
base_url = r"https://api.ipma.pt/public/opendata/weatherforecast/daily/"
endpoint = r"1110600.json"
extract_and_store_json_data(base_url, endpoint)

load_tables_from_bucket_to_bigquery(BUCKET_NAME, BIGQUERY_PROJECT, BIGQUERY_DATASET, "weather_last_fivedays", "json")