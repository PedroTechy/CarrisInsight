import requests
import io
import zipfile
import sys
import json
from google.cloud import storage
import logging
import sys
  
BASE_API_URL = "https://api.carrismetropolitana.pt/" 
ZIP_FILES = ['calendar_dates.txt', 'trips.txt', 'stop_times.txt']
ENDPOINTS = ["municipalities", "stops", "lines", "routes"]
BUCKET_NAME= "edit-data-eng-project-group3"

logging.basicConfig(level=logging.INFO,  # Set the default logging level
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
                    handlers=[logging.StreamHandler()])  # Output to stdout (default)

def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    logging.info(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )


def extract_and_store_api_data(endpoints: list): 

    for endpoint in endpoints:
        try:
            url = f"{BASE_API_URL}{endpoint}"
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



def extract_and_store_zip_files(file_list):

    url = f"{BASE_API_URL}/gtfs"
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
                # added a generic exception to catch any error for a specific file without stopping the loop.
                logging.error(f"Error when trying to handle {target_filename}. File has been skipped")
                continue



if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1]=='api':
            logging.info('Getting Api endpoints')
            if sys.argv[2]:
                #gives the possibility to pass an endpoint as an argument
                logging.info(f"Api endpoint name: {sys.argv[2]}")
                provided_endpoints = [sys.argv[2]]
                extract_and_store_api_data(provided_endpoints)
            else:
                logging.error('''Please specify the api endpoint you want to extract.\nExamples: 
                    python3 airflow_dags/debugging_dags_without_airflow.py json municipalities''')


        elif sys.argv[1]=='zip':
            logging.info('Getting zip files')
            if sys.argv[2]:
                logging.info(f"Zip file name: {sys.argv[2]}")
                provided_files = [sys.argv[2]]
                extract_and_store_zip_files(provided_files)
            else:
                logging.error('''Please specify the zip file you want to extract.\n
                    Examples:python3 airflow_dags/debugging_dags_without_airflow.py zip calendar_dates.txt''')
    else:
        logging.info("Running extraction for configured files and endpoints")
        logging.info(ENDPOINTS)
        logging.info(ZIP_FILES)
        extract_and_store_api_data(ENDPOINTS)
        extract_and_store_zip_files(ZIP_FILES)


