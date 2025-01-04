import json 
import requests
import os
import io
import zipfile
import sys
  
BASE_API_URL = "https://api.carrismetropolitana.pt/" 




def extract_and_store_data_test(endpoint): 

    url = f"{BASE_API_URL}{endpoint}"
    response = requests.get(url)
    
    if response.status_code == 200: 
        file_path = f"raw_data/{endpoint}.json"
        data=response.content
        print(f"Data from {endpoint} uploaded to {file_path}")
    else:
        raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        
    local_file_path = f"{endpoint}.json"
    with open(local_file_path, 'wb') as f:
        f.write(data)

    x = 1

def extract_zip_files(file_list):

    url = f"{BASE_API_URL}/gtfs"
    response = requests.get(url)


    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
    # Step 3: List the files in the zip
        zip_file_list = zip_ref.namelist()
        print(f"Files in the zip archive: {zip_file_list}")

        for file in file_list:
        # Step 4: Extract and parse the specific file you want
            target_filename = file # Replace with the actual file you're interested in

            if target_filename in zip_file_list:
                with zip_ref.open(target_filename) as target_file:
                    # Example: Read the file and parse its content
                    file_content = target_file.read().decode('utf-8')  # Decode the bytes to string if it's a text file
                    print(f"Content of {target_filename}:\n")
                    print(f"Received file with len: {len(file_content)}")

                with open(target_filename, 'w') as target:
                    target.writelines(file_content)
            else:
                print(f"{target_filename} not found in the zip archive.")



if __name__ == "__main__":
    if sys.argv[1]=='json':
        print('Getting Json endpoints')
        endpoint = sys.argv[2]
        x = extract_and_store_data_test(endpoint)
    elif sys.argv[1]=='zip':
        print('Getting zip files')
        extract_zip_files(['calendar_dates.txt', 'trips.txt', 'stop_times.txt'])
    else:
        print('''Please specify if you want to extract json or zip content. Examples: 
                  python3 airflow_dags/debugging_dags_without_airflow.py json municipalities
                  python3 airflow_dags/debugging_dags_without_airflow.py zip''')
