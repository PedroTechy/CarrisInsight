import json 
import requests
import os
  
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
        
    local_file_path = f"shapes.json"
    with open(local_file_path, 'wb') as f:
        f.write(data)

    x = 1

endpoint = "shapes"
x = extract_and_store_data_test(endpoint)
