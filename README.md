# CarrisInsight
## Data Engineering – Applied Project (Group 3)

## Project Overview
This project focuses on creating data pipelines to support a real-time dashboard for monitoring Carris bus positions and related data.

## Project Architecture

### Streaming Data Pipeline
* Spark Structured Streaming processes JSON files from the Carris API.
* Data is stored and processed using Google Cloud Storage (GCS).

### Batch Data Pipeline
* Apache Airflow orchestrates data extraction and transformation.
* Raw data is stored in GCS and transformed for BigQuery using dbt.

## Project Structure  

This project is organized into three main folders, each corresponding to a key component of the data engineering pipeline. Below is an overview of each folder, along with the associated development branches:  

### `airflow_dags/`  
- **Branch**: `airflow_development`  
- Contains DAGs and batch scripts that will be run together within **Apache Airflow** to orchestrate workflows.  

### `dbt_project/`  
- **Branch**: `dbt_development`  
- Contains the **dbt (Data Build Tool)** project files, including models, seeds, and configurations for transforming raw data into analytics-ready datasets in **BigQuery**.  

### `spark_jobs/`  
- **Branch**: `streaming_development`  
- Contains **pySpark** streaming scripts for real-time data processing and transformation.  

## Project Resources 

### Bucket for raw data dump and staging 
 - [Bucket - Staging](https://console.cloud.google.com/storage/browser/edit-data-eng-project-group3?authuser=2&invt=AbmZ9w&project=data-eng-dev-437916&pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22)))

### Access to Airflow 
  - [Airflow](http://edit-data-eng.duckdns.org/home)
    - **Username** - daniel.moraes@̶w̶e̶a̶r̶e̶e̶d̶i̶t̶.̶i̶o̶ (without @weareedit.io)
    - **Password** - Given by Gonçalo
  - [Bucket - DAG scripts](https://console.cloud.google.com/storage/browser/edit-de-project-airflow-dags/dags;tab=objects?authuser=1&inv=1&invt=Abl9Ew&project=data-eng-dev-437916&pli=1&prefix=&forceOnObjectsSortingFiltering=false)

### BigQuery
 - [data_eng_project_group3](https://console.cloud.google.com/bigquery?referrer=search&authuser=0&inv=1&invt=AbmjTA&project=data-eng-dev-437916&ws=!1m4!1m3!3m2!1sdata-eng-dev-437916!2sdata_eng_project_group3)

### Running dbt
 - [Cloud Run Jobs](https://console.cloud.google.com/run/jobs?authuser=2&inv=1&invt=AbnSMQ&project=data-eng-dev-437916)

### Data Extraction Sources
(Not managed by us, only extraction)
 - [API Carris](https://github.com/carrismetropolitana/api)
 - [Bucket - Vehicles (Streaming)](https://console.cloud.google.com/storage/browser/edit-de-project-streaming-data/carris-vehicles;tab=objects?inv=1&invt=Abl7xw&prefix=&forceOnObjectsSortingFiltering=false&authuser=1)
 - [BigQuery - Historical Stop Times](https://console.cloud.google.com/bigquery?referrer=search&authuser=0&inv=1&invt=AbmjmQ&project=data-eng-dev-437916&ws=!1m5!1m4!4m3!1sdata-eng-dev-437916!2sde_project_teachers!3shistorical_stop_times)

### Useful Links
- [Python Client for Google Cloud Storage - Snippets](https://github.com/googleapis/python-storage/tree/main/samples/snippets)
   - How to upload into GCS - [storage_upload_file.py](https://github.com/googleapis/python-storage/blob/main/samples/snippets/storage_upload_file.py)
 
## Tools and Technologies
* Google Cloud Storage
* BigQuery
* Spark
* Airflow
* dbt
* Python

## Deliverables 
* Dashboard mockup

## Optional Features
* Add extra data sources to enrich output:
   -- weather, traffic, cultural/social events, interest points 



# Project Setup Guide

## Prerequisites

Before starting, ensure you have Python 3.x installed on your system.

## Installation Instructions

### MAC

Create and activate virtual environment:
```sh
python3 -m venv .project
source .project/bin/activate
```

Install dependencies:
```sh
pip3 install -r requirements.txt
```

### Windows

Create and activate virtual environment:
```sh
python -m venv .project
.\project\Scripts\activate.bat
```

Install dependencies:
```sh
pip install -r requirements.txt
```

## Google Cloud Setup
We also need to have GC SDK installed to connect with the Google Cloude Services platform and BigQuery.

### Installing Google Cloud SDK

#### MAC

Install Homebrew:
```sh
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
Install Google Cloud SDK using Homebrew:
```sh
brew install --cask google-cloud-sdk
```

#### Windows

Download the Google Cloud SDK Installer from: [Google Cloud SDK Installer](https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe)

Run the installer and follow the installation wizard.

Add the SDK to System PATH:
1. Open **Edit System Envirnomenet Variables**.
2. Under **System Variables**, select **Path**.
3. Add the installation path (typically `C:\Users\YourUsername\AppData\Local\Google\Cloud SDK`).

Verify installation by opening a new command prompt:
```sh
gcloud version
```

### Configuring Google Cloud

Initialize Google Cloud:
```sh
gcloud init
```

Set up application default credentials and log into your EDIT account:
```sh
gcloud auth application-default login
```
  
----

Run python script 
- python3 airflow_dags/upload_data.py  <bucket-name> <source-file-name> <target-file-name>

## DBT build commands

docker build . --tag <docker_user>/edit-de-project-dbt:<version_tag>
docker push <docker_user>/edit-de-project-dbt:<version_tag>

To create cloud job:
gcloud run jobs create group3-dbt --image <docker_user>/edit-de-project-dbt:<version_tag> --task-timeout 60m --region europe-west1 --memory 2Gi --service-account <provided_service_account>

To update existing cloud jub:

gcloud run jobs update group3-dbt --image <docker_user>/edit-de-project-dbt:<version_tag> --task-timeout 60m --region europe-west1 --memory 2Gi --service-account <provided_service_account>


## Queries to be answered


-- fact_trip -- uma tabela com uma linha por cada viagem que aconteceu (trip table * calendar_date * dates para descobrir o servico de cada dia)
-- trip_id, start_time, end_time, distance_travel

--nota: partir start e end times em dim date link e horas



-- Velocidade média
-- Número de viagens
-- Quilómetros percorridos
-- Tempo de viagem total


-- Data
-- Linha
-- Rota
-- Direção
-- Rotas que param em estação X
-- Rotas que servem munícipio X


linha -> rota 
rota varios 
   patterns 205 ir ate campanha ou ate castelo queijo


cada pattern tem um shape 


-- fact_trip -- uma tabela com uma linha por cada viagem que aconteceu (trip table * calendar_date * dates para descobrir o servico de cada dia) 

To create the fact trips we need to use silver_stop_times  and the historical stop times (to get real times info), raw_trips and the calendar dates and dates to get the type of trip
    
fact_trips {
        string trip_id
        string sk_start_date
        string sk_end_date
        string sk_routes
        string start_time
        string end_time
        string real_start_time
        string real_end_time
        string distance_travelled
        string direction
        int service_id
	}

dim_date {
	sk_date
        string month
        string day	
	string
    }
	
dim_weather {
	sk_date (we will then join this with date to get the weather for a given trip)
	weather_id ?
	temp_min
	temp_max
	}

dim_events {  (if we ever add this)
	sk_date
	columns_with_event_info
}
	

dim_routes {
	sk_routes
        int route_id
        string line_id
        string lines_long_name
        string route_name
        string i_name_municipalities_list
        string id_name_stops_list
    }



