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

---

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
```
```sh
.\.project\Scripts\Activate.ps1
```

Install dependencies:
```sh
pip install -r requirements.txt
```

## Google Cloud Setup
We also need to have GC SDK installed to connect with the Google Cloud Services platform and BigQuery.

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
1. Open **Edit System Environment Variables**.
2. Under **System Variables**, select **Path**.
3. Add the installation path (typically `C:\Users\YourUsername\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin`).

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

## DBT Setup

- [DBT Setup Repo with README](https://github.com/jgnog/edit-de-project-dbt)

Setup the working directory for dbt:
(Make sure you are in the correct directory - dbt_project)
![image](https://github.com/user-attachments/assets/9422f03b-ba58-4bf9-a946-42ccdb4c48f0)
```sh
export DBT_PROFILES_DIR=.
```

----

Run python script 
- python3 airflow_dags/upload_data.py  <bucket-name> <source-file-name> <target-file-name>
