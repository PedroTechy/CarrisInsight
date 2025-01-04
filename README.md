# CarrisInsight
## Data Engineering – Applied Project

## Project Overview
This project focuses on creating data pipelines to support a real-time dashboard for monitoring Carris bus positions and related data.

## Project Structure
* **dbt Models**: Data transformations for BigQuery.
* **Spark Jobs**: Real-time data processing.
* **Airflow DAGs**: Batch data workflow orchestration.

## Architecture

### Streaming Data Pipeline
* Spark Structured Streaming processes JSON files from the Carris API.
* Data is stored and processed using Google Cloud Storage (GCS).

### Batch Data Pipeline
* Apache Airflow orchestrates data extraction and transformation.
* Raw data is stored in GCS and transformed for BigQuery using dbt.

## Tools and Technologies
* Google Cloud Storage
* BigQuery
* Spark
* Airflow
* dbt
* Python

## Deliverables 
* Dashbaord mockup

## Optional Features
* Add extra data sources to enrich output:
   -- weather, traffic, cultural/social events, interest points 
* 

## Checklist 

### Bucket for dags
 -  https://console.cloud.google.com/storage/browser/edit-de-project-airflow-dags/dags;tab=objects?authuser=1&invt=Abl7xA&project=data-eng-dev-437916&pli=1&prefix=&forceOnObjectsSortingFiltering=false

### Bucket for raw data dump and staging 
 - 

### Bucket for reading streaming data 
 - https://console.cloud.google.com/storage/browser/edit-de-project-streaming-data/carris-vehicles;tab=objects?inv=1&invt=Abl7xw&prefix=&forceOnObjectsSortingFiltering=false&authuser=1

### Access to Airflow 
  - 
### Access to BigQuery
  - 


## Project notes diary

### Day 1 
 - Extract data from API and load in bucket
      https://github.com/googleapis/python-storage/blob/main/samples/snippets/storage_upload_file.py

 - Get data from bucket - transform to tabular - load to big query 



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

Install using Homebrew:
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
