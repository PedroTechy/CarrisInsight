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

## Future Enhancements
* Adding more data sources.
* Improving batch pipeline for historical data.
* Adding monitoring and alerting features.

## Checklist 

Bucket for dags
 -  https://console.cloud.google.com/storage/browser/edit-de-project-airflow-dags/dags;tab=objects?authuser=1&invt=Abl7xA&project=data-eng-dev-437916&pli=1&prefix=&forceOnObjectsSortingFiltering=false

Bucket for raw data dump and staging 
 - ?

Bucket for streaming data 
 - https://console.cloud.google.com/storage/browser/edit-de-project-streaming-data/carris-vehicles;tab=objects?inv=1&invt=Abl7xw&prefix=&forceOnObjectsSortingFiltering=false&authuser=1

Access to Airflow 
  - 
Access to BigQuery
