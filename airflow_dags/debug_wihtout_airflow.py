from dag_extract_and_load_data_g3 import *

if __name__=="__main__":
    extract_and_store_zip_files(BASE_API_URL, "gtfs", ZIP_FILES)
    load_tables_from_bucket_to_bigquery(BUCKET_NAME, BIGQUERY_PROJECT, BIGQUERY_DATASET, "stop_times", "csv")