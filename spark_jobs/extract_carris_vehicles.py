from pyspark.sql import SparkSession

# Initialize Spark session with BigQuery connector
spark = SparkSession.builder \
    .appName("GCS to BigQuery Streaming with Auto-Table Creation") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar") \
    .getOrCreate()

# Input GCS path with your data
input_path = "gs://edit-de-project-streaming-data/carris-vehicles"

# Read streaming data from GCS without specifying schema
streaming_df = spark.readStream \
    .format("json") \
    .load(input_path)

# Define output BigQuery table
output_table = "data-eng-dev-437916.data_eng_project_group3.raw_vehicles"

# Write streaming data to BigQuery with auto-table creation
streaming_query = streaming_df.writeStream \
    .format("bigquery") \
    .option("table", output_table) \
    .option("checkpointLocation", "gs://edit-data-eng-project-group3/streaming_data/checkpoints") \
    .option("temporaryGcsBucket", "gs://edit-data-eng-project-group3/streaming_data/temp-bucket") \
    .option("writeDisposition", "WRITE_APPEND") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .outputMode("append") \
    .start()

# Wait for the streaming query to finish
streaming_query.awaitTermination()
