from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_timestamp
import utils as utils
import subprocess
import os
import logging

logging.basicConfig(level=logging.INFO,  # Set the default logging level
                    # Log format
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    # Output to stdout (default)
                    handlers=[logging.StreamHandler()])


# Globals
GS_INPUT_PATH = "gs://edit-de-project-streaming-data/carris-vehicles"
HOME_DIRECTORY = os.getenv("HOME")


# Clean directory before starting
subprocess.run(['rm', '-rf', 'content/lake/' + '/*'], check=True)

# Configure the Spark application
spark = SparkSession.builder \
    .appName('pyspark-run-with-gcp-bucket') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

# Login with command gcloud auth application-default login
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                     f"{HOME_DIRECTORY}/.config/gcloud/application_default_credentials.json")

# Read the data from the bucket
df = (spark.readStream.option("maxFilesPerTrigger", 1)
      .format("json")
      .schema(utils.schema)
      .load(GS_INPUT_PATH))
logging.info("will start streaming")


transformed_df = df.withColumn(
    "timestamp", to_timestamp(from_unixtime("timestamp")))

# Group by vehicle ID and window, then get the first and last timestamps and lat/lon values
result_df = (
    transformed_df
    .transform(utils.aggregate_data)
    .transform(utils.calculate_distances_udf)

)
#Write streaming result to bucket in parquet format
query = (result_df.writeStream
         .outputMode('append')
         .option('checkpointLocation', 'content/lake/processing/vehicles_checkpoint')
         .trigger(processingTime='10 seconds')
         .start('content/lake/processing/vehicles/data')
         )

query.awaitTermination(300)

query.stop()
