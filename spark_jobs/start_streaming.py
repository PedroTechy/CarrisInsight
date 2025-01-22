from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
from pyspark.sql.functions import min, max, first, last, col, window, from_unixtime, to_timestamp, count, udf
import utils as utils
import subprocess
import os

subprocess.run(['rm', '-rf', 'content/lake/' + '/*'], check=True)
home_directory = os.getenv("HOME")

spark = SparkSession.builder \
    .appName('pyspark-run-with-gcp-bucket') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

gs_input_path = "gs://edit-de-project-streaming-data/carris-vehicles"
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                     f"{home_directory}/.config/gcloud/application_default_credentials.json")

df = (spark.readStream.option("maxFilesPerTrigger", 1)
      .format("json")
      .schema(utils.schema)
      .load(gs_input_path))
print("will start streaming")


transformed_df = df.withColumn(
    "timestamp", to_timestamp(from_unixtime("timestamp")))
# Write a df with the datetype transform only
print("Transformed data")


# Group by vehicle ID and window, then get the first and last timestamps and lat/lon values
result_df = (
    transformed_df
    .transform(utils.aggregate_data)
    .transform(
        utils.calculate_distances)

)

query = (result_df.writeStream
         .outputMode('append')
         .option('checkpointLocation', 'content/lake/processing/vehicles_checkpoint')
         .trigger(processingTime='10 seconds')
         .start('content/lake/processing/vehicles/data')
         )

query.awaitTermination(300)

query.stop()
