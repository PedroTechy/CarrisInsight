from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
from pyspark.sql.functions import min, max, first, last, col, window, from_unixtime, to_timestamp, count, udf
import math 
import requests
import json

schema = StructType([
    StructField("bearing", FloatType(), True),
    StructField("block_id", StringType(), True),
    StructField("current_status", StringType(), True),
    StructField("id", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("line_id", StringType(), True),
    StructField("lon", FloatType(), True),
    StructField("pattern_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("schedule_relationship", StringType(), True),
    StructField("shift_id", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("stop_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("trip_id", StringType(), True)
])

def haversine(lat1, lon1, lat2, lon2):
    try:
      # Earth radius in kilometers
      R = 6371.0

      # Convert latitude and longitude from degrees to radians
      lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
      lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)

      # Differences
      delta_lat = lat2_rad - lat1_rad
      delta_lon = lon2_rad - lon1_rad

      # Haversine formula
      a = math.sin(delta_lat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2)**2
      c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

      # Distance
      distance = R * c
      return distance
    except:
      return 0
haversine_udf = udf(haversine, FloatType())
    
def aggregate_data(df):
    window_spec = window("timestamp", "2 minutes", "10 seconds")

    transformed = (df.withWatermark("timestamp", "3 minutes")
    .groupBy("id", "trip_id", window_spec)
    .agg(
        max("current_status").alias("current_status"),
        max("route_id").alias("route_id"),
        max("stop_id").alias("stop_id"),
        min("timestamp").alias("first_timestamp"),
        max("timestamp").alias("last_timestamp"),
        first("lat").alias("first_lat"),
        first("lon").alias("first_lon"),
        last("lat").alias("last_lat"),
        last("lon").alias("last_lon")
    )
    )
    return transformed



def calculate_distances(df):
    dist_df = (df.withColumn(
        "distance",
        haversine_udf(col("first_lat"), col("first_lon"), col("last_lat"), col("last_lon")
        ))
    .withColumn(
        "time_delta", col("last_timestamp").cast("long") - col("first_timestamp").cast("long")
    ).withColumn("average_speed", col("distance") / (col("time_delta") / 3600))
        )
    print("ended transform")

    return dist_df

def get_stops(spark):
    url = f"https://api.carrismetropolitana.pt/stops"
    response = requests.get(url)

    filtered = [{'stop_id': stop['stop_id'], 'stop_lat': float(
        stop['lat']), 'stop_lon': float(stop['lon'])} for stop in json.loads(response.text)]
    
    schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("stop_lat", FloatType(), True),
    StructField("stop_lon", FloatType(), True)])

    stops = spark.createDataFrame(filtered, schema=schema)

    return stops


