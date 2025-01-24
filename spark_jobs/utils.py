from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.sql.functions import min, max, first, last, col, window, udf
import pyspark.sql.functions as F
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


@udf(returnType=FloatType())
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points on the Earth's surface using the Haversine formula.

    Parameters:
    lat1 (float): Latitude of the first point in decimal degrees.
    lon1 (float): Longitude of the first point in decimal degrees.
    lat2 (float): Latitude of the second point in decimal degrees.
    lon2 (float): Longitude of the second point in decimal degrees.

    Returns:
    float: Distance between the two points in kilometers.
    """
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
        a = math.sin(delta_lat / 2)**2 + math.cos(lat1_rad) * \
            math.cos(lat2_rad) * math.sin(delta_lon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        # Distance
        distance = R * c
        return distance
    except:
        return 0


def haversine_without_udf(df: DataFrame, lat_1: str, lat_2: str, long_1: str, long_2: str) -> DataFrame:
    """
    Calculate the great-circle distance between two points on the Earth's surface using the Haversine formula
    without using a UDF (User Defined Function) in a PySpark DataFrame.

    Parameters:
    df (DataFrame): The input PySpark DataFrame containing the latitude and longitude columns.
    lat_1 (str): The name of the column containing the latitude of the first point.
    lat_2 (str): The name of the column containing the latitude of the second point.
    long_1 (str): The name of the column containing the longitude of the first point.
    long_2 (str): The name of the column containing the longitude of the second point.

    Returns:
    DataFrame: The input DataFrame with an additional column "distance" containing the calculated distances in kilometers.
    """
    dist_df = (df
               .withColumn(
                   "distance",
                   6371.0 * (2 * F.atan2(
                       F.sqrt(

                           F.sin(
                               (F.radians(col(lat_2)) - F.radians(col(lat_1))) / 2)**2 +

                           F.cos(F.radians(col(lat_1))) * F.cos(F.radians(col(lat_2))) *
                           F.sin((F.radians(col(long_2)) -
                                  F.radians(col(long_1)))/2)**2

                       ),
                       F.sqrt(1 - (F.sin(
                           (F.radians(col(lat_2)) - F.radians(col(lat_1))) / 2)**2 +

                           F.cos(F.radians(col(lat_1))) * F.cos(F.radians(col(lat_2))) *
                           F.sin((F.radians(col(long_2)) - F.radians(col(long_1)))/2)**2))))
               )

               )
    return dist_df


def aggregate_data(df: DataFrame) -> DataFrame:
    """
    Aggregate data in a PySpark DataFrame using a sliding window.

    Parameters:
    df (DataFrame): The input PySpark DataFrame containing the data to be aggregated.

    Returns:
    DataFrame: The transformed DataFrame with aggregated data.
    """
    window_spec = window("timestamp", "2 minutes", "30 seconds")

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


def calculate_distances_udf(df: DataFrame) -> DataFrame:
    """
    Transform the data in a PySpark DataFrame by calculating the distance, time delta, and average speed.

    Parameters:
    df (DataFrame): The input PySpark DataFrame containing the data to be transformed.

    Returns:
    DataFrame: The transformed DataFrame with additional columns for distance, time delta, and average speed.
    """
    dist_df = (df
               .withColumn(
                   "distance",
                   haversine(col("first_lat"), col("first_lon"), col("last_lat"), col("last_lon")
                                 ))
               .withColumn(
                   "time_delta", col("last_timestamp").cast(
                       "long") - col("first_timestamp").cast("long")
               ).withColumn("average_speed", col("distance") / (col("time_delta") / 3600))
               )
    return dist_df


def calculate_distances(df: DataFrame) -> DataFrame:    
    """
    Transform the data in a PySpark DataFrame by calculating the distance, time delta, and average speed.

    Parameters:
    df (DataFrame): The input PySpark DataFrame containing the data to be transformed.

    Returns:
    DataFrame: The transformed DataFrame with additional columns for distance, time delta, and average speed.
    """
    dist_df = (df
               .transform(haversine_without_udf, 'first_lat', 'first_lon', 'last_lat', 'last_lon')
               .withColumn(
                   "time_delta", col("last_timestamp").cast(
                       "long") - col("first_timestamp").cast("long")
               ).withColumn("average_speed", col("distance") / (col("time_delta") / 3600))
               )
    return dist_df


def get_stops(spark: SparkSession) -> DataFrame:
    """
    Fetch stop data from the Carris Metropolitana API and create a PySpark DataFrame.

    Parameters:
    spark (SparkSession): The Spark session to use for creating the DataFrame.

    Returns:
    DataFrame: A PySpark DataFrame containing stop_id, stop_lat, and stop_lon columns.
    """
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
