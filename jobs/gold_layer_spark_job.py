import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, current_timestamp, lit, when, date_trunc

# The BigQuery connector for the Gold layer
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22,'
    'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0 '
    'pyspark-shell'
)

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver_to_Gold_Processing") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/gcs-connector-hadoop3-2.2.22.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/gcs-connector-hadoop3-2.2.22.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/tmp/gcp_key.json") \
        .getOrCreate()

def process_gold_layer(spark, bucket_name, project_id):
    print("Building Gold Layer: Aggregating & Joining Data...")
    
    bike_silver_path = f"gs://{bucket_name}/silver/bike/"
    weather_silver_path = f"gs://{bucket_name}/silver/weather/"
    
    try:
        bike_df = spark.read.parquet(bike_silver_path)
        weather_df = spark.read.parquet(weather_silver_path)
        bike_df = bike_df.withColumn("join_hour", date_trunc("hour", col("observation_time")))
        weather_df = weather_df.withColumn("join_hour", date_trunc("hour", col("observation_time"))).dropDuplicates(["join_hour"])
        
        bike_df = bike_df.withColumn("join_hour", date_trunc("hour", col("observation_time"))).dropDuplicates(["join_hour", "station_id"])
        weather_df = weather_df.withColumn("join_hour", date_trunc("hour", col("observation_time"))) \
                               .dropDuplicates(["join_hour"])
                               
        # DIMENSION: dim_station

        dim_station = bike_df.select(
            col("station_id"),
            col("station_name"),
            col("latitude"),
            col("longitude"),
            col("nb_docks").alias("total_docks")
        ).dropDuplicates(["station_id"])
        
        
        # 3. DIMENSION: dim_weather
      
        dim_weather = weather_df.select(
            col("join_hour"),
            col("temperature_celsius"),
            col("weather_condition"),
            col("wind_speed_m_s")
        )
        
        #FACT: fact_station_status
        
        fact_station_status = bike_df.select(
            col("station_id"), # Foreign Key to dim_station
            col("join_hour"),  # Foreign Key to dim_weather
            col("observation_time").alias("exact_observation_time"),
            
            # Metrics
            col("nb_bikes").alias("total_bikes"),
            col("nb_standard_bikes"),
            col("nb_e_bikes"),
            col("nb_empty_docks"),
            
            # Utilization percentage = (nb_bikes / nb_docks) * 100
            when(col("nb_docks") == 0, 0.0)
            .otherwise(round((col("nb_bikes").cast("double") / col("nb_docks").cast("double")) * 100, 2))
            .alias("utilization_pct"),
            
            when(col("nb_bikes") == 0, True).otherwise(False).alias("is_station_empty"),
            when(col("nb_bikes") == col("nb_docks"), True).otherwise(False).alias("is_station_full"),
            
            current_timestamp().alias("gold_loaded_at")
        )
        
        # WRITE TO BIGQUERY
        tables = {
            "dim_station": dim_station,
            "dim_weather": dim_weather,
            "fact_station_status": fact_station_status
        }
        
        for table_name, df in tables.items():
            bq_path = f"{project_id}.london_bike_gold.{table_name}"
            df.write \
                .format("bigquery") \
                .option("table", bq_path) \
                .option("temporaryGcsBucket", bucket_name) \
                .mode("overwrite") \
                .save()
            print(f"Loaded {table_name} to BigQuery.")
            
    except Exception as e:
        print(f"Error in Gold layer processing: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: gold_processing.py <gcs_bucket_name> <gcp_project_id>")
        sys.exit(1)
        
    GCS_BUCKET = sys.argv[1]
    GCP_PROJECT = sys.argv[2]
    
    spark_session = create_spark_session()
    process_gold_layer(spark_session, GCS_BUCKET, GCP_PROJECT)
    spark_session.stop()
