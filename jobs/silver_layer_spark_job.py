import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, from_unixtime

def create_spark_session():
    """
    Creates a SparkSession configured to talk to Google Cloud Storage.
    """
    return SparkSession.builder \
        .appName("Bronze_to_Silver_Processing") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/gcs-connector-hadoop3-2.2.22.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/gcs-connector-hadoop3-2.2.22.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "/tmp/gcp_key.json") \
        .getOrCreate()

def process_weather_data(spark, bucket_name):
    print("Processing Weather Data...")
    
    bronze_path = f"gs://{bucket_name}/bronze/weather/*/*.json"
    
    try:
        raw_weather_df = spark.read.json(bronze_path)
        
        # JSON is heavily nested retrieve only relevant information
        silver_weather_df = raw_weather_df.select(
            from_unixtime(col("dt")).cast("timestamp").alias("observation_time"), # Ground truth time from API
            col("name").alias("city"),
            col("main.temp").alias("temperature_celsius"),
            col("main.humidity").alias("humidity_percent"),
            col("wind.speed").alias("wind_speed_m_s"),
            col("weather").getItem(0).getField("main").alias("weather_condition"),
            current_timestamp().alias("processed_at") # Metadata: when the pipeline ran
        )
        
        # 3. Write to the Silver layer as Parquet
        silver_path = f"gs://{bucket_name}/silver/weather/"
        
        silver_weather_df.write.mode("overwrite").parquet(silver_path)
            
        print(f"Weather data processed and saved to {silver_path}")
        
    except Exception as e:
        print(f"Error processing weather data: {e}")

def process_bike_data(spark, bucket_name):
    print("Processing Bike Data...")
    
    bronze_path = f"gs://{bucket_name}/bronze/bike/*/*.json"
    
    try:
        # TfL returns a massive array of JSON objects, Spark handles this natively
        raw_bike_df = spark.read.json(bronze_path)
        
        # Get top level items 
        silver_bike_df = raw_bike_df.select(
            col("id").alias("station_id"),
            col("commonName").alias("station_name"),
            col("lat").alias("latitude"),
            col("lon").alias("longitude"),
            # Extracting values from additional properties nested array
            expr("filter(additionalProperties, x -> x.key = 'NbBikes')[0].value").cast("int").alias("nb_bikes"),
            expr("filter(additionalProperties, x -> x.key = 'NbEmptyDocks')[0].value").cast("int").alias("nb_empty_docks"),
            expr("filter(additionalProperties, x -> x.key = 'NbDocks')[0].value").cast("int").alias("nb_docks"),
            expr("filter(additionalProperties, x -> x.key = 'NbBikes')[0].modified").cast("timestamp").alias("observation_time"), # Ground truth time from API
            current_timestamp().alias("processed_at") # Metadata: when the ETL ran
        )
        
        silver_path = f"gs://{bucket_name}/silver/bike/"
        
        silver_bike_df.write.mode("overwrite").parquet(silver_path)
            
        print(f"Bike data processed and saved to {silver_path}")
        
    except Exception as e:
        print(f"Error processing bike data: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: silver_processing.py <gcs_bucket_name>")
        sys.exit(1)
        
    GCS_BUCKET = sys.argv[1]
    
    spark_session = create_spark_session()
    
    process_weather_data(spark_session, GCS_BUCKET)
    process_bike_data(spark_session, GCS_BUCKET)
    
    spark_session.stop()