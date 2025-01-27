from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofweek, avg, sum, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark Session with Kafka and BigQuery Connector JARs
spark = SparkSession.builder.master("local[*]") \
    .appName("BigQueryKafkaProcessing") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
    .getOrCreate()

# Define schema based on BigQuery table structure
schema = StructType([
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# Read Kafka Stream from the "nyc-taxi" topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nyc-taxi") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the Kafka message (JSON)
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Handle missing values (replace nulls with default values)
df = df.fillna({
    "tpep_pickup_datetime": "1970-01-01 00:00:00",
    "tpep_dropoff_datetime": "1970-01-01 00:00:00",
    "passenger_count": 0,
    "trip_distance": 0.0,
    "total_amount": 0.0
})

# Data processing: Convert timestamps and extract features
df_processed = df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                 .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
                 .withColumn("year", year(col("pickup_datetime"))) \
                 .withColumn("month", month(col("pickup_datetime"))) \
                 .withColumn("day_of_week", dayofweek(col("pickup_datetime")))

# Apply watermark using the correct column (pickup_datetime)
df_processed = df_processed.withWatermark("pickup_datetime", "1 minute")

# Aggregation: Calculate average and total trip statistics per month
df_aggregated = df_processed.groupBy("year", "month") \
    .agg(
        avg("trip_distance").alias("avg_trip_distance"),
        sum("total_amount").alias("total_revenue"),
        count("*").alias("total_trips"),
        avg("passenger_count").alias("avg_passenger_count")
    )


# Write the processed data to BigQuery
query = df_aggregated.writeStream \
    .format("bigquery") \
    .option("table", "tidy-etching-447916-t0.nyc_taxi_data.nyc_taxi_transformed") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("checkpointLocation", "/tmp/bq-checkpoints/") \
    .outputMode("complete") \
    .start()

query.awaitTermination()
