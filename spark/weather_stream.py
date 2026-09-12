from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("WeatherKafkaStream")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
weather_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather-data")
    .option("startingOffsets", "earliest")
    .load()
)

# Kafka value is stored as binary, so convert it to string
weather_data = weather_stream.select(
    col("value").cast("string").alias("weather_json")
)

# Display the incoming data
query = (
    weather_data.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()