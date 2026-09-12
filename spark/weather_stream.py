from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

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
    .option("kafka.bootstrap.servers", "172.22.16.1:9092")
    .option("subscribe", "weather-data")
    .option("startingOffsets", "earliest")
    .load()
)

# Define the JSON structure
weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Convert Kafka value to string
weather_json = weather_stream.select(
    col("value").cast("string").alias("weather_json")
)

# Parse JSON
weather_data = weather_json.select(
    from_json(col("weather_json"), weather_schema).alias("data")
).select(
    "data.city",
    "data.temperature",
    "data.humidity",
    "data.timestamp"
)

# Display the processed data
query = (
    weather_data.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()