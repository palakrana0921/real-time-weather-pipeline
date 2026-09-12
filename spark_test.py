from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("WeatherPipeline")
    .getOrCreate()
)

data = [
    ("Pune", 30, 70),
    ("Mumbai", 31, 75),
    ("Delhi", 34, 40)
]

columns = ["city", "temperature", "humidity"]

df = spark.createDataFrame(data, columns)

df.show()

spark.stop()