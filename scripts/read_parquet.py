from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParquetReadTest").getOrCreate()

# Load the Parquet file
df = spark.read.parquet("/data/landing/part-00000-e8988741-3c1a-4751-bd18-708f4d6c0e59-c000.snappy.parquet")

# Display first few records to visually inspect
df.show()

