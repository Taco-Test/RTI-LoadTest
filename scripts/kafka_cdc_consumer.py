from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KafkaCDCConsumer") \
        .getOrCreate()

    # Reduce logging
    spark.sparkContext.setLogLevel("WARN")
    topic_name = "postgres.public.memberdetails"

    # Reading data from Kafka
    kafkaStream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",  "kafka:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest")\
        .load()

    # Here, "value" is the message content from Kafka.
    # You can perform transformations as needed.
    values = kafkaStream.selectExpr("CAST(value AS STRING)")

    # Writing data to Parquet in append mode
    query = values.writeStream \
     .outputMode("append") \
     .format("parquet") \
     .option("path", "/data/l2") \
     .option("checkpointLocation", "/data/c2") \
     .start()

#    query = values.writeStream \
#        .outputMode("append") \
#        .format("console") \
#        .start()

    query.awaitTermination()

