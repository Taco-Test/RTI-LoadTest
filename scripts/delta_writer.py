from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaToDeltaLake") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

kafka_bootstrap_servers = "kafka:9092"  # Replace with your Kafka brokers
kafka_topic = "postgres.public.memberdetails"

schema = StructType([
    StructField("Id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), True),
    StructField("birthdate", DateType(), True),
    StructField("email_id", StringType(), True),
    StructField("country_of_birth", StringType(), True),
    StructField("company_name", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

parsed_df = kafka_df.select(from_avro("value", schema).alias("data")).select("data.*")

delta_table_path = "deltalake_rti/test/"
if not spark._jsparkSession.catalog().tableExists(delta_table_path):
    parsed_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_table_path)
else:
    delta_table = spark.read \
        .format("delta") \
        .load(delta_table_path)
delta_table = DeltaTable.forPath(spark, delta_table_path)
delta_table.alias("existing_data") \
    .merge(
        parsed_df.alias("new_data"),
        "existing_data.id = new_data.id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

query = parsed_df.writeStream \
    .format("delta") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.format("delta").mode("append").save(delta_table_path)) \
    .start()

query.awaitTermination()
