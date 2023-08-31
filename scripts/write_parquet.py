from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro, to_avro

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaToParquetApp") \
        .getOrCreate()

def read_avro_schema_from_file(schema_path):
    with open(schema_path, 'r') as file:
        return file.read()

def kafka_to_parquet(spark, avro_schema, kafka_bootstrap_servers, topic):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .option("failOnDataLoss", "false")\
        .load()
    
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    # Define the schema based on the table structure
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

    # Deserialize the Avro data from Kafka
    avro_df = df.select(from_avro(col("value"), avro_schema).alias("memberdetails"))
    
    # Extract the nested fields
    columns_to_select = [field.name for field in schema.fields]
    processed_df = avro_df.select("memberdetails.*").select(*columns_to_select)

    query = processed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/data/raw_data") \
        .option("checkpointLocation", "/data/rawdata_checkpoints") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    avro_schema = read_avro_schema_from_file('/data/schemas/memberdetails.avsc')
    kafka_to_parquet(spark, avro_schema, "kafka:9092", "postgres.public.memberdetails")

