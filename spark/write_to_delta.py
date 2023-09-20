from delta import *
from pyspark.sql.functions import col
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.avro import CachedSchemaRegistryClient
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, NullType

KAFKA_BROKER = "localhost:29092"  # Update if your Kafka broker is running on a different host or port
TOPIC_NAME = "postgres.public.customer"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
schema_registry_subject = f"{TOPIC_NAME}-value"
STORAGE_ACCOUNT = "cdctestsprint0a"   # "realtimeingestion"
ACCOUNT_KEY = "OdfaSZLm20odct7tQbSd6SrYuZIYecOKNKFDLRcvvcf02xCG0LfAuvEwCPOV7i97+QvdwAAaUj+n+AStoS5JUQ=="
# ACCOUNT_KEY = "AKAr03HZgakLGk2CydRkuvA/y/1c7I/SiR//QCiLCknGRGdFRIIf+Tlsn88sXV/CTJ5XGLaOzgsx+AStW0IaWg=="
BASE_DIR = "wasbs://debezium-cdc@cdctestsprint0a.blob.core.windows.net"
CHECKPOINT_LOC = "cdc_test/checkpoints"
TARGET_FOLDER = "cdc_test/tables"

builder = SparkSession.builder.appName("DebeziumCDCConsumer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",ACCOUNT_KEY)
raw_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER).option("subscribe", TOPIC_NAME).option("startingOffsets", "earliest").option("failOnDataLoss", "false").load()

raw_stream_string = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

#  get magic byte value
raw_stream = raw_stream.withColumn("magicByte", func.expr("substring(value, 1, 1)"))

#  get schema id from value
raw_stream = raw_stream.withColumn("valueSchemaId", func.expr("substring(value, 2, 4)"))

# remove first 5 bytes from value
raw_stream = raw_stream.withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)
    return sr, latest_version

# get schema using subject name
_, latest_version_raw_stream = get_schema_from_schema_registry(SCHEMA_REGISTRY_URL, schema_registry_subject)
TABLENAME = TOPIC_NAME.split('.')[-1]

# deserialize data
fromAvroOptions = {"mode":"PERMISSIVE"}

decoded_output = raw_stream.select(from_avro(func.col("fixedValue"), latest_version_raw_stream.schema.schema_str, fromAvroOptions).alias("raw_stream"))
raw_stream_value_df = decoded_output.select("raw_stream.*")
raw_stream_value_df.printSchema()
table_vals = raw_stream_value_df.select(col("before.id").alias("before_id"),"after.*",col("op").alias("operation")) 
raw_stream_value_df \
	.writeStream \
	.format("delta") \
	.trigger(processingTime='30 second') \
	.outputMode("append") \
	.option("truncate", "false") \
	.option("failOnDataLoss","false") \
	.option("checkpointLocation",BASE_DIR+'/'+CHECKPOINT_LOC+'/'+TABLENAME) \
	.start(BASE_DIR+'/'+TARGET_FOLDER+'/'+TABLENAME) \
	.awaitTermination()

TABLENAME2= TABLENAME+'values'

table_vals.writeStream \
        .format("delta") \
        .trigger(processingTime='30 second') \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("failOnDataLoss","false") \
        .option("checkpointLocation",BASE_DIR+'/'+CHECKPOINT_LOC+'2') \
        .start(BASE_DIR+'/'+TARGET_FOLDER+'/'+TABLENAME2) \
        .awaitTermination()

#schema_registry_client = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
#kafka_topic_value_schema = schema_registry_client.get_latest_schema(f"{TOPIC_NAME}-value")
#schema = avro.schema.Parse(open("./schema.avsc", "rb").read())
# Create a DatumReader
#reader = DatumReader(schema)

#def decode(msg_value):
#    message_bytes = io.BytesIO(msg_value)
#    message_bytes.seek(7)
#    decoder = BinaryDecoder(message_bytes)
#    event_dict = reader.read(decoder)
#    return event_dict
#
#def avro_to_struct(avro_schema_str):
#    avro_schema = json.loads(avro_schema_str)
#    fields = avro_schema.get("fields", [])
#    
#    struct_fields = []
#    for field in fields:
#        name = field["name"]
#        type_ = field["type"]
#        nullable = False
#        
#        # Determine the Spark data type based on Avro type
#        if type_ == "int":
#            spark_type = IntegerType()
#        elif type_ == "string":
#            spark_type = StringType()
#        elif isinstance(type_, list):  # Handling nullable fields
#            non_null_type = next(filter(lambda x: x != "null", type_), "string")
#            if non_null_type == "string":
#                spark_type = StringType()
#            else:
#                raise ValueError(f"Unsupported type {non_null_type}")
#            nullable = True
#        else:
#            raise ValueError(f"Unsupported type {type_}")
#        
#        struct_fields.append(StructField(name, spark_type, nullable))
#    
#    return StructType(struct_fields)
#
#def upsert_to_delta_table(delta_path: str, new_data: DataFrame, merge_condition: str, update_mappings: dict, insert_mappings: dict):
#    deltaTable = DeltaTable.forPath(spark, delta_path)
#
#    (deltaTable.alias("oldData")
#     .merge(new_data.alias("newData"), merge_condition)
#     .whenMatchedUpdate(set = update_mappings)
#     .whenNotMatchedInsert(values = insert_mappings)
#     .execute())
#    print("success")
#    return None
#
#consumer = KafkaConsumer("postgres.public.memberdetails",bootstrap_servers=["localhost:29092"])
#
#DELTA_TABLE_PATH = "./deltalake_test"
#CHECKPOINT_DIR = "./deltalake_test/checkpoints"
#
#struct_schema = avro_to_struct(schema)
#try:
#    spark.read.format("delta").load(DELTA_TABLE_PATH)
#except Exception as e:
#    # If table does not exist, create an empty one with the desired schema
#    spark.createDataFrame([], struct_schema).write.format("delta").save(DELTA_TABLE_PATH)
#
#while True:
#    count = 0
#    for msg in consumer:   
#        data = decode(msg.value)
#        new_data = spark.createDataFrame([Row(data)])
#        print(data)
#        try:
#            new_data.write.format("delta").mode("append").save(DELTA_TABLE_PATH+TOPIC_NAME.split('.')[-1])
#           # query = df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/path/to/checkpoint/dir").start(DELTA_TABLE_PATH)
#           # query.awaitTermination()
#        except:
#            deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH+TOPIC_NAME.split('.')[-1])
#            deltaTable.alias("oldData").merge(new_data.alias("newData"),"oldData.Id = newData.Id").whenMatchedUpdate(set = { "first_name": "newData.first_name", "last_name": "newData.last_name", "gender": "newData.gender", "email_id": "newData.email_id", "country_of_birth": "newData.country_of_birth", "company_name": "newData.company_name" }).whenNotMatchedInsert(values = { "Id": "newData.Id", "first_name": "newData.first_name", "last_name": "newData.last_name", "gender": "newData.gender", "email_id": "newData.email_id", "country_of_birth": "newData.country_of_birth", "company_name": "newData.company_name" }).execute()
