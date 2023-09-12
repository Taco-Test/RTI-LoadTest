from delta import *
from pyspark.sql.functions import col
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from confluent_kafka.avro import CachedSchemaRegistryClient
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, NullType

KAFKA_BROKER = "localhost:29092"  # Update if your Kafka broker is running on a different host or port
TOPIC_NAME = "postgres.public.memberdetails"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


builder = SparkSession.builder.appName("DebeziumCDCConsumer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
STORAGE_ACCOUNT = "realtimeingestion"
ACCOUNT_KEY = "AKAr03HZgakLGk2CydRkuvA/y/1c7I/SiR//QCiLCknGRGdFRIIf+Tlsn88sXV/CTJ5XGLaOzgsx+AStW0IaWg=="
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",ACCOUNT_KEY)

BASE_DIR = "wasbs://olas-data-extract@realtimeingestion.blob.core.windows.net/"
        
raw_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER).option("subscribe", TOPIC_NAME).option("startingOffsets", "earliest").load()

raw_stream_string = raw_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Load the Avro schema
schema_registry_client = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
kafka_topic_value_schema = schema_registry_client.get_latest_schema(f"{TOPIC_NAME}-value")

schema = avro.schema.Parse(open("./schema.avsc", "rb").read())
# Create a DatumReader
reader = DatumReader(schema)

def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(7)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict

def avro_to_struct(avro_schema_str):
    avro_schema = json.loads(avro_schema_str)
    fields = avro_schema.get("fields", [])
    
    struct_fields = []
    for field in fields:
        name = field["name"]
        type_ = field["type"]
        nullable = False
        
        # Determine the Spark data type based on Avro type
        if type_ == "int":
            spark_type = IntegerType()
        elif type_ == "string":
            spark_type = StringType()
        elif isinstance(type_, list):  # Handling nullable fields
            non_null_type = next(filter(lambda x: x != "null", type_), "string")
            if non_null_type == "string":
                spark_type = StringType()
            else:
                raise ValueError(f"Unsupported type {non_null_type}")
            nullable = True
        else:
            raise ValueError(f"Unsupported type {type_}")
        
        struct_fields.append(StructField(name, spark_type, nullable))
    
    return StructType(struct_fields)

def upsert_to_delta_table(delta_path: str, new_data: DataFrame, merge_condition: str, update_mappings: dict, insert_mappings: dict):
    deltaTable = DeltaTable.forPath(spark, delta_path)

    (deltaTable.alias("oldData")
     .merge(new_data.alias("newData"), merge_condition)
     .whenMatchedUpdate(set = update_mappings)
     .whenNotMatchedInsert(values = insert_mappings)
     .execute())
    print("success")
    return None

consumer = KafkaConsumer("postgres.public.memberdetails",bootstrap_servers=["localhost:29092"])

DELTA_TABLE_PATH = "./deltalake_test"
CHECKPOINT_DIR = "./deltalake_test/checkpoints"

struct_schema = avro_to_struct(schema)
try:
    spark.read.format("delta").load(DELTA_TABLE_PATH)
except Exception as e:
    # If table does not exist, create an empty one with the desired schema
    spark.createDataFrame([], struct_schema).write.format("delta").save(DELTA_TABLE_PATH)

while True:
    count = 0
    for msg in consumer:   
        data = decode(msg.value)
        new_data = spark.createDataFrame([Row(data)])
        print(data)
        try:
            new_data.write.format("delta").mode("append").save(DELTA_TABLE_PATH+TOPIC_NAME.split('.')[-1])
           # query = df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/path/to/checkpoint/dir").start(DELTA_TABLE_PATH)
           # query.awaitTermination()
        except:
            deltaTable = DeltaTable.forPath(spark, DELTA_TABLE_PATH+TOPIC_NAME.split('.')[-1])
            deltaTable.alias("oldData").merge(new_data.alias("newData"),"oldData.Id = newData.Id").whenMatchedUpdate(set = { "first_name": "newData.first_name", "last_name": "newData.last_name", "gender": "newData.gender", "email_id": "newData.email_id", "country_of_birth": "newData.country_of_birth", "company_name": "newData.company_name" }).whenNotMatchedInsert(values = { "Id": "newData.Id", "first_name": "newData.first_name", "last_name": "newData.last_name", "gender": "newData.gender", "email_id": "newData.email_id", "country_of_birth": "newData.country_of_birth", "company_name": "newData.company_name" }).execute()
