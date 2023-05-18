import datetime
import pandas as pd
from kafka import KafkaProducer
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, BooleanType

"""""
    dans ce fichier on va faire de la data preparation
    en spark on va lire le fichier dans la bucket minio
    enregistrer la données en dataframe
    et reecrire dans une nouvelle bucket minIO
    
"""""
if __name__ == "__main__":
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    timestamp = datetime.datetime.now().strftime('%d-%m-%y')
    data = client.get_object("receiver", f"receiver_{timestamp}.json")
    df = pd.read_json(data.read().decode('utf-8'))
    if not client.bucket_exists("received"):
        client.make_bucket("received")
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Connect to local Spark instance
    spark = SparkSession.builder.appName("ReceiverDataStorage").getOrCreate()
    # Define schema for Capteurs data
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("height", DoubleType(), True),
        StructField("width", DoubleType(), True),
        StructField("door_number", IntegerType(), True),
        StructField("entrance_exit", BooleanType(), True),
        StructField("parking_spot", IntegerType(), True),
    ])

    # Define Kafka topic to read from
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "receiver") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    timestamp = datetime.datetime.now().strftime('%d-%m-%y')
    df.to_json(f"received_{timestamp}.json", orient='records')
    client.fput_object("received", f"received_{timestamp}.json", f"received_{timestamp}.json")