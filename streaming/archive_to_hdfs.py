"""Voie froide — archivage du flux Kafka dans HDFS (Spark Structured Streaming).

Kafka (topic hospital-discharges)  ->  parsing JSON + typage  ->  HDFS Parquet
                                                         partitionné par date

  - source      : Kafka (exigence de la fiche)
  - sortie      : hdfs://namenode:8020/data/raw/hospital_discharges
  - checkpoint  : hdfs://namenode:8020/checkpoints/archive_raw
                  => reprise exacte après arrêt (pas de perte ni de doublon)

Lancement (depuis la racine du projet) :
  powershell -ExecutionPolicy Bypass -File scripts/start_archive.ps1   (Windows)
  ./scripts/start_archive.sh                                    (Linux)
"""
import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "hospital-discharges")
OUTPUT = os.getenv("RAW_PATH", "hdfs://namenode:8020/data/raw/hospital_discharges")
CHECKPOINT = os.getenv("RAW_CHECKPOINT", "hdfs://namenode:8020/checkpoints/archive_raw")
TRIGGER = os.getenv("TRIGGER", "20 seconds")

INT_COLUMNS = [
    "encounter_id", "patient_nbr", "admission_type_id", "discharge_disposition_id",
    "admission_source_id", "time_in_hospital", "num_lab_procedures", "num_procedures",
    "num_medications", "number_outpatient", "number_emergency", "number_inpatient",
    "number_diagnoses",
]
STRING_COLUMNS = [
    "race", "gender", "age", "weight", "payer_code", "medical_specialty",
    "diag_1", "diag_2", "diag_3", "max_glu_serum", "A1Cresult",
    "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride",
    "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone",
    "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide", "examide",
    "citoglipton", "insulin", "glyburide-metformin", "glipizide-metformin",
    "glimepiride-pioglitazone", "metformin-rosiglitazone", "metformin-pioglitazone",
    "change", "diabetesMed", "readmitted",
]

# Schéma de l'événement JSON publié par ingestion/producer.py
EVENT_SCHEMA = StructType(
    [StructField("event_id", StringType()), StructField("event_type", StringType()),
     StructField("event_time", StringType()), StructField("seq", LongType()),
     StructField("source", StringType())]
    + [StructField(c, IntegerType()) for c in INT_COLUMNS]
    + [StructField(c, StringType()) for c in STRING_COLUMNS]
)

spark = SparkSession.builder.appName("archive-kafka-to-hdfs").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

events = (raw
    .select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"))
    .select("e.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
    .withColumn("event_time", F.to_timestamp("event_time"))
    .withColumn("ingest_date", F.to_date("kafka_timestamp")))

query = (events.writeStream
         .format("parquet")
         .option("path", OUTPUT)
         .option("checkpointLocation", CHECKPOINT)
         .partitionBy("ingest_date")
         .outputMode("append")
         .trigger(processingTime=TRIGGER)
         .queryName("archive_raw")
         .start())

print(f"Archivage démarré : {TOPIC} -> {OUTPUT}")
last_batch = None
while query.isActive:
    p = query.lastProgress
    # On n'affiche que les nouveaux lots contenant des données
    if p and p["batchId"] != last_batch and p["numInputRows"] > 0:
        print(f"[batch {p['batchId']}] {p['numInputRows']} événements archivés "
              f"({p.get('processedRowsPerSecond', 0):.1f} lignes/s)")
    if p:
        last_batch = p["batchId"]
    time.sleep(20)
query.awaitTermination()
