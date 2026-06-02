# consumer.py
# PURPOSE: Reads real-time patient vitals from Kafka topic
#          and loads them into Snowflake for dbt transformation
#
# This is the CONSUME and LOAD step of our streaming pipeline.
# Producer sends vitals → Kafka stores them → Consumer reads and loads to Snowflake
#
# In my Cigniti work: PySpark Structured Streaming consumed Kafka topics
# and loaded into Azure Data Lake. Same pattern here with Python + Snowflake.

import json
import os
import logging
from datetime import datetime
from kafka import KafkaConsumer
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# ─────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER      = "localhost:9092"
KAFKA_TOPIC       = "patient-vitals"
CONSUMER_GROUP    = "vitals-snowflake-loader"

# Batch settings
# Instead of loading one record at a time (slow),
# we collect records into a batch and load together (fast)
# In production: batch size depends on latency requirements
BATCH_SIZE        = 10    # load every 10 records
BATCH_TIMEOUT_SEC = 30    # or every 30 seconds — whichever comes first


# ─────────────────────────────────────────────────────────────────
# SNOWFLAKE CONNECTION
# Same pattern as Project 1 — credentials from environment variables
# Never hardcode passwords in code
# ─────────────────────────────────────────────────────────────────

def get_snowflake_connection():
    """
    Creates Snowflake connection using environment variables.
    In production: credentials come from AWS Secrets Manager
    or Azure Key Vault — never hardcoded.
    """
    conn = snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="HEALTHCARE_RAW",
        schema="PUBLIC"
    )
    logger.info("Snowflake connection established")
    return conn


def setup_snowflake(conn):
    """
    Creates the raw database, schema, and table in Snowflake.
    Runs once on first startup — idempotent (safe to run multiple times).
    CREATE IF NOT EXISTS means it never fails on re-run.
    """
    cursor = conn.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS HEALTHCARE_RAW")
    cursor.execute("USE DATABASE HEALTHCARE_RAW")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_PATIENT_VITALS (
            patient_id            VARCHAR,
            patient_name          VARCHAR,
            age                   INTEGER,
            ward                  VARCHAR,
            condition             VARCHAR,
            doctor_id             VARCHAR,
            heart_rate_bpm        INTEGER,
            systolic_bp_mmhg      INTEGER,
            diastolic_bp_mmhg     INTEGER,
            spo2_pct              INTEGER,
            temperature_celsius   FLOAT,
            respiratory_rate      INTEGER,
            reading_timestamp     TIMESTAMP_NTZ,
            device_id             VARCHAR,
            hospital_id           VARCHAR,
            data_source           VARCHAR,
            ingested_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    logger.info("Snowflake table RAW_PATIENT_VITALS ready")
    cursor.close()


# ─────────────────────────────────────────────────────────────────
# KAFKA CONSUMER
# Reads messages from Kafka topic in batches
# Batching is more efficient than one-by-one inserts
# ─────────────────────────────────────────────────────────────────

def create_consumer():
    """
    Creates Kafka consumer connected to the patient-vitals topic.

    consumer_group: tracks which messages this consumer has read.
    If consumer restarts, it picks up from where it left off.
    Critical for not losing or duplicating data.

    auto_offset_reset='earliest': if no checkpoint exists,
    start from the beginning of the topic.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_records=BATCH_SIZE
    )
    logger.info(f"Kafka consumer connected | Group: {CONSUMER_GROUP}")
    return consumer


# ─────────────────────────────────────────────────────────────────
# BATCH LOADER
# Collects records into a list, converts to DataFrame,
# and loads to Snowflake as a batch
# ─────────────────────────────────────────────────────────────────

def load_batch_to_snowflake(conn, batch):
    """
    Loads a batch of vitals records into Snowflake.
    Uses pandas DataFrame + write_pandas for efficient bulk insert.
    Much faster than inserting row by row.
    """
    if not batch:
        return

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(batch)

    # Add ingestion timestamp
    df["ingested_at"] = datetime.utcnow().isoformat()

    # Convert timestamp string to datetime
    df["reading_timestamp"] = pd.to_datetime(df["reading_timestamp"])

    # Uppercase column names — Snowflake standard
    df.columns = [col.upper() for col in df.columns]

    success, chunks, rows, _ = write_pandas(
        conn, df, "RAW_PATIENT_VITALS"
    )

    if success:
        logger.info(f"Loaded batch of {rows} vitals records to Snowflake")
    else:
        logger.error("Batch load failed — check Snowflake connection")


# ─────────────────────────────────────────────────────────────────
# MAIN CONSUMER LOOP
# Polls Kafka continuously, batches records, loads to Snowflake
# ─────────────────────────────────────────────────────────────────

def consume_and_load(consumer, conn):
    """
    Main loop — polls Kafka for new messages,
    collects into batch, loads to Snowflake when batch is full
    or timeout is reached.
    """
    logger.info("Starting consumer loop — waiting for patient vitals...")
    batch = []
    last_load_time = datetime.utcnow()
    total_records = 0

    try:
        while True:
            # Poll Kafka for new messages
            # timeout_ms=1000 means wait up to 1 second for messages
            messages = consumer.poll(timeout_ms=1000)

            for topic_partition, records in messages.items():
                for record in records:
                    vitals = record.value

                    batch.append(vitals)

                    logger.info(
                        f"Received | Patient: {vitals.get('patient_id')} | "
                        f"Ward: {vitals.get('ward')} | "
                        f"HR: {vitals.get('heart_rate_bpm')} BPM | "
                        f"SpO2: {vitals.get('spo2_pct')}%"
                    )

            # Load batch when full OR timeout reached
            seconds_since_load = (
                datetime.utcnow() - last_load_time
            ).seconds

            if len(batch) >= BATCH_SIZE or seconds_since_load >= BATCH_TIMEOUT_SEC:
                if batch:
                    load_batch_to_snowflake(conn, batch)
                    total_records += len(batch)
                    logger.info(f"Total records loaded so far: {total_records}")
                    batch = []
                    last_load_time = datetime.utcnow()

    except KeyboardInterrupt:
        # Load any remaining records before shutdown
        if batch:
            load_batch_to_snowflake(conn, batch)
        logger.info(f"Consumer stopped. Total records loaded: {total_records}")
        consumer.close()
        conn.close()


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Healthcare Vitals Consumer starting...")

    # Setup Snowflake
    conn = get_snowflake_connection()
    setup_snowflake(conn)

    # Create consumer
    consumer = create_consumer()

    # Start consuming and loading
    consume_and_load(consumer, conn)
