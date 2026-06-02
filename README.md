# Healthcare Patient Vitals — Real-Time Streaming Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-231F20)
![Snowflake](https://img.shields.io/badge/Snowflake-Snowpipe-29B5E8)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF6F3C)
![SQL](https://img.shields.io/badge/SQL-Analytics-green)

## What This Project Does
Builds a real-time patient vitals streaming pipeline that simulates
IoT patient monitors sending heart rate, blood pressure, and oxygen
saturation data through Apache Kafka into Snowflake via Snowpipe,
then transforms the data using dbt into clinical analytics summaries.

## Why I Built This
In my professional work I have built real-time IoT streaming pipelines
processing sensor data from 5,000+ smart bins using Apache Kafka and
Azure Databricks. This project applies the same streaming architecture
to a healthcare domain — demonstrating how real-time pipeline patterns
translate across industries.

## Architecture
Python Producer (simulates patient monitors)
        ↓
Apache Kafka Topic — patient-vitals
        ↓
Python Consumer (reads from Kafka topic)
        ↓
Snowflake Stage + Snowpipe (auto-ingest in real-time)
        ↓
dbt staging model — cleans and standardizes vitals
        ↓
dbt mart model — patient health summaries and alerts
        ↓
Analytics-ready clinical data

## Tech Stack
| Tool | Purpose |
|------|---------|
| Python | Kafka producer and consumer scripts |
| Apache Kafka | Real-time message streaming |
| Snowflake Snowpipe | Auto-ingest streaming data into warehouse |
| dbt Core | SQL transformations and data quality tests |
| SQL | Data modeling and clinical aggregations |

## Project Structure
healthcare-patient-vitals-pipeline/
├── README.md
├── .gitignore
├── dbt_project.yml
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── snowflake/
│   └── setup.sql
├── models/
│   ├── staging/
│   │   └── stg_patient_vitals.sql
│   ├── marts/
│   │   └── mart_patient_health_summary.sql
│   └── schema.yml
└── tests/
    └── test_valid_heart_rate.sql

## Key Features
- Real-time patient vitals streaming via Apache Kafka
- Automatic ingestion into Snowflake using Snowpipe
- Modular dbt models — staging to mart pattern
- Clinical data quality tests — heart rate, BP, SpO2 ranges
- Simulated patient monitor data — no real patient data used

## Data Simulated
- Heart rate (BPM) — normal range 60-100
- Systolic blood pressure (mmHg) — normal range 90-120
- Diastolic blood pressure (mmHg) — normal range 60-80
- Oxygen saturation SpO2 (%) — normal range 95-100
- Patient ID, ward, timestamp

## How To Run This Project

Step 1 — Clone the repository
git clone https://github.com/vurugonda-mounika/healthcare-patient-vitals-pipeline.git

Step 2 — Install dependencies
pip install kafka-python snowflake-connector-python dbt-snowflake faker

Step 3 — Start Kafka locally
docker-compose up -d zookeeper kafka

Step 4 — Run the Kafka producer
python kafka/producer.py

Step 5 — Run the Kafka consumer
python kafka/consumer.py

Step 6 — Run dbt transformations
dbt run
dbt test

## Results
- Real-time vitals streamed through Kafka at sub-second latency
- Snowpipe auto-ingests data into Snowflake within seconds
- Staging layer standardizes all vital sign columns and data types
- Mart layer flags abnormal readings and summarizes by patient and ward
- All dbt data quality tests passing

## Important Note
All patient data in this project is 100% simulated using Python
Faker library. No real patient data is used anywhere.

## Author
Mounika Vurugonda
