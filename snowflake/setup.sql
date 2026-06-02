-- setup.sql
-- PURPOSE: Complete Snowflake infrastructure setup for the
--          healthcare patient vitals streaming pipeline
--
-- Run this ONCE before starting the Kafka consumer
-- All statements are idempotent — safe to run multiple times
-- IF NOT EXISTS means it never fails on re-run
--
-- In real work: this script would be run by a DevOps/DataOps engineer
-- during environment setup. Same pattern used at enterprise level.

-- ─────────────────────────────────────────────────────────────────
-- STEP 1: DATABASE AND WAREHOUSE SETUP
-- ─────────────────────────────────────────────────────────────────

-- Create dedicated database for healthcare raw data
-- Separating raw data into its own database is best practice
-- Prevents accidental mixing with transformed data
CREATE DATABASE IF NOT EXISTS HEALTHCARE_RAW
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw healthcare streaming data — patient vitals from Kafka pipeline';

USE DATABASE HEALTHCARE_RAW;

-- Create schema for raw ingestion layer
CREATE SCHEMA IF NOT EXISTS PUBLIC
    COMMENT = 'Raw layer — unmodified data as received from Kafka consumer';

-- Create dedicated virtual warehouse for pipeline workloads
-- Separate warehouse = separate compute = no contention with other queries
-- AUTO_SUSPEND saves cost when pipeline is idle
-- AUTO_RESUME starts automatically when queries arrive
CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_WH
    WAREHOUSE_SIZE    = 'X-SMALL'   -- sufficient for streaming ingestion
    AUTO_SUSPEND      = 60          -- suspend after 60 seconds idle
    AUTO_RESUME       = TRUE
    COMMENT           = 'Dedicated warehouse for healthcare streaming pipeline';

USE WAREHOUSE HEALTHCARE_WH;


-- ─────────────────────────────────────────────────────────────────
-- STEP 2: RAW TABLE
-- Stores all incoming patient vitals exactly as received from Kafka
-- No transformations — dbt handles all transformation logic
-- ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS HEALTHCARE_RAW.PUBLIC.RAW_PATIENT_VITALS (
    -- Patient identifiers
    PATIENT_ID              VARCHAR(20)     NOT NULL,
    PATIENT_NAME            VARCHAR(100),
    AGE                     INTEGER,
    WARD                    VARCHAR(50),
    CONDITION               VARCHAR(50),
    DOCTOR_ID               VARCHAR(20),

    -- Vital signs — stored as raw values, dbt will validate ranges
    HEART_RATE_BPM          INTEGER,
    SYSTOLIC_BP_MMHG        INTEGER,
    DIASTOLIC_BP_MMHG       INTEGER,
    SPO2_PCT                INTEGER,
    TEMPERATURE_CELSIUS     FLOAT,
    RESPIRATORY_RATE        INTEGER,

    -- Metadata
    READING_TIMESTAMP       TIMESTAMP_NTZ,
    DEVICE_ID               VARCHAR(50),
    HOSPITAL_ID             VARCHAR(20),
    DATA_SOURCE             VARCHAR(50),

    -- Pipeline audit columns
    -- These are added by the consumer, not the source system
    INGESTED_AT             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    KAFKA_PARTITION         INTEGER,
    KAFKA_OFFSET            BIGINT
)
COMMENT = 'Raw patient vitals from Kafka — one row per monitor reading';


-- ─────────────────────────────────────────────────────────────────
-- STEP 3: SNOWPIPE SETUP
-- Snowpipe is Snowflake auto-ingestion — loads data automatically
-- as soon as files arrive in the Snowflake stage
-- This is the real-time ingestion mechanism
-- ─────────────────────────────────────────────────────────────────

-- Create an internal stage
-- Stage = temporary landing zone for files before loading into table
-- Think of it like an inbox — files arrive here, Snowpipe picks them up
CREATE STAGE IF NOT EXISTS HEALTHCARE_RAW.PUBLIC.VITALS_STAGE
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = TRUE    -- handles JSON arrays correctly
        IGNORE_UTF8_ERRORS = TRUE   -- tolerant of encoding issues
    )
    COMMENT = 'Staging area for patient vitals JSON files from Kafka consumer';

-- Create Snowpipe
-- Snowpipe watches the stage and auto-loads new files into the table
-- ON_ERROR = CONTINUE means bad records are skipped, not blocking the pipe
-- In production: failed records go to an error table for investigation
CREATE PIPE IF NOT EXISTS HEALTHCARE_RAW.PUBLIC.VITALS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingests patient vitals JSON from stage into RAW_PATIENT_VITALS'
AS
COPY INTO HEALTHCARE_RAW.PUBLIC.RAW_PATIENT_VITALS (
    PATIENT_ID, PATIENT_NAME, AGE, WARD, CONDITION, DOCTOR_ID,
    HEART_RATE_BPM, SYSTOLIC_BP_MMHG, DIASTOLIC_BP_MMHG,
    SPO2_PCT, TEMPERATURE_CELSIUS, RESPIRATORY_RATE,
    READING_TIMESTAMP, DEVICE_ID, HOSPITAL_ID, DATA_SOURCE
)
FROM (
    SELECT
        $1:patient_id::VARCHAR,
        $1:patient_name::VARCHAR,
        $1:age::INTEGER,
        $1:ward::VARCHAR,
        $1:condition::VARCHAR,
        $1:doctor_id::VARCHAR,
        $1:heart_rate_bpm::INTEGER,
        $1:systolic_bp_mmhg::INTEGER,
        $1:diastolic_bp_mmhg::INTEGER,
        $1:spo2_pct::INTEGER,
        $1:temperature_celsius::FLOAT,
        $1:respiratory_rate::INTEGER,
        $1:reading_timestamp::TIMESTAMP_NTZ,
        $1:device_id::VARCHAR,
        $1:hospital_id::VARCHAR,
        $1:data_source::VARCHAR
    FROM @HEALTHCARE_RAW.PUBLIC.VITALS_STAGE
)
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = CONTINUE;


-- ─────────────────────────────────────────────────────────────────
-- STEP 4: MONITORING QUERIES
-- Use these to monitor pipeline health in production
-- Run these to verify data is flowing correctly
-- ─────────────────────────────────────────────────────────────────

-- Check Snowpipe status — is it running?
-- SHOW PIPES;

-- Check how many records loaded and when
-- SELECT COUNT(*), MAX(INGESTED_AT)
-- FROM HEALTHCARE_RAW.PUBLIC.RAW_PATIENT_VITALS;

-- Check latest vitals per patient
-- SELECT PATIENT_ID, WARD, HEART_RATE_BPM, SPO2_PCT, READING_TIMESTAMP
-- FROM HEALTHCARE_RAW.PUBLIC.RAW_PATIENT_VITALS
-- ORDER BY READING_TIMESTAMP DESC
-- LIMIT 20;

-- Check for critical patients with abnormal readings
-- SELECT PATIENT_ID, WARD, CONDITION,
--        HEART_RATE_BPM, SPO2_PCT, SYSTOLIC_BP_MMHG
-- FROM HEALTHCARE_RAW.PUBLIC.RAW_PATIENT_VITALS
-- WHERE HEART_RATE_BPM < 60 OR HEART_RATE_BPM > 100
--    OR SPO2_PCT < 95
-- ORDER BY READING_TIMESTAMP DESC;

-- Check pipeline lag — how fresh is the data?
-- SELECT DATEDIFF('second', MAX(READING_TIMESTAMP), CURRENT_TIMESTAMP()) AS lag_seconds
-- FROM HEALTHCARE_RAW.PUBLIC.RAW_PATIENT_VITALS;


-- ─────────────────────────────────────────────────────────────────
-- STEP 5: GRANTS (for team environments)
-- In production: different roles have different access levels
-- Data engineers can read/write, analysts can only read
-- ─────────────────────────────────────────────────────────────────

-- Example grants — uncomment in team environment
-- GRANT USAGE ON DATABASE HEALTHCARE_RAW TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON SCHEMA HEALTHCARE_RAW.PUBLIC TO ROLE DATA_ENGINEER;
-- GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA HEALTHCARE_RAW.PUBLIC TO ROLE DATA_ENGINEER;
-- GRANT SELECT ON ALL TABLES IN SCHEMA HEALTHCARE_RAW.PUBLIC TO ROLE ANALYST;
