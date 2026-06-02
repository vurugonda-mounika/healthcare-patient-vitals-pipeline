-- stg_patient_vitals.sql
-- PURPOSE: Clean, validate, and standardize raw patient vitals
--          from Kafka streaming pipeline
--
-- This is the STAGING layer — first transformation step
-- Raw data from Kafka has no guarantees — we enforce quality here
-- before any clinical logic runs in the mart layer
--
-- In real healthcare systems: data quality at this layer
-- is critical — wrong vitals can affect clinical decisions

WITH raw_vitals AS (

    -- Read from raw Snowflake table loaded by Kafka consumer
    SELECT * FROM {{ source('healthcare_raw', 'RAW_PATIENT_VITALS') }}

),

validated AS (

    SELECT
        -- ── Patient identifiers ──────────────────────────────────
        TRIM(PATIENT_ID)                                AS patient_id,
        TRIM(PATIENT_NAME)                              AS patient_name,

        -- Validate age — must be between 0 and 120
        -- TRY_CAST returns NULL for invalid values instead of crashing
        CASE
            WHEN AGE BETWEEN 0 AND 120 THEN AGE
            ELSE NULL
        END                                             AS age,

        TRIM(WARD)                                      AS ward,
        TRIM(CONDITION)                                 AS patient_condition,
        TRIM(DOCTOR_ID)                                 AS doctor_id,

        -- ── Vital signs with clinical range validation ────────────
        -- Normal ranges based on standard clinical guidelines
        -- Out-of-range values are kept but flagged — not removed
        -- Removing data could hide real critical readings

        -- Heart rate: valid range 0-300 BPM
        -- Below 0 or above 300 = sensor error, not clinical reading
        CASE
            WHEN HEART_RATE_BPM BETWEEN 0 AND 300 THEN HEART_RATE_BPM
            ELSE NULL
        END                                             AS heart_rate_bpm,

        -- Blood pressure: valid systolic range 50-250 mmHg
        CASE
            WHEN SYSTOLIC_BP_MMHG BETWEEN 50 AND 250 THEN SYSTOLIC_BP_MMHG
            ELSE NULL
        END                                             AS systolic_bp_mmhg,

        -- Diastolic BP: valid range 30-150 mmHg
        CASE
            WHEN DIASTOLIC_BP_MMHG BETWEEN 30 AND 150 THEN DIASTOLIC_BP_MMHG
            ELSE NULL
        END                                             AS diastolic_bp_mmhg,

        -- SpO2: valid range 50-100%
        -- Below 50% = sensor disconnected, not real reading
        CASE
            WHEN SPO2_PCT BETWEEN 50 AND 100 THEN SPO2_PCT
            ELSE NULL
        END                                             AS spo2_pct,

        -- Temperature: valid range 30-45 Celsius
        CASE
            WHEN TEMPERATURE_CELSIUS BETWEEN 30.0 AND 45.0
            THEN ROUND(TEMPERATURE_CELSIUS, 1)
            ELSE NULL
        END                                             AS temperature_celsius,

        -- Respiratory rate: valid range 0-60 breaths/min
        CASE
            WHEN RESPIRATORY_RATE BETWEEN 0 AND 60 THEN RESPIRATORY_RATE
            ELSE NULL
        END                                             AS respiratory_rate,

        -- ── Clinical flags ────────────────────────────────────────
        -- Flag abnormal readings based on standard clinical thresholds
        -- These flags are used in the mart layer for alert logic
        -- In real systems: these trigger nurse alerts or EHR notifications

        -- Tachycardia (high HR) or Bradycardia (low HR)
        CASE
            WHEN HEART_RATE_BPM > 100 THEN 'TACHYCARDIA'
            WHEN HEART_RATE_BPM < 60  THEN 'BRADYCARDIA'
            ELSE 'NORMAL'
        END                                             AS heart_rate_status,

        -- Blood pressure classification
        CASE
            WHEN SYSTOLIC_BP_MMHG >= 180 THEN 'HYPERTENSIVE_CRISIS'
            WHEN SYSTOLIC_BP_MMHG >= 130 THEN 'HIGH'
            WHEN SYSTOLIC_BP_MMHG < 90   THEN 'LOW'
            ELSE 'NORMAL'
        END                                             AS bp_status,

        -- Oxygen saturation status
        CASE
            WHEN SPO2_PCT < 90  THEN 'CRITICAL_LOW'
            WHEN SPO2_PCT < 95  THEN 'LOW'
            ELSE 'NORMAL'
        END                                             AS spo2_status,

        -- Temperature status
        CASE
            WHEN TEMPERATURE_CELSIUS >= 38.0 THEN 'FEVER'
            WHEN TEMPERATURE_CELSIUS < 36.0  THEN 'HYPOTHERMIA'
            ELSE 'NORMAL'
        END                                             AS temperature_status,

        -- Overall alert flag — TRUE if ANY vital is abnormal
        -- Used in mart layer to count alerts per ward
        CASE
            WHEN HEART_RATE_BPM > 100 OR HEART_RATE_BPM < 60   THEN TRUE
            WHEN SYSTOLIC_BP_MMHG >= 180 OR SYSTOLIC_BP_MMHG < 90 THEN TRUE
            WHEN SPO2_PCT < 95                                   THEN TRUE
            WHEN TEMPERATURE_CELSIUS >= 38.0                     THEN TRUE
            ELSE FALSE
        END                                             AS has_alert,

        -- ── Timestamps ────────────────────────────────────────────
        READING_TIMESTAMP                               AS reading_timestamp,
        DATE_TRUNC('hour', READING_TIMESTAMP)           AS reading_hour,
        DATE_TRUNC('day',  READING_TIMESTAMP)           AS reading_date,
        INGESTED_AT                                     AS ingested_at,

        -- Pipeline latency — how long from reading to ingestion
        -- In production: monitor this to detect pipeline slowdowns
        DATEDIFF(
            'second',
            READING_TIMESTAMP,
            INGESTED_AT
        )                                               AS ingestion_lag_seconds,

        -- ── Metadata ──────────────────────────────────────────────
        DEVICE_ID                                       AS device_id,
        HOSPITAL_ID                                     AS hospital_id,
        DATA_SOURCE                                     AS data_source

    FROM raw_vitals

    -- Remove records with no patient ID — unusable
    WHERE PATIENT_ID IS NOT NULL
    AND   TRIM(PATIENT_ID) != ''

    -- Remove records with no timestamp — cannot place in time
    AND   READING_TIMESTAMP IS NOT NULL

)

SELECT * FROM validated
