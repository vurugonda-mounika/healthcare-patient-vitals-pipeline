-- mart_patient_health_summary.sql
-- PURPOSE: Business-ready clinical analytics from cleaned patient vitals
--
-- Creates two analytical views:
-- 1. Ward-level alert summary — which wards have most critical patients
-- 2. Patient-level health summary — latest vitals + overall status per patient
--
-- In real hospitals: this layer feeds clinical dashboards,
-- charge nurse screens, and hospital operations reports
-- Same staging → mart pattern used at LetQuickly for property analytics

WITH staged_vitals AS (

    -- Read from staging model — clean, validated, flagged data
    SELECT * FROM {{ ref('stg_patient_vitals') }}

),

-- ─────────────────────────────────────────────────────────────────
-- PART 1: LATEST READING PER PATIENT
-- For each patient, get their most recent vitals only
-- We do not want old readings affecting current health status
-- ─────────────────────────────────────────────────────────────────

latest_per_patient AS (

    SELECT
        patient_id,
        patient_name,
        age,
        ward,
        patient_condition,
        doctor_id,
        heart_rate_bpm,
        systolic_bp_mmhg,
        diastolic_bp_mmhg,
        spo2_pct,
        temperature_celsius,
        respiratory_rate,
        heart_rate_status,
        bp_status,
        spo2_status,
        temperature_status,
        has_alert,
        reading_timestamp,
        -- ROW_NUMBER assigns 1 to the most recent reading per patient
        -- We filter to row_num = 1 below to get latest only
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY reading_timestamp DESC
        ) AS row_num
    FROM staged_vitals

),

-- ─────────────────────────────────────────────────────────────────
-- PART 2: PATIENT HEALTH SUMMARY
-- One row per patient — their current status based on latest reading
-- ─────────────────────────────────────────────────────────────────

patient_summary AS (

    SELECT
        patient_id,
        patient_name,
        age,
        ward,
        patient_condition,
        doctor_id,

        -- Latest vital readings
        heart_rate_bpm          AS current_heart_rate_bpm,
        systolic_bp_mmhg        AS current_systolic_bp,
        diastolic_bp_mmhg       AS current_diastolic_bp,
        spo2_pct                AS current_spo2_pct,
        temperature_celsius     AS current_temperature,
        respiratory_rate        AS current_respiratory_rate,

        -- Individual vital statuses
        heart_rate_status,
        bp_status,
        spo2_status,
        temperature_status,

        -- Overall patient risk level
        -- Based on combination of abnormal vitals
        CASE
            WHEN spo2_pct < 90
              OR (heart_rate_bpm < 40 OR heart_rate_bpm > 130)
              OR systolic_bp_mmhg >= 180
              OR systolic_bp_mmhg < 80
            THEN 'CRITICAL'
            WHEN has_alert = TRUE
            THEN 'WARNING'
            ELSE 'STABLE'
        END                     AS patient_risk_level,

        -- How many vital signs are currently abnormal
        (CASE WHEN heart_rate_status != 'NORMAL' THEN 1 ELSE 0 END +
         CASE WHEN bp_status         != 'NORMAL' THEN 1 ELSE 0 END +
         CASE WHEN spo2_status       != 'NORMAL' THEN 1 ELSE 0 END +
         CASE WHEN temperature_status != 'NORMAL' THEN 1 ELSE 0 END
        )                       AS abnormal_vitals_count,

        reading_timestamp       AS last_reading_at,

        -- How many minutes since last reading
        -- If this is high — monitor may be disconnected
        DATEDIFF(
            'minute',
            reading_timestamp,
            CURRENT_TIMESTAMP()
        )                       AS minutes_since_last_reading

    FROM latest_per_patient
    WHERE row_num = 1   -- latest reading only

),

-- ─────────────────────────────────────────────────────────────────
-- PART 3: WARD ALERT SUMMARY
-- Aggregated by ward — how many alerts, avg vitals, critical count
-- Hospital operations teams look at ward-level summaries
-- ─────────────────────────────────────────────────────────────────

ward_summary AS (

    SELECT
        ward,
        DATE_TRUNC('hour', reading_timestamp)   AS reading_hour,

        -- Volume
        COUNT(*)                                AS total_readings,
        COUNT(DISTINCT patient_id)              AS unique_patients,

        -- Alert counts
        SUM(CASE WHEN has_alert THEN 1 ELSE 0 END)  AS total_alerts,
        ROUND(
            100.0 * SUM(CASE WHEN has_alert THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0), 1
        )                                       AS alert_rate_pct,

        -- Critical vital counts
        SUM(CASE WHEN heart_rate_status = 'TACHYCARDIA' THEN 1 ELSE 0 END) AS tachycardia_count,
        SUM(CASE WHEN heart_rate_status = 'BRADYCARDIA' THEN 1 ELSE 0 END) AS bradycardia_count,
        SUM(CASE WHEN spo2_status = 'CRITICAL_LOW'      THEN 1 ELSE 0 END) AS critical_spo2_count,
        SUM(CASE WHEN bp_status = 'HYPERTENSIVE_CRISIS' THEN 1 ELSE 0 END) AS hypertensive_crisis_count,

        -- Average vitals for the ward in this hour
        ROUND(AVG(heart_rate_bpm), 1)           AS avg_heart_rate,
        ROUND(AVG(systolic_bp_mmhg), 1)         AS avg_systolic_bp,
        ROUND(AVG(spo2_pct), 1)                 AS avg_spo2,
        ROUND(AVG(temperature_celsius), 2)      AS avg_temperature,

        -- Pipeline freshness
        MAX(reading_timestamp)                  AS latest_reading_at,
        MIN(reading_timestamp)                  AS earliest_reading_at

    FROM staged_vitals
    GROUP BY
        ward,
        DATE_TRUNC('hour', reading_timestamp)

),

-- ─────────────────────────────────────────────────────────────────
-- FINAL OUTPUT
-- Join patient summary with ward context for complete picture
-- ─────────────────────────────────────────────────────────────────

final AS (

    SELECT
        -- Patient details
        ps.patient_id,
        ps.patient_name,
        ps.age,
        ps.ward,
        ps.patient_condition,
        ps.doctor_id,

        -- Current vitals
        ps.current_heart_rate_bpm,
        ps.current_systolic_bp,
        ps.current_diastolic_bp,
        ps.current_spo2_pct,
        ps.current_temperature,
        ps.current_respiratory_rate,

        -- Status flags
        ps.heart_rate_status,
        ps.bp_status,
        ps.spo2_status,
        ps.temperature_status,
        ps.patient_risk_level,
        ps.abnormal_vitals_count,

        -- Timing
        ps.last_reading_at,
        ps.minutes_since_last_reading,

        -- Ward context — how busy/critical is this patient's ward?
        ws.total_readings           AS ward_readings_this_hour,
        ws.alert_rate_pct           AS ward_alert_rate_pct,
        ws.avg_heart_rate           AS ward_avg_heart_rate,
        ws.avg_spo2                 AS ward_avg_spo2

    FROM patient_summary ps
    LEFT JOIN ward_summary ws
        ON  ps.ward = ws.ward
        AND ws.reading_hour = DATE_TRUNC('hour', CURRENT_TIMESTAMP())

    -- Most critical patients first
    ORDER BY
        CASE ps.patient_risk_level
            WHEN 'CRITICAL' THEN 1
            WHEN 'WARNING'  THEN 2
            WHEN 'STABLE'   THEN 3
            ELSE 4
        END,
        ps.abnormal_vitals_count DESC

)

SELECT * FROM final
