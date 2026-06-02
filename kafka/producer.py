# producer.py
# PURPOSE: Simulates hospital patient monitoring devices sending
#          real-time vitals data into Apache Kafka
#
# In real healthcare systems: patient monitors send HL7/FHIR messages
# to a message broker every few seconds. This script simulates that
# exact pattern using realistic vital sign ranges.
#
# In my Cigniti work: same pattern — IoT smart bins sent telemetry
# (fill levels, GPS, temperature) to Kafka at sub-minute latency.
# This demonstrates identical streaming architecture in healthcare domain.

import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# ─────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

# Kafka configuration
# In production: broker address comes from environment variable
# Here: localhost for local development
KAFKA_BROKER   = "localhost:9092"
KAFKA_TOPIC    = "patient-vitals"
SEND_INTERVAL  = 2   # seconds between readings — simulates real monitor frequency

# Hospital wards — realistic hospital structure
WARDS = [
    "ICU",
    "Emergency",
    "Cardiology",
    "General Ward A",
    "General Ward B",
    "Pediatrics"
]

# ─────────────────────────────────────────────────────────────────
# PATIENT REGISTRY
# Simulates a fixed set of admitted patients
# In real work: this would come from the hospital EMR system (Epic/Cerner)
# ─────────────────────────────────────────────────────────────────

def generate_patient_registry(num_patients=20):
    """
    Creates a simulated registry of admitted patients.
    Each patient has a fixed ID, name, age, ward, and condition.
    Vitals vary based on their condition — makes data realistic.
    """
    patients = []
    conditions = ["Stable", "Critical", "Recovering", "Under Observation"]

    for i in range(num_patients):
        patients.append({
            "patient_id":  f"PAT-{str(i+1).zfill(4)}",
            "patient_name": fake.name(),
            "age":         random.randint(18, 90),
            "ward":        random.choice(WARDS),
            "condition":   random.choice(conditions),
            "doctor_id":   f"DR-{str(random.randint(1,10)).zfill(3)}"
        })

    logger.info(f"Generated registry of {num_patients} patients")
    return patients


# ─────────────────────────────────────────────────────────────────
# VITALS GENERATOR
# Generates realistic vital signs based on patient condition
# Critical patients have abnormal readings — tests our alert logic
# ─────────────────────────────────────────────────────────────────

def generate_vitals(patient):
    """
    Generates realistic vital signs for a patient.
    Condition affects the ranges — critical patients may have
    abnormal readings that our dbt mart model will flag as alerts.

    Normal ranges (clinical standards):
    - Heart rate:         60-100 BPM
    - Systolic BP:        90-120 mmHg
    - Diastolic BP:       60-80  mmHg
    - SpO2 (oxygen):      95-100 %
    - Temperature:        36.1-37.2 C
    - Respiratory rate:   12-20 breaths/min
    """
    condition = patient["condition"]

    if condition == "Critical":
        # Critical patients have abnormal vitals
        heart_rate        = random.randint(40, 140)   # dangerously low or high
        systolic_bp       = random.randint(70, 180)   # hypotension or hypertension
        diastolic_bp      = random.randint(40, 110)
        spo2              = random.randint(85, 99)    # low oxygen
        temperature       = round(random.uniform(35.0, 39.5), 1)
        respiratory_rate  = random.randint(8, 30)

    elif condition == "Stable":
        # Stable patients have normal vitals with minor variation
        heart_rate        = random.randint(60, 100)
        systolic_bp       = random.randint(110, 130)
        diastolic_bp      = random.randint(70, 85)
        spo2              = random.randint(96, 100)
        temperature       = round(random.uniform(36.1, 37.2), 1)
        respiratory_rate  = random.randint(12, 18)

    else:
        # Recovering / Under Observation — slightly off normal
        heart_rate        = random.randint(55, 110)
        systolic_bp       = random.randint(100, 145)
        diastolic_bp      = random.randint(60, 95)
        spo2              = random.randint(93, 100)
        temperature       = round(random.uniform(36.0, 38.0), 1)
        respiratory_rate  = random.randint(10, 22)

    return {
        "heart_rate_bpm":       heart_rate,
        "systolic_bp_mmhg":     systolic_bp,
        "diastolic_bp_mmhg":    diastolic_bp,
        "spo2_pct":             spo2,
        "temperature_celsius":  temperature,
        "respiratory_rate":     respiratory_rate
    }


# ─────────────────────────────────────────────────────────────────
# KAFKA PRODUCER
# Serializes patient vitals as JSON and sends to Kafka topic
# In production: same pattern — serialize to JSON or Avro, send to topic
# ─────────────────────────────────────────────────────────────────

def create_producer():
    """
    Creates and returns a Kafka producer.
    value_serializer converts Python dict to JSON bytes automatically.
    In production: would use Avro schema for stronger typing.
    """
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",           # wait for all replicas to confirm — data safety
        retries=3,            # retry on failure — production reliability pattern
        linger_ms=10          # slight batching for efficiency
    )
    logger.info(f"Kafka producer connected to {KAFKA_BROKER}")
    return producer


def stream_vitals(producer, patients):
    """
    Main streaming loop.
    Continuously picks a random patient, generates their vitals,
    builds a complete message, and sends it to Kafka.
    Runs indefinitely — press Ctrl+C to stop.
    """
    logger.info(f"Starting vitals stream to topic: {KAFKA_TOPIC}")
    logger.info(f"Sending readings every {SEND_INTERVAL} seconds...")
    messages_sent = 0

    try:
        while True:
            # Pick a random patient from the registry
            patient = random.choice(patients)

            # Generate their current vitals
            vitals = generate_vitals(patient)

            # Build complete message — patient info + vitals + timestamp
            message = {
                # Patient identifiers
                "patient_id":         patient["patient_id"],
                "patient_name":       patient["patient_name"],
                "age":                patient["age"],
                "ward":               patient["ward"],
                "condition":          patient["condition"],
                "doctor_id":          patient["doctor_id"],

                # Vital signs
                "heart_rate_bpm":     vitals["heart_rate_bpm"],
                "systolic_bp_mmhg":   vitals["systolic_bp_mmhg"],
                "diastolic_bp_mmhg":  vitals["diastolic_bp_mmhg"],
                "spo2_pct":           vitals["spo2_pct"],
                "temperature_celsius":vitals["temperature_celsius"],
                "respiratory_rate":   vitals["respiratory_rate"],

                # Metadata
                "reading_timestamp":  datetime.utcnow().isoformat(),
                "device_id":          f"MONITOR-{patient['patient_id']}",
                "hospital_id":        "HOSP-001",
                "data_source":        "simulated_patient_monitor"
            }

            # Send to Kafka — patient_id as key ensures
            # same patient always goes to same partition
            producer.send(
                KAFKA_TOPIC,
                key=patient["patient_id"],
                value=message
            )

            messages_sent += 1
            logger.info(
                f"Sent reading {messages_sent} | "
                f"Patient: {patient['patient_id']} | "
                f"Ward: {patient['ward']} | "
                f"HR: {vitals['heart_rate_bpm']} BPM | "
                f"SpO2: {vitals['spo2_pct']}%"
            )

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        logger.info(f"Stream stopped. Total messages sent: {messages_sent}")
        producer.flush()
        producer.close()


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Healthcare Patient Vitals Producer starting...")
    logger.info("NOTE: All data is simulated. No real patient data used.")

    # Create patient registry
    patients = generate_patient_registry(num_patients=20)

    # Create Kafka producer
    producer = create_producer()

    # Start streaming
    stream_vitals(producer, patients)
