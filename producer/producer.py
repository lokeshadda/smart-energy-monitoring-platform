#!/usr/bin/env python3
import os
import json
import time
import random
from typing import Dict
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------- Config ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:29092")
TOPIC = os.getenv("TOPIC", "energy.readings")
SPIKE_CITY = os.getenv("SPIKE_CITY", "Miami")
SPIKE_DELAY_SEC = int(os.getenv("SPIKE_DELAY_SEC", "30"))
SPIKE_DURATION_SEC = int(os.getenv("SPIKE_DURATION_SEC", "60"))
HOUSEHOLDS_PER_CITY = int(os.getenv("HOUSEHOLDS_PER_CITY", "20"))

CITIES = ["Tampa", "Orlando", "Miami", "StPete", "Jacksonville"]
APPLIANCES = ["HVAC","Refrigerator","Washer","Dryer","Dishwasher","Oven","Microwave","WaterHeater","Lighting"]

# Spike window (optional), defaults match your earlier logic
SPIKE_CITY = os.getenv("SPIKE_CITY", "Miami")
SPIKE_DELAY_SEC = int(os.getenv("SPIKE_DELAY_SEC", "30"))
SPIKE_DURATION_SEC = int(os.getenv("SPIKE_DURATION_SEC", "60"))
START_EPOCH = int(time.time())
SPIKE_START = START_EPOCH + SPIKE_DELAY_SEC
SPIKE_END = SPIKE_START + SPIKE_DURATION_SEC

HOUSEHOLDS_PER_CITY = int(os.getenv("HOUSEHOLDS_PER_CITY", "20"))
HOUSEHOLDS = {city: [f"{city[:3].upper()}-{i:04d}" for i in range(1, HOUSEHOLDS_PER_CITY + 1)] for city in CITIES}

# ---------- Helpers ----------
def ts() -> int:
    return int(time.time())

def normal_reading(city: str, hh: str) -> Dict:
    return {
        "city": city,
        "household_id": hh,
        "appliance": random.choice(APPLIANCES),
        "watts": round(random.uniform(300, 2200), 2),
        "timestamp": ts(),
        "mode": "normal"            # ← harmless hint (ignored by Spark schema)
    }

def spike_reading(city: str, hh: str) -> Dict:
    watts = round(random.uniform(4000, 6000), 2)
    print(f"*** SPIKE {city} {hh}: {watts}W ***")
    return {
        "city": city,
        "household_id": hh,
        "appliance": random.choice(APPLIANCES),
        "watts": watts,
        "timestamp": ts(),
        "mode": "spike",            # ← indicates spike period
        "anomaly_hint": "SPIKE"     # ← optional; also ignored by Spark schema
    }

def on_delivery_err(err: KafkaError):
    print(f"[producer] delivery error: {err}")

# ---------- Producer ----------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    acks="all",
    retries=10,
    linger_ms=50,
    compression_type="lz4",
    value_serializer=lambda v: json.dumps(v, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

print(f"[producer] broker={KAFKA_BOOTSTRAP} topic={TOPIC}")
print(f"[producer] spike city={SPIKE_CITY} window: +{SPIKE_DELAY_SEC}s for {SPIKE_DURATION_SEC}s")

try:
    while True:
        now = ts()
        spike_active = SPIKE_START <= now < SPIKE_END

        for city in CITIES:
            hh = random.choice(HOUSEHOLDS[city])
            record = spike_reading(city, hh) if (spike_active and city == SPIKE_CITY) else normal_reading(city, hh)

            fut = producer.send(TOPIC, key=city, value=record)
            fut.add_errback(on_delivery_err)

            print("Sent:", record)

        producer.flush()  # flush once per loop
        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped by user.")
finally:
    try:
        producer.flush(5)
    finally:
        producer.close()
