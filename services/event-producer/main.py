import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer
import h3

KAFKA_CONFIG = {"bootstrap.servers": "kafka:9092"}
TOPIC = "rides.requested.madrid"
MADRID_CENTER = (40.4168, -3.7038)

def get_producer():
    """Retries connection until Kafka is available"""
    while True:
        try:
            p = Producer(KAFKA_CONFIG)
            # Push a test poll to check connectivity
            p.poll(0)
            print("Successfully connected to Kafka")
            return p
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(3)

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[PRODUCED] Partition: {msg.partition()}")

producer = get_producer()

try:
    while True:
        lat = MADRID_CENTER[0] + random.uniform(-0.02, 0.02)
        lng = MADRID_CENTER[1] + random.uniform(-0.02, 0.02)
        
        # Correct H3 v4 function
        h3_index = h3.latlng_to_cell(lat, lng, 8)

        event = {
            "event_id": str(random.randint(1000, 9999)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "h3_res8": h3_index,
        }

        producer.produce(
            topic=TOPIC,
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    producer.flush()