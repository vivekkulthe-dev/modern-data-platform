import os
import sys
import json
import time
import uuid
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

# Kafka configuration using our verified Chapter 1 settings
conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVER"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': os.getenv("KAFKA_USER"),
    'sasl.password': os.getenv("KAFKA_PASSWORD"),
    'ssl.ca.location': os.getenv("KAFKA_CA_LOCATION", "config/ca.pem"),
    'client.id': 'ecommerce-telemetry-producer',
    # Delivery guarantees: retry automatically and ensure idempotent delivery
    'enable.idempotence': True,
    'acks': 'all'
}

topic = os.getenv("KAFKA_TOPIC", "orders-stream")

# Delivery callback invoked once per message upon broker confirmation or failure
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def generate_synthetic_event():
    customer_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    device_type = random.choice(["DESKTOP", "MOBILE", "TABLET"])
    event_type = random.choices(
        ["PAGE_VIEW", "ADD_TO_CART", "CHECKOUT_INITIATED", "CHECKOUT_COMPLETED"],
        weights=[0.65, 0.20, 0.10, 0.05]
    )[0]
    
    payload = {}
    if event_type == "PAGE_VIEW":
        payload = {
            "page_url": f"/products/{random.randint(100, 999)}",
            "referrer": random.choice(["google", "instagram", "direct", "newsletter"])
        }
    elif event_type == "ADD_TO_CART":
        payload = {
            "product_id": str(uuid.uuid4()),
            "quantity": random.randint(1, 3),
            "unit_price": round(random.uniform(15.0, 300.0), 2)
        }
    elif event_type == "CHECKOUT_INITIATED":
        payload = {
            "cart_value": round(random.uniform(30.0, 600.0), 2),
            "item_count": random.randint(1, 5)
        }
    elif event_type == "CHECKOUT_COMPLETED":
        payload = {
            "order_id": str(uuid.uuid4()),
            "total_amount": round(random.uniform(30.0, 600.0), 2),
            "payment_method": random.choice(["CREDIT_CARD", "PAYPAL", "APPLE_PAY"])
        }

    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "customer_id": customer_id,
        "session_id": session_id,
        "device_type": device_type,
        "payload": payload
    }

def run_producer(total_events=200):
    producer = Producer(conf)
    print(f"Starting event emission to topic '{topic}'...")
    print(f"Target count: {total_events} messages")
    
    start_time = time.time()
    for i in range(1, total_events + 1):
        event = generate_synthetic_event()
        # Key on customer_id to guarantee chronological partition ordering per customer
        key_bytes = event["customer_id"].encode('utf-8')
        value_bytes = json.dumps(event).encode('utf-8')

        producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            on_delivery=delivery_report
        )
        producer.poll(0)  # Serve delivery callbacks

        if i % 50 == 0 or i == total_events:
            print(f"  --> Produced {i}/{total_events} events...")
        time.sleep(0.02)  # Emit at ~50 messages/sec rate

    print("Flushing buffer to Kafka broker...")
    producer.flush(10)
    duration = time.time() - start_time
    print(f"Producer finished: {total_events} events emitted in {duration:.2f}s ({total_events/duration:.1f} events/sec).")

if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    run_producer(count)
