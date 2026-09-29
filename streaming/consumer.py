import os
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVER"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': os.getenv("KAFKA_USER"),
    'sasl.password': os.getenv("KAFKA_PASSWORD"),
    'ssl.ca.location': os.getenv("KAFKA_CA_LOCATION", "config/ca.pem"),
    'group.id': 'telemetry-audit-worker-group-v2',  # Fresh consumer group
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

topic = os.getenv("KAFKA_TOPIC", "orders-stream")

def run_consumer(max_messages=200):
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    print(f"Consumer subscribed to '{topic}'. Waiting for partition assignment...")

    counts_by_type = {}
    latencies = []
    messages_processed = 0
    empty_polls = 0
    max_empty_polls = 10  # Allows up to 10 seconds for initial connection and drain

    try:
        while messages_processed < max_messages:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                empty_polls += 1
                if empty_polls > max_empty_polls:
                    print("Reached poll timeout limit. No further messages available.")
                    break
                continue

            # Reset idle poll counter upon receiving data
            empty_polls = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            try:
                data = json.loads(msg.value().decode('utf-8'))
                event_type = data.get("event_type", "UNKNOWN")
                counts_by_type[event_type] = counts_by_type.get(event_type, 0) + 1

                # Transit latency calculation
                produced_at = datetime.fromisoformat(data["event_timestamp"])
                now = datetime.now(timezone.utc)
                latency_ms = (now - produced_at).total_seconds() * 1000.0
                latencies.append(latency_ms)

                messages_processed += 1
                if messages_processed % 50 == 0 or messages_processed == max_messages:
                    print(f"  [Offset {msg.offset()}] Ingested {messages_processed}/{max_messages} records...")

                consumer.commit(message=msg, asynchronous=False)

            except Exception as e:
                print(f"Malformed payload at offset {msg.offset()}: {e}")

    finally:
        consumer.close()

    print("\n" + "=" * 60)
    print("STREAM CONSUMPTION AUDIT SUMMARY")
    print("=" * 60)
    print(f"Total Messages Ingested: {messages_processed}")
    print("Breakdown by Event Type:")
    for etype, count in counts_by_type.items():
        print(f"  - {etype}: {count}")
    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        print(f"Average Transit Latency : {avg_latency:.2f} ms")
    print("=" * 60)

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    run_consumer(limit)
