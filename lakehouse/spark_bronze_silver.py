import os
import sys
import json
import boto3
import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer
from deltalake import DeltaTable, write_deltalake

load_dotenv()

# AWS S3 Config
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")

if not S3_BUCKET or not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    print("ERROR: Missing AWS credentials or AWS_S3_BUCKET in .env")
    sys.exit(1)

# Step 1: Poll records from Kafka topic
print("Fetching event stream from Aiven Kafka...")
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVER")
kafka_user = os.getenv("KAFKA_USER")
kafka_password = os.getenv("KAFKA_PASSWORD")
kafka_topic = os.getenv("KAFKA_TOPIC", "orders-stream")
ca_path = os.getenv("KAFKA_CA_LOCATION", "config/ca.pem")

consumer_conf = {
    'bootstrap.servers': kafka_bootstrap,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': kafka_user,
    'sasl.password': kafka_password,
    'ssl.ca.location': ca_path,
    'group.id': 'lakehouse-ingestion-v1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

c = Consumer(consumer_conf)
c.subscribe([kafka_topic])

messages = []
print("Polling events...")
for _ in range(100):
    msg = c.poll(0.5)
    if msg is None:
        continue
    if msg.error():
        continue
    try:
        messages.append(json.loads(msg.value().decode('utf-8')))
    except Exception:
        continue

c.close()

if not messages:
    print("No new messages found in Kafka. Run 'python streaming/producer.py' first.")
    sys.exit(0)

print(f"Retrieved {len(messages)} events from Kafka.")

# Step 2: Write Bronze Layer (Raw Immutable JSON to S3)
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

timestamp_prefix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
bronze_key = f"bronze/orders_stream/batch_{timestamp_prefix}.json"
bronze_body = "\n".join([json.dumps(m) for m in messages])

print(f"Writing raw events to Bronze Layer: s3://{S3_BUCKET}/{bronze_key}...")
s3.put_object(
    Bucket=S3_BUCKET,
    Key=bronze_key,
    Body=bronze_body.encode('utf-8')
)
print("Bronze write completed.")

# Step 3: Transform, Flatten, and Deduplicate for Silver Layer
print("Transforming and flattening records for Silver Layer...")

silver_records = []
seen_event_ids = set()

for m in messages:
    eid = m.get("event_id")
    if not eid or eid in seen_event_ids:
        continue
    seen_event_ids.add(eid)

    payload = m.get("payload", {})
    ts_str = m.get("event_timestamp")
    
    # Parse event date for partitioning
    event_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    event_date_str = event_dt.strftime("%Y-%m-%d")

    silver_records.append({
        "event_id": str(eid),
        "event_timestamp": ts_str,
        "event_date": event_date_str,
        "event_type": str(m.get("event_type")),
        "customer_id": str(m.get("customer_id")),
        "session_id": str(m.get("session_id")),
        "device_type": str(m.get("device_type", "UNKNOWN")),
        "product_id": str(payload.get("product_id") or ""),
        "quantity": int(payload.get("quantity") or 0),
        "unit_price": float(payload.get("unit_price") or 0.0),
        "cart_value": float(payload.get("cart_value") or 0.0),
        "order_id": str(payload.get("order_id") or ""),
        "total_amount": float(payload.get("total_amount") or 0.0),
        "payment_method": str(payload.get("payment_method") or ""),
        "referrer": str(payload.get("referrer") or "")
    })

# Convert to PyArrow Table
arrow_table = pa.Table.from_pylist(silver_records)

# Load the verified region
os.environ["AWS_REGION"] = AWS_REGION
os.environ["AWS_DEFAULT_REGION"] = AWS_REGION

# Step 4: Write to S3 Silver Layer as a True Delta Lake Table
silver_uri = f"s3://{S3_BUCKET}/silver/orders_events"

storage_options = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_REGION": AWS_REGION,
    "region": AWS_REGION,
    "aws_region": AWS_REGION,
    "AWS_ENDPOINT_URL": f"https://s3.{AWS_REGION}.amazonaws.com",
    "endpoint_url": f"https://s3.{AWS_REGION}.amazonaws.com",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "allow_unsafe_rename": "true"
}

print(f"Writing {len(silver_records)} records to Silver Delta Table: {silver_uri}...")
write_deltalake(
    silver_uri,
    arrow_table,
    mode="append",
    partition_by=["event_date"],
    storage_options=storage_options
)
print("Silver Delta write completed.")

# Step 5: Read Delta Log & History to verify ACID metadata
dt = DeltaTable(silver_uri, storage_options=storage_options)
print("\n--- DELTA TABLE ACID METADATA ---")
print(f"Current Delta Version: {dt.version()}")
print(f"Total Data Files     : {len(dt.file_uris())}")

print("\n--- SAMPLE READ VIA DELTA ENGINE ---")
sample_df = dt.to_pandas().head(5)
print(sample_df[["event_id", "event_type", "customer_id", "order_id", "total_amount", "event_date"]])
print("\nLakehouse processing pipeline executed successfully!")

