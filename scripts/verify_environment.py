import os
import sys
import psycopg2
import boto3
from confluent_kafka.admin import AdminClient
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def test_neon():
    print("[1/4] Checking Neon PostgreSQL...", end=" ")
    try:
        conn = psycopg2.connect(
            host=os.getenv("NEON_HOST"),
            port=os.getenv("NEON_PORT"),
            dbname=os.getenv("NEON_DATABASE"),
            user=os.getenv("NEON_USER"),
            password=os.getenv("NEON_PASSWORD"),
            sslmode=os.getenv("NEON_SSLMODE", "require")
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        v = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"PASSED ({v[:21]})")
        return True
    except Exception as e:
        print(f"FAILED: {e}")
        return False

def test_aiven_kafka():
    print("[2/4] Checking Aiven Kafka Broker...", end=" ")
    try:
        conf = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVER"),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'SCRAM-SHA-256',
            'sasl.username': os.getenv("KAFKA_USER"),
            'sasl.password': os.getenv("KAFKA_PASSWORD"),
        }
        admin = AdminClient(conf)
        metadata = admin.list_topics(timeout=10.0)
        topic = os.getenv("KAFKA_TOPIC")
        if topic in metadata.topics:
            print(f"PASSED (Topic '{topic}' verified)")
            return True
        else:
            print(f"FAILED (Topic '{topic}' not found)")
            return False
    except Exception as e:
        print(f"FAILED: {e}")
        return False

def test_aws_s3():
    print("[3/4] Checking AWS S3 Bucket...", end=" ")
    try:
        s3 = boto3.client(
            's3',
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        response = s3.list_buckets()
        buckets = [b['Name'] for b in response.get('Buckets', [])]
        target = os.getenv("AWS_S3_BUCKET")
        if target in buckets:
            print(f"PASSED (Bucket '{target}' accessible)")
            return True
        else:
            print(f"FAILED (Bucket '{target}' not found)")
            return False
    except Exception as e:
        print(f"FAILED: {e}")
        return False

def test_bigquery():
    print("[4/4] Checking Google BigQuery Sandbox...", end=" ")
    try:
        client = bigquery.Client(project=os.getenv("GCP_PROJECT_ID"))
        datasets = [d.dataset_id for d in client.list_datasets()]
        target = os.getenv("BQ_RAW_DATASET")
        if target in datasets:
            print(f"PASSED (Dataset '{target}' detected)")
            return True
        else:
            print(f"FAILED (Dataset '{target}' not found)")
            return False
    except Exception as e:
        print(f"FAILED: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("100% CLOUD INFRASTRUCTURE SMOKE TEST")
    print("=" * 60)
    results = [test_neon(), test_aiven_kafka(), test_aws_s3(), test_bigquery()]
    print("=" * 60)
    if all(results):
        print("ALL CLOUD PLATFORMS REACHABLE. Ready for Chapter 2.")
    else:
        print("CONFIGURATION CHECK FAILED. Review errors above.")
        sys.exit(1)
