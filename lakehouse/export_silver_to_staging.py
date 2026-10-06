import os
import io
import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv
from deltalake import DeltaTable

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")

silver_uri = f"s3://{S3_BUCKET}/silver/orders_events"
export_key = "export/orders_events/staged_events.parquet"

storage_options = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_REGION": AWS_REGION,
    "region": AWS_REGION,
    "AWS_ENDPOINT_URL": f"https://s3.{AWS_REGION}.amazonaws.com",
    "endpoint_url": f"https://s3.{AWS_REGION}.amazonaws.com",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "allow_unsafe_rename": "true"
}

# 1. Read curated records from Silver Delta table
print(f"Reading active snapshot from Silver Delta table: {silver_uri}...")
dt = DeltaTable(silver_uri, storage_options=storage_options)
arrow_table = dt.to_pyarrow_table()
print(f"Loaded {arrow_table.num_rows} records from Delta Table.")

# 2. Convert Arrow table to standard Parquet bytes in memory
print("Serializing Arrow table to Parquet bytes...")
buffer = io.BytesIO()
pq.write_table(arrow_table, buffer, compression="snappy")
parquet_bytes = buffer.getvalue()

# 3. Direct upload to s3://.../export/ via Boto3
print(f"Uploading clean Parquet staging file to s3://{S3_BUCKET}/{export_key}...")
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

s3.put_object(
    Bucket=S3_BUCKET,
    Key=export_key,
    Body=parquet_bytes
)

print(f"Successfully exported {len(parquet_bytes)} bytes to S3 export staging folder!")
