# main.py
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import storage
import io

# Config
BUCKET_NAME = "gs://BUCKET-NAME/bank-fingerprints-nightly"  # Change to your bucket
ROWS = 100_000  # Small POC dataset

def generate_data():
    """Generate lightweight fingerprint data for POC"""
    df = pd.DataFrame({
        "visitor_id": np.random.randint(1, 20000, size=ROWS),
        "user_id": np.random.randint(1, 500000, size=ROWS),
        "chargeback": np.random.rand(ROWS) < 0.002,
        "campaign_id": np.random.randint(1, 300, size=ROWS),
        "ml_fraud_score": np.random.beta(0.5, 5, size=ROWS),
        "is_emulator": np.random.rand(ROWS) < 0.01,
        "is_vpn": np.random.rand(ROWS) < 0.03,
        "is_proxy": np.random.rand(ROWS) < 0.01,
        "ip": np.random.randint(1, 2**32, size=ROWS)
    })
    return df

def upload_to_gcs(df):
    """Upload DataFrame as Parquet to GCS with partitioned path"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    now = datetime.utcnow()
    path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/fingerprints.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    blob = bucket.blob(path)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"✅ Uploaded {path} ({len(df):,} rows, {buf.tell()/1e6:.2f} MB)")

def dump_to_gcs(request):
    """
    Cloud Function entrypoint
    Can be triggered via HTTP or Cloud Scheduler
    """
    df = generate_data()
    upload_to_gcs(df)
    return f"Uploaded {len(df):,} rows to gs://{BUCKET_NAME}", 200
