import os
import pandas as pd
import json
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
 
# Load environment variables
load_dotenv()
 
# MinIO client
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
)
 
BUCKET = os.getenv("MINIO_BUCKET")
SOURCE = "esc50"
ESC50_BASE = os.getenv("ESC50_BASE_PATH")
AUDIO_DIR = os.path.join(ESC50_BASE, "audio")
META_CSV = os.path.join(ESC50_BASE, "meta", "esc50.csv")
 
def ensure_bucket():
    """Create bucket if it doesn't exist"""
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"Bucket created: {BUCKET}")
    else:
        print(f"Bucket already exists: {BUCKET}")
 
def upload_to_minio(data_bytes, object_path, content_type="application/octet-stream"):
    """Upload bytes directly to MinIO"""
    try:
        minio_client.put_object(
            BUCKET,
            object_path,
            io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type=content_type
        )
        print(f"  Uploaded: {object_path}")
        return True
    except S3Error as e:
        print(f"  Error uploading {object_path}: {e}")
        return False
 
# ============================================
# MAIN INGESTION
# ============================================
print("=" * 50)
print("ESC-50 INGESTION — Starting")
print("=" * 50)
 
ensure_bucket()
 
# Read metadata CSV
print("\nReading metadata CSV...")
df = pd.read_csv(META_CSV)
print(f"  Total audio files: {len(df)}")
print(f"  Categories: {df['category'].nunique()}")
 
# Upload each audio file to MinIO
print("\nUploading audio files...")
uploaded = 0
failed = 0
 
for _, row in df.iterrows():
    audio_filename = row["filename"]
    audio_file_path = os.path.join(AUDIO_DIR, audio_filename)
    minio_object_path = f"{SOURCE}/audios/{audio_filename}"
 
    if os.path.exists(audio_file_path):
        with open(audio_file_path, "rb") as f:
            audio_bytes = f.read()
        success = upload_to_minio(audio_bytes, minio_object_path, "audio/wav")
        if success:
            uploaded += 1
    else:
        print(f"  File not found: {audio_filename}")
        failed += 1
 
# Upload metadata CSV to MinIO
print("\nUploading metadata...")
csv_bytes = df.to_csv(index=False).encode("utf-8")
upload_to_minio(csv_bytes, f"{SOURCE}/metadata/esc50_metadata.csv", "text/csv")
 
# Upload metadata JSON to MinIO
records = df.to_dict(orient="records")
json_bytes = json.dumps(records, indent=2).encode("utf-8")
upload_to_minio(json_bytes, f"{SOURCE}/metadata/esc50_metadata.json", "application/json")
 
print("\n" + "=" * 50)
print("ESC-50 INGESTION — Done")
print(f"  Audio files uploaded: {uploaded}")
print(f"  Audio files failed:   {failed}")
print(f"  Bucket: {BUCKET}")
print(f"  Audios: {SOURCE}/audios/")
print(f"  Metadata: {SOURCE}/metadata/")
print("=" * 50)