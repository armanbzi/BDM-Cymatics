import requests
import os
import json
import pandas as pd
import time
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io

# Load environment variables
load_dotenv()

# FreeSound credentials
API_KEY = os.getenv("FREESOUND_API_KEY")
CLIENT_ID = os.getenv("FREESOUND_CLIENT_ID")

# MinIO client
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
)

BUCKET = os.getenv("MINIO_BUCKET")
SOURCE = "freesound"

# Categories for cymatics
QUERIES = [
    "rain", "church bells", "crickets",
    "thunder", "water drops", "sea waves",
    "wind", "singing bowl", "tuning fork"
]

def ensure_bucket():
    """Create bucket if it doesn't exist"""
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"Bucket created: {BUCKET}")
    else:
        print(f"Bucket already exists: {BUCKET}")

def search_sounds(query, num_results=10):
    """Search sounds in FreeSound by category"""
    url = "https://freesound.org/apiv2/search/text/"
    params = {
        "query": query,
        "token": API_KEY,
        "fields": "id,name,duration,previews,tags,description",
        "page_size": num_results,
        "filter": "duration:[1 TO 10]"
    }
    response = requests.get(url, params=params)
    return response.json()

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
print("FREESOUND INGESTION — Starting")
print("=" * 50)

ensure_bucket()

metadata_total = []

for query in QUERIES:
    print(f"\nSearching: '{query}'")
    results = search_sounds(query, num_results=10)

    if "results" not in results:
        print(f"  Error: {results}")
        continue

    print(f"  Found: {results['count']} sounds")

    for sound in results["results"]:
        filename = f"{query.replace(' ', '_')}_{sound['id']}.mp3"
        audio_path = f"{SOURCE}/audios/{filename}"

        # Download audio preview
        url_preview = sound["previews"]["preview-lq-mp3"]
        audio_response = requests.get(url_preview)

        if audio_response.status_code == 200:
            # Upload audio to MinIO
            upload_to_minio(audio_response.content, audio_path, "audio/mpeg")

            # Collect metadata
            metadata_total.append({
                "id": sound["id"],
                "name": sound["name"],
                "category": query,
                "duration": sound["duration"],
                "filename": filename,
                "minio_path": f"{BUCKET}/{audio_path}",
                "tags": str(sound.get("tags", [])),
                "source": "freesound_api"
            })
        else:
            print(f"  Failed to download: {filename}")

        time.sleep(0.3)  # Respect API limits

# Upload metadata CSV to MinIO
df_meta = pd.DataFrame(metadata_total)
csv_bytes = df_meta.to_csv(index=False).encode("utf-8")
upload_to_minio(csv_bytes, f"{SOURCE}/metadata/freesound_metadata.csv", "text/csv")

# Upload metadata JSON to MinIO
json_bytes = json.dumps(metadata_total, indent=2).encode("utf-8")
upload_to_minio(json_bytes, f"{SOURCE}/metadata/freesound_metadata.json", "application/json")

print("\n" + "=" * 50)
print("FREESOUND INGESTION — Done")
print(f"  Total sounds ingested: {len(metadata_total)}")
print(f"  Bucket: {BUCKET}")
print(f"  Audios: {SOURCE}/audios/")
print(f"  Metadata: {SOURCE}/metadata/")
print("=" * 50)
