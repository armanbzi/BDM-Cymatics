# Cymatics BDM — Run instructions

Copy `env.example` to `.env` and set MinIO and FreeSound credentials. Run from the project root.

## Cold path (batch ingestion)

1. Start MinIO (optional for step 2): `docker compose up -d`
2. Download audio + metadata from FreeSound:

```bash
python ingestion/freesound_ingestion.py
```

3. Create buckets and upload metadata to MinIO:

```bash
python ingestion/minio_connection.py
```

## Warm path

```bash
docker compose up -d
python landing_zone_warm.py
```

## Hot path

Terminal 1:

```bash
docker compose up -d
python landing_zone_hot_consumer.py
```

Terminal 2:

```bash
python landing_zone_hot_producer.py
```
