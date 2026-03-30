"""
Cymatics Ingestion DAG
Orchestrates the cold path ingestion pipeline:
1. FreeSound API ingestion (monthly — fetches new sounds)
2. Delta Lake metadata update (after each ingestion)

Schedule: Every 1st of the month at 00:00 UTC
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for all tasks
default_args = {
    "owner": "cymatics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cymatics_cold_path_ingestion",
    default_args=default_args,
    description="Monthly cold path ingestion: FreeSound API + Delta Lake metadata update",
    schedule_interval="0 0 1 * *",  # Every 1st of the month at 00:00 UTC
    start_date=datetime(2026, 3, 24),
    catchup=False,
    tags=["cymatics", "cold-path", "ingestion"],
) as dag:

    # Task 1: FreeSound ingestion (fetch new sounds from API)
    freesound_ingestion = BashOperator(
        task_id="freesound_ingestion",
        bash_command="cd /opt/airflow/project && python ingestion/freesound_ingestion.py",
        env={
            "MINIO_ENDPOINT": "{{ var.value.get('MINIO_ENDPOINT', 'localhost:9000') }}",
            "MINIO_ACCESS_KEY": "{{ var.value.get('MINIO_ACCESS_KEY', 'minioadmin') }}",
            "MINIO_SECRET_KEY": "{{ var.value.get('MINIO_SECRET_KEY', 'minioadmin') }}",
            "MINIO_SECURE": "false",
            "MINIO_BUCKET": "landing-zone",
            "FREESOUND_API_KEY": "{{ var.value.get('FREESOUND_API_KEY', '') }}",
            "FREESOUND_CLIENT_ID": "{{ var.value.get('FREESOUND_CLIENT_ID', '') }}",
        },
    )

    # Task 2: Update Delta Lake metadata tables after ingestion
    delta_lake_update = BashOperator(
        task_id="delta_lake_metadata_update",
        bash_command="cd /opt/airflow/project && python landing_zone/delta_lake_metadata.py",
        env={
            "MINIO_ENDPOINT": "{{ var.value.get('MINIO_ENDPOINT', 'localhost:9000') }}",
            "MINIO_ACCESS_KEY": "{{ var.value.get('MINIO_ACCESS_KEY', 'minioadmin') }}",
            "MINIO_SECRET_KEY": "{{ var.value.get('MINIO_SECRET_KEY', 'minioadmin') }}",
            "MINIO_SECURE": "false",
            "MINIO_BUCKET": "landing-zone",
        },
    )

    # Define execution order:
    # First ingest from FreeSound, then update Delta Lake
    freesound_ingestion >> delta_lake_update