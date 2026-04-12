FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    python-dotenv requests numpy scipy minio pyarrow
