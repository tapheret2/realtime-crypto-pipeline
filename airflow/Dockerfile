# syntax=docker/dockerfile:1.6
FROM apache/airflow:2.9.2-python3.11

USER root

# Install OS deps Spark/JDBC sometimes need at runtime.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
