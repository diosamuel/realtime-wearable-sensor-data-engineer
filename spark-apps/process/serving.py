#!/usr/bin/env python3

import os
from urllib.parse import urlparse

import duckdb


S3_PARQUET_PATH = os.getenv(
    "SENSOR_PARQUET_URI",
    "s3://wearable-sensor-demo/HAR_SmartHealth/current_sensor/**/*.parquet",
)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
LIMIT = int(os.getenv("SERVING_LIMIT", "20"))


def sql_quote(value):
    return "'" + value.replace("'", "''") + "'"


def endpoint_host(endpoint):
    parsed = urlparse(endpoint)
    return parsed.netloc if parsed.scheme else endpoint


def use_ssl(endpoint):
    explicit = os.getenv("MINIO_USE_SSL")
    if explicit is not None:
        return explicit.lower() in {"1", "true", "yes", "on"}

    return urlparse(endpoint).scheme == "https"


def main():
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region = {sql_quote(MINIO_REGION)}")
    conn.execute(f"SET s3_endpoint = {sql_quote(endpoint_host(MINIO_ENDPOINT))}")
    conn.execute(f"SET s3_access_key_id = {sql_quote(MINIO_ACCESS_KEY)}")
    conn.execute(f"SET s3_secret_access_key = {sql_quote(MINIO_SECRET_KEY)}")
    conn.execute("SET s3_url_style = 'path'")
    conn.execute(f"SET s3_use_ssl = {str(use_ssl(MINIO_ENDPOINT)).lower()}")

    return conn