#!/usr/bin/env python3

import os
import time
from dataclasses import dataclass
from typing import Dict

import boto3
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "password123")
    bucket: str = os.getenv("MINIO_BUCKET", "wearable-sensor-demo")
    region: str = os.getenv("MINIO_REGION", "us-east-1")


class MinioCrud:
    def __init__(self, config: MinioConfig):
        self.config = config
        self.client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
        )

    def create_bucket(self):
        try:
            self.client.head_bucket(Bucket=self.config.bucket)
            print(f"Bucket already exists: {self.config.bucket}")
        except ClientError:
            self.client.create_bucket(Bucket=self.config.bucket)
            print(f"Created bucket: {self.config.bucket}")

    def ensure_bucket_ready(self, attempts: int = 6, delay_seconds: int = 5):
        for attempt in range(1, attempts + 1):
            try:
                self.create_bucket()
                return
            except Exception as exc:
                if attempt == attempts:
                    raise

                print(
                    f"MinIO not ready ({exc}). "
                    f"Retrying {attempt}/{attempts - 1}..."
                )
                time.sleep(delay_seconds)

    def create_object(self, key: str, content: str):
        self.client.put_object(
            Bucket=self.config.bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )
        print(f"Created object: s3://{self.config.bucket}/{key}")

    def read_object(self, key: str):
        response = self.client.get_object(Bucket=self.config.bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        print(f"Read object: {content}")
        return content

    def update_object(self, key: str, content: str):
        self.client.put_object(
            Bucket=self.config.bucket,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/plain",
        )
        print(f"Updated object: s3://{self.config.bucket}/{key}")

    def list_objects(self):
        response = self.client.list_objects_v2(Bucket=self.config.bucket)
        objects = response.get("Contents", [])

        print("Objects:")
        for item in objects:
            print(f"- {item['Key']} ({item['Size']} bytes)")

        return objects

    def delete_prefix(self, prefix: str):
        paginator = self.client.get_paginator("list_objects_v2")
        normalized_prefix = prefix.strip("/")

        for page in paginator.paginate(Bucket=self.config.bucket, Prefix=f"{normalized_prefix}/"):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                self.client.delete_objects(
                    Bucket=self.config.bucket,
                    Delete={"Objects": objects},
                )

        print(f"Deleted prefix: s3://{self.config.bucket}/{normalized_prefix}/")

    def upload_file(self, file_path: str, key: str):
        self.client.upload_file(file_path, self.config.bucket, key)
        print(f"Uploaded object: s3://{self.config.bucket}/{key}")

    def upload_directory(self, local_dir: str, prefix: str):
        if not os.path.isdir(local_dir):
            raise ValueError(f"Directory not found: {local_dir}")

        normalized_prefix = prefix.strip("/")
        for root, _, files in os.walk(local_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(file_path, local_dir).replace(os.sep, "/")
                self.upload_file(file_path, f"{normalized_prefix}/{relative_path}")

        return f"s3://{self.config.bucket}/{normalized_prefix}"

    def parquet_prefix(self, parquet_path: str):
        base_prefix = os.getenv("MINIO_PARQUET_PREFIX", "HAR_SmartHealth").strip("/")
        parquet_name = os.path.basename(os.path.normpath(parquet_path))
        return f"{base_prefix}/{parquet_name}" if base_prefix else parquet_name

    def upload_parquet_outputs(self, parquet_paths: Dict[str, str]):
        self.ensure_bucket_ready()
        minio_paths = {}

        for name, parquet_path in parquet_paths.items():
            prefix = self.parquet_prefix(parquet_path)
            self.delete_prefix(prefix)
            minio_paths[name] = self.upload_directory(parquet_path, prefix)
            print(f"Uploaded parquet to MinIO: {minio_paths[name]}")

        return minio_paths

    def delete_object(self, key: str):
        self.client.delete_object(Bucket=self.config.bucket, Key=key)
        print(f"Deleted object: s3://{self.config.bucket}/{key}")

    def run_demo(self):
        key = "sensor-demo.txt"

        self.create_bucket()
        self.create_object(key, "create: wearable sensor data")
        self.read_object(key)
        self.update_object(key, "update: wearable sensor data")
        self.read_object(key)
        self.list_objects()
        self.delete_object(key)


def main():
    MinioCrud(MinioConfig()).run_demo()


if __name__ == "__main__":
    main()
