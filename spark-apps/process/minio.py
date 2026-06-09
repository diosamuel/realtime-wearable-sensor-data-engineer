#!/usr/bin/env python3

import os
from dataclasses import dataclass

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
