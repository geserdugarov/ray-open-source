#!/usr/bin/env bash
set -euo pipefail

BUCKET="${BUCKET:-lance-bench}"
MINIO_URL="${MINIO_URL:-http://127.0.0.1:9000}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BENCH_PYTHON="${BENCH_PYTHON:-${SCRIPT_DIR}/../.venv/bin/python}"

BUCKET="${BUCKET}" MINIO_URL="${MINIO_URL}" "${BENCH_PYTHON}" - <<'PY'
import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

client = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_URL"],
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
bucket = os.environ["BUCKET"]
try:
    client.head_bucket(Bucket=bucket)
except ClientError as error:
    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
        raise
    client.create_bucket(Bucket=bucket)
print(f"MinIO bucket ready: {bucket}")
PY
