#!/usr/bin/env bash
set -euo pipefail

BUCKET="${BUCKET:-lance-bench}"
NETWORK="${NETWORK:-lance_distributed_cache_default}"
MINIO_URL="${MINIO_URL:-http://minio:9000}"
MC_IMAGE="${MC_IMAGE:-quay.io/minio/mc:latest}"

docker run --rm \
  --network "${NETWORK}" \
  --entrypoint sh \
  -e BUCKET="${BUCKET}" \
  -e MINIO_URL="${MINIO_URL}" \
  "${MC_IMAGE}" -c '
    set -e
    mc alias set local "${MINIO_URL}" minioadmin minioadmin
    mc mb --ignore-existing "local/${BUCKET}"
    mc ls local/
  '
