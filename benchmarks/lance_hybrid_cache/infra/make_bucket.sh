#!/usr/bin/env bash
# Create the lance-bench bucket on the local MinIO.
# Uses a one-shot `mc` docker container so no host install of mc is required.
set -euo pipefail

BUCKET="${BUCKET:-lance-bench}"
NETWORK="${NETWORK:-lance_hybrid_cache_default}"
MINIO_URL="${MINIO_URL:-http://minio:9000}"
MC_IMAGE="${MC_IMAGE:-quay.io/minio/mc:latest}"

# Use the docker compose project network so mc can reach the MinIO service by
# name. Callers can override NETWORK/MINIO_URL for non-compose setups.
if ! docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  NETWORK="lance_hybrid_cache_default"
fi

docker run --rm \
  --network "${NETWORK}" \
  --entrypoint sh \
  -e BUCKET="${BUCKET}" \
  -e MINIO_URL="${MINIO_URL}" \
  "${MC_IMAGE}" -c '
    set -e
    mc alias set local "${MINIO_URL}" minioadmin minioadmin
    mc mb --ignore-existing "local/${BUCKET}"
    mc anonymous set public "local/${BUCKET}"
    mc ls local/
  '
