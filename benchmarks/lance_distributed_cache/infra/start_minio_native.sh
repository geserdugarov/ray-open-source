#!/usr/bin/env bash
# Build a pinned MinIO source release when prebuilt images are unavailable.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${STATE_DIR:-${SCRIPT_DIR}/../out/minio}"
MINIO_RELEASE="${MINIO_RELEASE:-RELEASE.2025-04-22T22-12-26Z}"
MINIO_BINARY="${MINIO_BINARY:-${STATE_DIR}/bin/minio}"
MINIO_BIND_ADDR="${MINIO_BIND_ADDR:-127.0.0.1}"
mkdir -p "${STATE_DIR}/bin" "${STATE_DIR}/data"
STATE_DIR="$(cd -- "${STATE_DIR}" && pwd)"

if [[ ! -x "${MINIO_BINARY}" ]]; then
  GOBIN="${STATE_DIR}/bin" GOMAXPROCS="${GOMAXPROCS:-2}" \
    go install "github.com/minio/minio@${MINIO_RELEASE}"
fi
if [[ -f "${STATE_DIR}/minio.pid" ]] && kill -0 "$(cat "${STATE_DIR}/minio.pid")" 2>/dev/null; then
  echo "MinIO PID is still alive; inspect ${STATE_DIR}/minio.pid before restarting" >&2
  exit 1
fi

nohup env \
  MINIO_ROOT_USER="${AWS_ACCESS_KEY_ID:-minioadmin}" \
  MINIO_ROOT_PASSWORD="${AWS_SECRET_ACCESS_KEY:-minioadmin}" \
  "${MINIO_BINARY}" server "${STATE_DIR}/data" \
  --address "${MINIO_BIND_ADDR}:9000" \
  --console-address "127.0.0.1:9001" \
  > "${STATE_DIR}/minio.log" 2>&1 < /dev/null &
minio_pid=$!
printf '%s\n' "${minio_pid}" > "${STATE_DIR}/minio.pid"

for _ in {1..40}; do
  if curl -fsS "http://${MINIO_BIND_ADDR}:9000/minio/health/live" > /dev/null 2>&1; then
    echo "MinIO ready at http://${MINIO_BIND_ADDR}:9000 (PID ${minio_pid})"
    echo "Create a bucket with MINIO_URL=http://${MINIO_BIND_ADDR}:9000 infra/make_bucket.sh"
    exit 0
  fi
  if ! kill -0 "${minio_pid}" 2>/dev/null; then
    cat "${STATE_DIR}/minio.log" >&2
    exit 1
  fi
  sleep 1
done
kill "${minio_pid}"
echo "MinIO startup timed out; see ${STATE_DIR}/minio.log" >&2
exit 1
