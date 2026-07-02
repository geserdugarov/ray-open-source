#!/usr/bin/env bash
# Start local MinIO for small Lance S3 checks and create the default bucket.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${SCRIPT_DIR}/docker-compose.yml}"
BUCKET="${BUCKET:-lance-bench}"
CONTAINER_NAME="${CONTAINER_NAME:-lance-bench-minio}"
ENDPOINT_HOST="${ENDPOINT_HOST:-127.0.0.1}"
ENDPOINT_PORT="${ENDPOINT_PORT:-9000}"
ENDPOINT_URL="${ENDPOINT_URL:-http://${ENDPOINT_HOST}:${ENDPOINT_PORT}}"
WAIT_SECS="${WAIT_SECS:-60}"

usage() {
  cat <<'EOF'
Usage: infra/run_minio.sh [up|env|status|logs|down]

Commands:
  up      Start MinIO, wait until healthy, create BUCKET. This is the default.
  env     Print environment variables for Lance/Python clients.
  status  Show docker compose service status.
  logs    Follow MinIO logs.
  down    Stop MinIO without deleting the persisted Docker volume.

Optional environment:
  BUCKET=lance-bench
  MINIO_BIND_ADDR=127.0.0.1
  ENDPOINT_HOST=127.0.0.1
  ENDPOINT_PORT=9000
  MINIO_IMAGE=quay.io/minio/minio:latest
  MC_IMAGE=quay.io/minio/mc:latest
EOF
}

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required but was not found on PATH" >&2
    exit 127
  fi

  if ! docker info >/dev/null 2>&1; then
    cat >&2 <<'EOF'
Cannot connect to the Docker daemon.
Run this from an account that can access Docker, or run the script with sudo
if this host requires sudo for Docker commands.
EOF
    exit 1
  fi
}

wait_for_minio() {
  local start now status
  start="$(date +%s)"

  while true; do
    status="$(
      docker inspect \
        -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "${CONTAINER_NAME}" 2>/dev/null || true
    )"

    if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
      return 0
    fi

    now="$(date +%s)"
    if ((now - start >= WAIT_SECS)); then
      echo "Timed out waiting for MinIO container ${CONTAINER_NAME} to become healthy" >&2
      compose ps >&2 || true
      exit 1
    fi

    sleep 2
  done
}

print_env() {
  cat <<EOF
export AWS_ENDPOINT_URL=${ENDPOINT_URL}
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export AWS_ALLOW_HTTP=true

# Lance URI example:
#   s3://${BUCKET}/smoke/
EOF
}

cmd="${1:-up}"
case "${cmd}" in
  up|start)
    require_docker
    compose up -d minio
    wait_for_minio
    BUCKET="${BUCKET}" bash "${SCRIPT_DIR}/make_bucket.sh"
    echo "MinIO is ready at ${ENDPOINT_URL}; bucket: ${BUCKET}"
    print_env
    ;;
  env)
    print_env
    ;;
  status|ps)
    require_docker
    compose ps
    ;;
  logs)
    require_docker
    compose logs -f minio
    ;;
  down|stop)
    require_docker
    compose down
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
