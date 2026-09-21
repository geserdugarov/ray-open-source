#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${SCRIPT_DIR}/docker-compose.yml}"
BUCKET="${BUCKET:-lance-bench}"
CONTAINER_NAME="${CONTAINER_NAME:-lance-cache-bench-minio}"
WAIT_SECS="${WAIT_SECS:-60}"

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

require_docker() {
  command -v docker >/dev/null 2>&1 || {
    echo "docker is required" >&2
    exit 127
  }
  docker info >/dev/null 2>&1 || {
    echo "cannot connect to the Docker daemon" >&2
    exit 1
  }
}

wait_for_minio() {
  local started now status
  started="$(date +%s)"
  while true; do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${CONTAINER_NAME}" 2>/dev/null || true)"
    if [[ "${status}" == "healthy" ]]; then
      return
    fi
    now="$(date +%s)"
    if ((now - started >= WAIT_SECS)); then
      echo "timed out waiting for ${CONTAINER_NAME}" >&2
      compose ps >&2 || true
      exit 1
    fi
    sleep 2
  done
}

case "${1:-up}" in
  up|start)
    require_docker
    compose up -d minio
    wait_for_minio
    BUCKET="${BUCKET}" bash "${SCRIPT_DIR}/make_bucket.sh"
    echo "MinIO ready at http://127.0.0.1:9000; bucket=${BUCKET}"
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
  *)
    echo "usage: $0 [up|status|logs|down]" >&2
    exit 2
    ;;
esac
