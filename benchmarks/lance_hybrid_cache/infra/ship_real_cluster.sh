#!/usr/bin/env bash

set -euo pipefail

RAY_CHECKOUT="${RAY_CHECKOUT:-$HOME/git/ray-open-source}"
WHEELHOUSE="${WHEELHOUSE:-$HOME/wheelhouse}"
SSH_USER="${SSH_USER:-$USER}"
if [[ -z "${REMOTE_HOME:-}" ]]; then
  if [[ "$SSH_USER" == "root" ]]; then
    REMOTE_HOME="/root"
  else
    REMOTE_HOME="/home/$SSH_USER"
  fi
fi

: "${COORD_IP:?Set COORD_IP to the coordinator/head node IP or SSH host.}"
: "${ACTOR0_IP:?Set ACTOR0_IP to actor node 0 IP or SSH host.}"
: "${ACTOR1_IP:?Set ACTOR1_IP to actor node 1 IP or SSH host.}"
: "${MINIO_HOST:?Set MINIO_HOST to the separate MinIO node IP or SSH host.}"

COORD_SSH_HOST="${COORD_SSH_HOST:-$COORD_IP}"
ACTOR0_SSH_HOST="${ACTOR0_SSH_HOST:-$ACTOR0_IP}"
ACTOR1_SSH_HOST="${ACTOR1_SSH_HOST:-$ACTOR1_IP}"
MINIO_SSH_HOST="${MINIO_SSH_HOST:-$MINIO_HOST}"

RSYNC_COMMON_ARGS=(-a --delete --human-readable --info=progress2,stats1)

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

rsync_with_log() {
  local label="$1"
  shift

  log "rsync start: ${label}"
  rsync "${RSYNC_COMMON_ARGS[@]}" "$@"
  log "rsync done: ${label}"
}

for path in \
  "$WHEELHOUSE" \
  "$RAY_CHECKOUT/benchmarks/lance_hybrid_cache"; do
  if [[ ! -d "$path" ]]; then
    echo "missing required directory: $path" >&2
    exit 1
  fi
done

ship_ray_node() {
  local role="$1"
  local host="$2"
  local target="${SSH_USER}@${host}"

  log "shipping to ${role}: ${target}, remote home: ${REMOTE_HOME}"

  log "mkdir start: ${target}"
  ssh "$target" "mkdir -p \
    '$REMOTE_HOME/git/ray-open-source/benchmarks' \
    '$REMOTE_HOME/git/ray-open-source/python' \
    '$REMOTE_HOME/wheelhouse'"
  log "mkdir done: ${target}"

  rsync_with_log "wheelhouse -> ${target}:${REMOTE_HOME}/wheelhouse" \
    "$WHEELHOUSE/" \
    "$target:$REMOTE_HOME/wheelhouse/"

  rsync_with_log "ray benchmark -> ${target}:${REMOTE_HOME}/git/ray-open-source/benchmarks/lance_hybrid_cache" \
    "$RAY_CHECKOUT/benchmarks/lance_hybrid_cache/" \
    "$target:$REMOTE_HOME/git/ray-open-source/benchmarks/lance_hybrid_cache/"

  log "shipping done: ${role}"
}

ship_minio_node() {
  local host="$1"
  local target="${SSH_USER}@${host}"

  log "shipping to minio: ${target}, remote home: ${REMOTE_HOME}"

  log "mkdir start: ${target}"
  ssh "$target" "mkdir -p \
    '$REMOTE_HOME/git/ray-open-source/benchmarks/lance_hybrid_cache'"
  log "mkdir done: ${target}"

  rsync_with_log "minio infra -> ${target}:${REMOTE_HOME}/git/ray-open-source/benchmarks/lance_hybrid_cache/infra" \
    "$RAY_CHECKOUT/benchmarks/lance_hybrid_cache/infra/" \
    "$target:$REMOTE_HOME/git/ray-open-source/benchmarks/lance_hybrid_cache/infra/"

  log "shipping done: minio"
}

ship_ray_node "coordinator" "$COORD_SSH_HOST"
ship_ray_node "actor0" "$ACTOR0_SSH_HOST"
ship_ray_node "actor1" "$ACTOR1_SSH_HOST"
ship_minio_node "$MINIO_SSH_HOST"

log "shipping complete"
