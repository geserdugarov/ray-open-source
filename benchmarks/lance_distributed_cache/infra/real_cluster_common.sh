#!/usr/bin/env bash
# Shared settings for the real-cluster commands. Source from Bash.
# shellcheck disable=SC2034  # Settings are consumed by the calling scripts.

INFRA_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname -- "$INFRA_DIR")"
if [[ -n "${CLUSTER_ENV:-}" || -f "$INFRA_DIR/real_cluster.env" ]]; then
  # shellcheck disable=SC1090
  source "${CLUSTER_ENV:-$INFRA_DIR/real_cluster.env}"
fi

WHEELHOUSE="${WHEELHOUSE:-$BENCH_DIR/out/wheelhouse}"
PYTHON_BIN="${PYTHON_BIN:-python3.12}"

cluster_settings() {
  : "${COORD_IP:?Set COORD_IP in infra/real_cluster.env or the environment}"
  : "${ACTOR0_IP:?Set ACTOR0_IP}"
  : "${ACTOR1_IP:?Set ACTOR1_IP}"
  : "${MINIO_HOST:?Set MINIO_HOST}"
  SSH_USER="${SSH_USER:-$USER}"
  if [[ "$SSH_USER" == root ]]; then
    REMOTE_HOME="${REMOTE_HOME:-/root}"
  else
    REMOTE_HOME="${REMOTE_HOME:-/home/$SSH_USER}"
  fi
  COORD_SSH_HOST="${COORD_SSH_HOST:-$COORD_IP}"
  ACTOR0_SSH_HOST="${ACTOR0_SSH_HOST:-$ACTOR0_IP}"
  ACTOR1_SSH_HOST="${ACTOR1_SSH_HOST:-$ACTOR1_IP}"
  MINIO_SSH_HOST="${MINIO_SSH_HOST:-$MINIO_HOST}"
  REMOTE_BENCH_DIR="${REMOTE_BENCH_DIR:-$REMOTE_HOME/git/ray-open-source/benchmarks/lance_distributed_cache}"
  REMOTE_WHEELHOUSE="${REMOTE_WHEELHOUSE:-$REMOTE_HOME/wheelhouse-lance12}"
  REMOTE_VENV="$REMOTE_BENCH_DIR/.venv"
  ENDPOINT_URL="${ENDPOINT_URL:-http://$MINIO_HOST:9000}"
  NVME_DIR="${NVME_DIR:-/mnt/nvme/lance-cache-bench}"
  ACTOR_RESOURCE=search_actor_node
}

remote() {
  local host="$1" command shell_command
  shift
  printf -v command '%q ' "$@"
  # Use Bash even when the account's login shell is sh; %q can emit $'...'.
  printf -v shell_command '%q' "$command"
  ssh -o BatchMode=yes "$SSH_USER@$host" "bash -c $shell_command"
}
