#!/usr/bin/env bash
# Control the head + two-worker cluster from the build machine.
set -euo pipefail
usage() {
  echo "Usage: $0 {start|minio|smoke|run|comparison|fetch|status|stop} [benchmark args]"
}
action="${1:---help}"
if [[ "$action" == --help || "$action" == -h ]]; then
  usage
  exit 0
fi
case "$action" in
  start|minio|smoke|run|comparison|fetch|status|stop) ;;
  *) usage >&2; exit 2 ;;
esac
shift
source "$(dirname -- "${BASH_SOURCE[0]}")/real_cluster_common.sh"
cluster_settings

check_workers() {
  remote "$COORD_SSH_HOST" "$REMOTE_VENV/bin/python" -c '
import sys
import ray
ray.init(address=sys.argv[1], logging_level="ERROR")
workers = [
    n for n in ray.nodes()
    if n["Alive"] and n["Resources"].get("search_actor_node", 0) >= 1
]
addresses = {n["NodeManagerAddress"] for n in workers}
if addresses != set(sys.argv[2:]) or len(workers) != 2:
    raise SystemExit(f"Expected exactly two benchmark workers at {sys.argv[2:]}; found {sorted(addresses)}")
if any(n["Resources"].get("search_actor_node") != 1 or n["Resources"].get("CPU", 0) < 1 for n in workers):
    raise SystemExit("Each worker needs one search_actor_node resource and at least one CPU")
print(f"Benchmark workers ready: {sorted(addresses)}")
ray.shutdown()
' "$COORD_IP:6379" "$ACTOR0_IP" "$ACTOR1_IP"
}

remote_logged() {
  local log_file="$1"
  shift
  remote "$COORD_SSH_HOST" bash -s -- "$log_file" "$@" <<'REMOTE'
set -euo pipefail
log_file="$1"
shift
mkdir -p -- "$(dirname -- "$log_file")"
{
  printf 'Started: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  status=0
  "$@" || status=$?
  printf 'Finished: %s; exit status: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$status"
  exit "$status"
} 2>&1 | tee -- "$log_file"
REMOTE
}

case "$action" in
  start)
    ports=(--node-manager-port=8076 --object-manager-port=8077
      --runtime-env-agent-port=8078 --dashboard-agent-grpc-port=8079
      --dashboard-agent-listen-port=52365 --min-worker-port=10002 --max-worker-port=10100)
    remote "$COORD_SSH_HOST" "$REMOTE_VENV/bin/ray" start --head \
      --node-ip-address="$COORD_IP" --port=6379 --num-cpus=0 \
      --dashboard-host=0.0.0.0 --dashboard-port=8265 "${ports[@]}"
    for role in 0 1; do
      if [[ "$role" == 0 ]]; then host="$ACTOR0_SSH_HOST"; ip="$ACTOR0_IP"
      else host="$ACTOR1_SSH_HOST"; ip="$ACTOR1_IP"; fi
      remote "$host" mkdir -p "$NVME_DIR"
      remote "$host" test -w "$NVME_DIR"
      remote "$host" "$REMOTE_VENV/bin/ray" start --address="$COORD_IP:6379" \
        --node-ip-address="$ip" --resources="{\"$ACTOR_RESOURCE\":1}" "${ports[@]}"
    done
    ;;
  minio)
    remote "$MINIO_SSH_HOST" env MINIO_BIND_ADDR=0.0.0.0 \
      MINIO_IMAGE="${MINIO_IMAGE:-quay.io/minio/minio:latest}" \
      docker compose -f "$REMOTE_BENCH_DIR/infra/docker-compose.yml" up -d --wait
    remote "$COORD_SSH_HOST" env MINIO_URL="$ENDPOINT_URL" BUCKET="${BUCKET:-lance-bench}" \
      BENCH_PYTHON="$REMOTE_VENV/bin/python" bash "$REMOTE_BENCH_DIR/infra/make_bucket.sh"
    ;;
  smoke|run|comparison)
    local_log_dir="${LOCAL_LOG_DIR:-$BENCH_DIR/out/real-cluster/logs}"
    mkdir -p -- "$local_log_dir"
    local_log="$(mktemp "$local_log_dir/client-${action}-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX.log")"
    log_name="${local_log##*/}"
    head_log="$REMOTE_BENCH_DIR/out/logs/${log_name#client-}"
    {
      printf 'Local console log: %s\n' "$local_log"
      printf 'Head console log: %s@%s:%s\n' "$SSH_USER" "$COORD_SSH_HOST" "$head_log"
      check_workers
      environment=(env RAY_ADDRESS="$COORD_IP:6379" ENDPOINT_URL="$ENDPOINT_URL"
        NVME_DIR="$NVME_DIR" LANCE_BUILD_COMMIT="${LANCE_BUILD_COMMIT:-}"
        OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1)
      if [[ "$action" == comparison ]]; then
        remote_logged "$head_log" "${environment[@]}" \
          NUM_ACTORS=2 ACTOR_RESOURCE="$ACTOR_RESOURCE" BUCKET="${BUCKET:-lance-bench}" \
          REPETITIONS="${REPETITIONS:-3}" EXPERIMENT_ID="${EXPERIMENT_ID:-}" \
          bash "$REMOTE_BENCH_DIR/run_comparison.sh" "$@"
      else
        options=()
        if [[ "$action" == smoke ]]; then
          options=(--scale 10000 --dim 32 --num-partitions 8 --num-bits 2
            --metadata-l1-mb 4 --partition-l1-mb 4 --measure-queries 8
            --warmup-queries 2 --k-list 10 --nprobes 2 --invalidation-drill)
        fi
        remote_logged "$head_log" "${environment[@]}" "$REMOTE_VENV/bin/python" -u \
          "$REMOTE_BENCH_DIR/run_benchmark.py" --num-actors 2 \
          --actor-resource "$ACTOR_RESOURCE" --endpoint-url "$ENDPOINT_URL" \
          --bucket "${BUCKET:-lance-bench}" --nvme-dir "$NVME_DIR" \
          --out-dir "$REMOTE_BENCH_DIR/out" --prewarm strict \
          --lance-cpu-threads 1 --lance-io-threads 8 "${options[@]}" "$@"
      fi
    } 2>&1 | tee -- "$local_log"
    ;;
  fetch)
    mkdir -p "$BENCH_DIR/out/real-cluster"
    rsync -az --protect-args -e 'ssh -o BatchMode=yes' \
      --exclude '/wheelhouse/' --exclude '/build-venv/' \
      "$SSH_USER@$COORD_SSH_HOST:$REMOTE_BENCH_DIR/out/" "$BENCH_DIR/out/real-cluster/"
    printf 'Results: %s\n' "$BENCH_DIR/out/real-cluster"
    ;;
  status)
    remote "$COORD_SSH_HOST" "$REMOTE_VENV/bin/ray" status --address="$COORD_IP:6379"
    ;;
  stop)
    failed=0
    for host in "$ACTOR0_SSH_HOST" "$ACTOR1_SSH_HOST" "$COORD_SSH_HOST"; do
      remote "$host" "$REMOTE_VENV/bin/ray" stop || failed=1
    done
    exit "$failed"
    ;;
esac
