#!/usr/bin/env bash
# Counterbalance scenario order while keeping queries and data fixed.
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BENCH_PYTHON="${BENCH_PYTHON:-${HERE}/.venv/bin/python}"
EXPERIMENT_ID="${EXPERIMENT_ID:-comparison-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
OUT_DIR="${OUT_DIR:-${HERE}/out/${EXPERIMENT_ID}}"
NVME_DIR="${NVME_DIR:-${HOME}/lance-two-level-cache}"
ENDPOINT_URL="${ENDPOINT_URL:-http://127.0.0.1:9000}"
REPETITIONS="${REPETITIONS:-3}"
if [[ ! "${REPETITIONS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "REPETITIONS must be a positive integer, got '${REPETITIONS}'" >&2
  exit 1
fi
mkdir -p "${OUT_DIR}"

export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LANCE_CPU_THREADS=1 LANCE_IO_THREADS=8
common=(
  "${HERE}/run_benchmark.py"
  --scale "${SCALE:-1000000}" --dim "${DIM:-256}"
  --num-partitions "${NUM_PARTITIONS:-512}" --num-bits 8
  --num-actors 4 --cpus-per-actor 1
  --metadata-l1-mb 16 --prewarm strict
  --measure-queries "${MEASURE_QUERIES:-256}" --warmup-queries 16
  --k-list 10 --nprobes "${NPROBES:-16}"
  --lance-cpu-threads 1 --lance-io-threads 8
  --endpoint-url "${ENDPOINT_URL}"
  --nvme-dir "${NVME_DIR}" --out-dir "${OUT_DIR}"
)

for ((trial=1; trial<=REPETITIONS; trial++)); do
  case $((trial % 3)) in
    1) small_order=no-index-cache,memory,distributed; pair_order=memory,distributed ;;
    2) small_order=distributed,no-index-cache,memory; pair_order=distributed,memory ;;
    0) small_order=memory,distributed,no-index-cache; pair_order=memory,distributed ;;
  esac
  for profile in small fit l2-only; do
    case "${profile}" in
      small) partition_mb=32; scenario_order="${small_order}" ;;
      fit) partition_mb=1024; scenario_order="${pair_order}" ;;
      l2-only) partition_mb=0; scenario_order="${pair_order}" ;;
    esac
    run_id="${EXPERIMENT_ID}-${profile}-${trial}"
    options=(--partition-l1-mb "${partition_mb}" --scenarios "${scenario_order}")
    if [[ "${trial}" == 1 && "${profile}" == small ]]; then
      options+=(--invalidation-drill)
    fi
    printf 'Running %s\n' "${run_id}"
    "${BENCH_PYTHON}" -u "${common[@]}" "${options[@]}" --l2-run-id "${run_id}" \
      > "${OUT_DIR}/${run_id}.log" 2>&1
    tail -n 4 "${OUT_DIR}/${run_id}.log"
  done
done
printf 'Comparison results: %s\n' "${OUT_DIR}"
