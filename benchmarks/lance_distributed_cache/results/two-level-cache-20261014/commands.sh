#!/usr/bin/env bash
set -euo pipefail
BENCH_ROOT=/home/d00838679/git/ray-open-source/benchmarks/lance_distributed_cache
BENCH_PYTHON="${BENCH_ROOT}/.venv/bin/python"
REPORT_ROOT="${BENCH_ROOT}/results/two-level-cache-20261014"
export LANCE_BUILD_COMMIT=ee7c7c6c2c8a989bb0ee51339ec3b69eb638b591
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LANCE_CPU_THREADS=1 LANCE_IO_THREADS=8

EXPERIMENT_ID=perf-final-20261014 ENDPOINT_URL=http://192.168.201.98:9000 \
  bash "${BENCH_ROOT}/run_comparison.sh"
MPLBACKEND=Agg "${BENCH_PYTHON}" "${BENCH_ROOT}/analyze_comparison.py" \
  "${BENCH_ROOT}/out/perf-final-20261014" --out-dir "${REPORT_ROOT}"

cold_root="${BENCH_ROOT}/out/perf-final-cold-20261014"
mkdir -p "${cold_root}"
for trial in 1 2 3; do
  if [[ "${trial}" == 2 ]]; then order=distributed,memory; else order=memory,distributed; fi
  run_id="perf-final-cold-20261014-small-${trial}"
  "${BENCH_PYTHON}" -u "${BENCH_ROOT}/run_benchmark.py" \
    --scale 1000000 --dim 256 --num-partitions 512 --num-bits 8 \
    --num-actors 4 --cpus-per-actor 1 --metadata-l1-mb 16 --partition-l1-mb 32 \
    --prewarm strict --measure-queries 256 --warmup-queries 16 --k-list 10 --nprobes 16 \
    --lance-cpu-threads 1 --lance-io-threads 8 \
    --endpoint-url http://192.168.201.98:9000 \
    --nvme-dir /home/d00838679/lance-two-level-cache --scenarios "${order}" \
    --drop-l2-page-cache-on-restart --lance-build-commit "${LANCE_BUILD_COMMIT}" \
    --l2-run-id "${run_id}" --out-dir "${cold_root}" > "${cold_root}/${run_id}.log" 2>&1
done
MPLBACKEND=Agg "${BENCH_PYTHON}" "${BENCH_ROOT}/analyze_comparison.py" \
  "${cold_root}" --out-dir "${REPORT_ROOT}/cold-l2"
printf 'Final performance comparisons and analysis complete.\n'
