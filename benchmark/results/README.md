# Single-node distributed IVF cache benchmark results

Run of `benchmark/run_distributed_bench.py` on a single host (local Ray
instance, no `--actor-resource`). Follows the recipe in
[`benchmark/README.md`](../README.md) §4 with `--simulate-invalidation`
appended so the canonical reload → invalidate → prewarm path is
exercised in addition to the steady-state search measurement.

## Environment

- Host: single Linux node (8 CPUs, 31 GiB RAM, ~600 GiB free disk)
- Python: 3.12 venv at `$HOME/venv-bench`
- pylance: 6.0.0, built editable from
  `~/git/lance-open-source` branch `private-cache-6.0-ver-1`
  (`9ebfe4de0` or newer)
- lance-ray: 0.4.2 editable from `~/git/lance-ray-open-source`
- Ray: 2.55.1 (local instance started by `ray.init`)
- Dataset URI: `/tmp/bench.lance` (local NVMe)
- Date: 2026-06-01

## Configuration

```
--scale 1000000  --dim 128
--num-partitions 256  --num-sub-vectors 16
--num-actors 4  --index-cache-mb 2048
--k 10  --nprobes 16
--measure-queries 500
--simulate-invalidation
```

Per actor: ~64 owned partitions, 2 GiB cache budget (total ~8 GiB
across 4 actors — fits comfortably in 23 GiB available RAM).

## Results

| Metric | Value |
|---|---|
| Prewarm wall (4 actors, parallel) | 1.37 s |
| End-to-end search latency (n=500) — p50 | 4.64 ms |
| End-to-end search latency — p99 | 6.20 ms |
| End-to-end search latency — mean | 4.73 ms |
| Invalidate + re-prewarm wall | 0.03 s |
| Post-rehydrate 100-query wall | 0.48 s |

Per-actor probe latency (from `IvfShardActor.stats()`):

| Actor | Owned parts | Probes | Probe p50 (ms) | Probe p99 (ms) | Wrong routes |
|---|---|---|---|---|---|
| 0 | 64 | 500 | 2.62 | 3.20 | 0 |
| 1 | 64 | 500 | 2.47 | 3.13 | 0 |
| 2 | 64 | 500 | 2.36 | 3.20 | 0 |
| 3 | 64 | 500 | 2.32 | 3.19 | 0 |

Routing invariant holds (`wrong_partition_probes_total == 0` on every
actor), so each shard only probed its own partition slice.

## Files

- `smoke_test.txt` — output of
  `examples/distributed_ivf_cache.py` (Phase 0 sanity check that the
  three pylance hooks `prewarm_index`, `invalidate_index_cache`,
  `scanner(nearest=…, partition_ids=…)` are present and the
  prewarm → search → invalidate → re-search loop works on a 1024×16
  toy dataset).
- `run_distributed_bench.txt` — raw stdout/stderr of the 1M-row
  benchmark run (includes the Ray-deduped pylance deprecation warning
  about `_distance` autoprojection; harmless for this benchmark, the
  driver explicitly requests `columns=["id"]`). The `.log` extension
  is gitignored repo-wide, so the artifact uses `.txt`.
