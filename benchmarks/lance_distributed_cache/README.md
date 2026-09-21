# Lance 12 distributed-cache benchmark

This benchmark measures Lance's two-level IVF partition cache from Ray actors.
It targets Lance commit `bf6fa8d75` (`feat(cache): two-level cache`), with the
public behavior documented by `e5375e298` and `ee7c7c6c2`.

The benchmark compares three per-actor sessions on the same IVF_RQ index and
the same deterministic query vectors:

| Scenario | Lance session | Durable across actor restart |
|---|---|---|
| `no-index-cache` | `Session(index_cache_size_bytes=0)` | No |
| `moka` | byte-sized in-memory index cache | No |
| `distributed` | metadata L1, optional decoded-partition L1, local-file L2 | Yes |

Each actor owns a distinct L2 directory. Ray controls process and node
placement; Lance owns cache loading, persistence, and invalidation.

## What was checked in Lance 12

The implementation and its documentation establish these benchmark contracts:

- `Session.with_distributed_cache` takes
  `index_metadata_l1_capacity_bytes` and the keyword
  `l1_partition_capacity_bytes`. The latter is `None` when decoded-partition
  L1 is disabled.
- Only current-format serializable IVF `PartitionEntry` values persist to L2.
  Index state and other metadata remain in memory.
- `dataset.prewarm_index(name)` is best effort. Supplying
  `partition_ids` selects the strict path; `partition_ids=[]` means every
  partition and propagates persistence failures.
- `Session.invalidate_index_cache(dataset_uri, index_addr)` invalidates one
  exact index namespace and fences in-flight publishers.
- One live process exclusively locks each L2 directory. L2 has no built-in
  capacity manager, so the operator owns disk sizing and cleanup.
- Persistent keys are intentionally opaque:

  ```text
  <l2-dir>/
    lance-distributed.lock
    blake3-128-v1/v1/
      .tombstones.json
      <64-hex-namespace>/
        <32-hex-entry-key>.bin
  ```

  The benchmark reports aggregate entry counts and bytes. It does not infer a
  partition ID from an opaque filename.
- The Rust backend has L2 counters, but commit `bf6fa8d75` does not expose them
  through Python. Python exposes `Session.size_bytes()` for the in-memory
  footprint, so latency plus read-only filesystem snapshots are the available
  external signals.
- That commit does not expose Python `compute_partition_ids` or
  `search_partitions` methods. This benchmark consequently measures ordinary
  full Lance queries in replicated actors. It does not pretend to benchmark a
  coordinator-routed, partition-sharded query path.

These details differ materially from the Lance 7 benchmark in Ray commit
`5142489f5b`: that suite used `moka_l1_partition_bytes`, reversible
`part-ivf-<id>.bin` filenames, and private sharded-query wrappers. None of those
assumptions are carried forward here.

## Measurement phases

For every scenario, actors run concurrently and process disjoint slices of the
same query set:

1. Prewarm: `strict` (default), `best-effort`, `natural`, or `none`.
2. `initial`: first measured pass after prewarm.
3. `l1_warm`: identical queries in the same process and session.
4. `process_restart`: actors and sessions are recreated on the same Ray nodes.
   The `distributed` scenario keeps its L2 directory; the other scenarios lose
   their index-cache state.
5. Optional `post_invalidation_rehydrate`: invalidate every physical index
   segment, verify that the dedicated L2 has no live namespaces or non-empty
   tombstones, strictly prewarm again, and repeat the measurement.

The summary includes per-query p50/p95/p99/mean latency, aggregate throughput,
summed `Session.size_bytes()`, and aggregate live L2 bytes and entry count.
Raw actor events preserve prewarm, restart, and invalidation timings.

## Environment

Use a virtual environment. The benchmark requires a pylance wheel built from
the cache branch; the released `12.0.0` version string alone does not prove the
post-tag cache commit is present.

Build Lance according to its `python/AGENTS.md` workflow, then install the
resulting wheel into a dedicated benchmark environment:

```bash
cd ../lance-open-source/python
make install
uv run maturin build --profile release-with-debug --out dist

cd ../../ray-open-source
uv venv benchmarks/lance_distributed_cache/.venv
uv pip install \
  --python benchmarks/lance_distributed_cache/.venv/bin/python \
  -r benchmarks/lance_distributed_cache/requirements.txt \
  ../lance-open-source/python/dist/pylance-12.0.0-*.whl
```

Before a long run, verify the exact surface in that environment:

```bash
benchmarks/lance_distributed_cache/.venv/bin/python - <<'PY'
import inspect
import lance

print(lance.__version__)
print(inspect.signature(lance.Session.with_distributed_cache))
print(inspect.signature(lance.LanceDataset.prewarm_index))
print(inspect.signature(lance.Session.invalidate_index_cache))
PY
```

The constructor must show `l1_partition_capacity_bytes`, and prewarm must show
`partition_ids`.

## Local MinIO run

Start MinIO and create the default bucket:

```bash
benchmarks/lance_distributed_cache/infra/run_minio.sh up
```

Choose a real local disk for `--nvme-dir`; avoid tmpfs and shared/network
filesystems. A small functional run is:

```bash
benchmarks/lance_distributed_cache/.venv/bin/python \
  benchmarks/lance_distributed_cache/run_benchmark.py \
  --scale 100000 \
  --dim 128 \
  --num-partitions 64 \
  --num-bits 2 \
  --num-actors 2 \
  --metadata-l1-mb 16 \
  --partition-l1-mb 32 \
  --warmup-queries 16 \
  --measure-queries 50 \
  --k-list 10 \
  --nprobes 8 \
  --nvme-dir /data/nvme/lance-cache-bench \
  --invalidation-drill
```

For a performance run, the defaults reproduce the prior benchmark's broad
shape: 10 million 1024-dimensional vectors, 3000 IVF_RQ partitions, and 8-bit
codes. Dataset and index setup can take a long time. Later invocations reuse the
content-addressed URI automatically; `--skip-setup` skips the existence check
and setup entirely.

Useful variants:

```bash
# Exercise query-driven population rather than whole-index strict prewarm.
.../run_benchmark.py --prewarm natural ...

# Isolate the persistent tier by disabling decoded-partition L1.
.../run_benchmark.py --partition-l1-mb 0 ...

# Strictly persist a known subset (all is encoded as an empty Python list).
.../run_benchmark.py --prewarm strict --prewarm-partitions 0,17,42 ...

# Reattach to an intentionally retained L2 run directory.
.../run_benchmark.py --l2-run-id known-warm-run --skip-setup ...
```

Every automatic run ID is unique, so the first distributed prewarm starts from
a new directory without deleting user data. Reusing an explicit run ID is an
operator choice. The benchmark never removes an L2 directory.

## Multi-node Ray runs

Install the same Ray and pylance builds on every node and use a dataset URI
reachable from every actor. Then pass `--ray-address auto` (or set
`RAY_ADDRESS`). The driver records each actor's Ray node ID and pins restarted
actors back to those exact nodes so `process_restart` reads the same node-local
L2. Use `--actor-resource NAME` if initial actors must be restricted to nodes
that advertise local NVMe.

`--nvme-dir` is interpreted independently on every worker. Actor `i` uses:

```text
<nvme-dir>/<run-id>/actor-<i>
```

Do not place two live actors on the same final directory and do not use a
shared L2 directory across nodes.

For a local MinIO latency experiment, `infra/netem_up.sh` delays only traffic
to port 9000, leaving Ray's loopback control traffic alone. Review the script
and run `infra/netem_down.sh` after the experiment.

## Outputs

Each run writes `<out-dir>/<run-id>/`:

- `run.json`: exact arguments, dataset identity, versions, and session specs;
- `events.jsonl`: per-actor info, prewarm, measurement, and invalidation data;
- `summary.csv`: one aggregate row per scenario, phase, and `k`.

Use `plot_results.py <summary.csv>` to create latency and throughput plots.

## Interpreting results

- Compare `process_restart` across scenarios to isolate the benefit of
  persistent L2. Moka and disabled-cache actors restart cold; distributed
  actors retain opaque partition files.
- Compare `initial` with `l1_warm` to see decoded in-process reuse.
- With `--partition-l1-mb 0`, a distributed restart and every subsequent miss
  decode from L2, which isolates local-file behavior from decoded L1.
- Strict-prewarm duration is an end-to-end persistence measurement. A second
  strict prewarm on an existing L2 validates and skips valid entries, so its
  timing is not comparable to first population.
- `Session.size_bytes()` is not an L2 size and is not a hit ratio.
- MinIO on the same host, Linux page cache, CPU oversubscription, and an NVMe
  shared by several actors can dominate results. Report topology, thread
  settings, storage placement, and the full `run.json` with measurements.

## Tests

The fast tests use stubs and synthetic cache directories:

```bash
python/venv/bin/python -m unittest discover \
  -s benchmarks/lance_distributed_cache -p 'test_*.py' -v
```

When the exact Lance build is importable, the integration test also creates a
small IVF_RQ index and checks strict prewarm, process restart, query execution,
and exact invalidation. It skips cleanly when that API is unavailable.
