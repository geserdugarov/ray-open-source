# Distributed IVF cache benchmark on Ray

Step-by-step instructions for benchmarking the Ray-sharded distributed
IVF vector-index cache. The cache itself is **not** in this Ray
project anymore — it was refactored out into the standalone
[`lance-ray`](https://github.com/lancedb/lance-ray) package, which
ships the `IvfShardActor` / `DistributedAnnSearch` /
`InvalidateOrchestrator` primitives this benchmark exercises. The
companion `lance` (pylance) build supplies the three Lance entry
points the actors call into.

The historical Ray-side benchmark subtree
(`benchmarks/lance_hybrid_cache/`) is gone — see the
[`cache-lance-ray-only`](https://github.com/geserdugarov/ray-open-source/tree/cache-lance-ray-only/benchmarks/lance_hybrid_cache)
tag for the v6 NVMe+DRAM benchmark that targeted the old
`Session.with_distributed_cache` API. v1 of the lance-ray-side cache
is **RAM-only L1** (per-actor pylance `Session(index_cache_size_bytes=...)`
moka cache); NVMe L2 is deferred to Phase 2.

## What you are measuring

- **Workload**: ANN search over a Lance dataset with an IVF vector
  index (`IVF_PQ` or `IVF_RQ`). Each Ray actor owns the partition slice
  `partition_id % num_actors` and probes only that slice; the
  coordinator broadcasts the query and merges per-actor partial
  top-K into a global top-K.
- **Cache surface**: `lance_ray.distributed_cache.IvfShardActor`
  (a `ray.remote`-wrapped owner of one shard) plus
  `DistributedAnnSearch` (broadcast + merge) and
  `InvalidateOrchestrator` (reload → invalidate → prewarm on index
  update). See `lance_ray/distributed_cache/__init__.py` for the
  exported API.
- **Pylance contract** (private branch — not on PyPI):
  - `LanceDataset.prewarm_index(name, *, partition_ids=...)` —
    warm the actor's owned partitions only.
  - `LanceDataset.invalidate_index_cache(index_addr)` — drain the
    previous-uuid partition entries after an index update.
  - `LanceDataset.scanner(nearest={..., "partition_ids": [...]})` —
    restrict the IVF Stage A centroid candidate set to the actor's
    owned subset.
- **Signals**: per-query latency (p50/p99 over the broadcast +
  per-actor probe + merge), per-actor `prewarm_obs_seconds`, and
  per-actor `wrong_partition_probes_total` from `IvfShardActor.stats()`.
  Pylance 6.0 does not expose per-Session hit/miss counters, so the
  routing-invariant counter is the closest cache-effectiveness proxy.

## 1. Build pylance from the distributed-cache branch

The three Phase 0 entry points above are not on PyPI yet. Build
`pylance` from `lance-open-source` on branch `private-cache-6.0-ver-1`
(commit `9ebfe4de0` or newer):

```bash
# Activate the same venv you intend to run the benchmark from.
python3.12 -m venv "$HOME/venv-bench"
source "$HOME/venv-bench/bin/activate"
pip install -U pip maturin

# pylance — distributed-cache branch
cd "$HOME/git/lance-open-source"
git checkout private-cache-6.0-ver-1
cd python
pip install -e .
# maturin builds in release mode under PEP 517; do NOT use
# `maturin develop` without --release before benchmarking.
```

Verify the three entry points are present before installing lance-ray
(the `IvfShardActor.prewarm` / `.invalidate` / `.probe` methods raise
a clear `RuntimeError` on first use if any is missing, but checking
upfront avoids spinning up Ray to discover that):

```bash
python - <<'PY'
import lance, inspect
ds_cls = lance.LanceDataset
assert hasattr(ds_cls, "prewarm_index"), "missing prewarm_index"
sig = inspect.signature(ds_cls.prewarm_index)
assert "partition_ids" in sig.parameters, "prewarm_index lacks partition_ids kwarg"
assert hasattr(ds_cls, "invalidate_index_cache"), "missing invalidate_index_cache"
print("pylance APIs OK; version:", lance.__version__)
PY
```

## 2. Install lance-ray

`lance-ray` carries the `distributed_cache` module the benchmark
drives. Install it from the local checkout so you can iterate on the
actor / coordinator code:

```bash
cd "$HOME/git/lance-ray-open-source"
pip install -e .
# Pulls ray[data], pyarrow, pylance>=6.0 — pylance is already pinned
# to your local editable install from step 1, so pip will keep it.
```

Confirm the API surface:

```bash
python -c "from lance_ray.distributed_cache import (
    IvfShardActor, DistributedAnnSearch, InvalidateOrchestrator,
    PartitionRouter, ActorConfig, SearchConfig); print('lance-ray cache OK')"
```

## 3. Smoke-test with the lance-ray example

`lance-ray` ships an end-to-end example that builds a tiny IVF_PQ
dataset, prewarms each shard, runs one distributed search, drives an
invalidate/re-prewarm, and searches again. Run it first to confirm
the full pipeline works on your machine — if this passes, the
benchmark below will run unchanged:

```bash
python "$HOME/git/lance-ray-open-source/examples/distributed_ivf_cache.py"
```

Expected output (excerpt):

```
INFO: Phase 1: prewarm each actor's owned slice
INFO:   actor 0 owns 2 partitions (prewarm_obs_seconds=...)
INFO:   actor 1 owns 2 partitions (prewarm_obs_seconds=...)
INFO: Phase 2: distributed ANN search
INFO:   top-5 ids=[...] distances=[...]
INFO: Phase 3: invalidate + re-prewarm on index update
INFO: Phase 4: search again after invalidate
INFO:   fresh search returned 5 rows
```

If the example logs `Skipping: this pylance build is missing a
required distributed-cache API`, go back to step 1 — your pylance
build is not on the distributed-cache branch.

## 4. Scale up to a benchmark workload

The example uses 1024 rows × 16 dims × 4 partitions for a fast smoke
test. To measure latency at a realistic scale, point the same
primitives at a larger dataset. The driver script that does this is
checked in at [`./benchmark/run_distributed_bench.py`](./run_distributed_bench.py);
it follows the example's lifecycle but instruments per-query latency
across a measure window. The script source is reproduced below for
reference (edit the file in-tree rather than re-pasting if you want to
tweak the lifecycle):

```python
# benchmark/run_distributed_bench.py — distributed IVF cache benchmark
import argparse
import os
import statistics
import time
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa
import ray
from lance_ray.distributed_cache import (
    DistributedAnnSearch,
    InvalidateOrchestrator,
    IvfShardActor,
    PartitionRouter,
)

def build_dataset(uri, n_rows, dim, n_parts, n_subv, seed=7):
    if Path(uri).exists():
        return lance.dataset(uri).list_indices()[0]["uuid"]
    rng = np.random.default_rng(seed)
    values = pa.array(rng.standard_normal(n_rows * dim, dtype=np.float32))
    vec = pa.FixedSizeListArray.from_arrays(values, dim)
    tbl = pa.Table.from_arrays(
        [vec, pa.array(range(n_rows), type=pa.int64())],
        names=["vector", "id"],
    )
    lance.write_dataset(tbl, uri, max_rows_per_file=max(n_rows // 32, 1))
    ds = lance.dataset(uri)
    ds.create_index(
        "vector", "IVF_PQ",
        name="vec_idx",
        num_partitions=n_parts,
        num_sub_vectors=n_subv,
    )
    return next(i for i in ds.list_indices() if i["name"] == "vec_idx")["uuid"]

def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), int(round(pct / 100.0 * len(ordered)))))
    return ordered[rank - 1]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--uri", default="/tmp/bench.lance")
    p.add_argument("--scale", type=int, default=1_000_000)
    p.add_argument("--dim", type=int, default=128)
    p.add_argument("--num-partitions", type=int, default=256)
    p.add_argument("--num-sub-vectors", type=int, default=16)
    p.add_argument("--num-actors", type=int, default=4)
    p.add_argument("--index-cache-mb", type=int, default=2048)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--nprobes", type=int, default=16)
    p.add_argument("--measure-queries", type=int, default=500)
    p.add_argument("--simulate-invalidation", action="store_true")
    p.add_argument(
        "--ray-address",
        default=os.environ.get("RAY_ADDRESS"),
        help="Ray cluster address, e.g. '10.42.0.10:6379' or 'auto' to attach "
             "to a running cluster on this host. Defaults to $RAY_ADDRESS; "
             "leave unset to start a local Ray instance.",
    )
    p.add_argument(
        "--actor-resource",
        default=None,
        help="Optional Ray custom resource name required by each "
             "IvfShardActor. Use this on a real cluster to pin actors to "
             "nodes that declared the resource via "
             "`ray start --resources='{\"<name>\": 1}'`. Each actor reserves "
             "1.0 of the resource via IvfShardActor.options(resources=...).",
    )
    args = p.parse_args()

    print(f"[setup] building/loading dataset at {args.uri}")
    index_uuid = build_dataset(
        args.uri, args.scale, args.dim,
        args.num_partitions, args.num_sub_vectors,
    )

    # When --ray-address (or $RAY_ADDRESS) is set, attach to that cluster
    # instead of starting a local Ray instance. `address="auto"` is the
    # supported way to attach to a Ray head running on the same host.
    ray.init(address=args.ray_address, ignore_reinit_error=True)
    try:
        actor_factory = IvfShardActor
        if args.actor_resource:
            # Resources MUST be passed to .options(...), not .remote(...).
            # Args to .remote(...) become positional/keyword args to the
            # actor's __init__ — so `resources=` there would raise TypeError.
            actor_factory = IvfShardActor.options(
                resources={args.actor_resource: 1.0},
            )
        actors = [
            actor_factory.remote(
                dataset_uri=args.uri,
                actor_index=i,
                num_actors=args.num_actors,
                index_name="vec_idx",
                index_cache_size_bytes=args.index_cache_mb * 1024 * 1024,
            )
            for i in range(args.num_actors)
        ]

        print(f"[prewarm] {args.num_actors} actors prewarming owned slices")
        t0 = time.perf_counter()
        ray.get([a.prewarm.remote(None) for a in actors])
        print(f"[prewarm] wall={time.perf_counter()-t0:.2f}s")

        owned = [
            PartitionRouter(args.num_actors).owned_by(i, args.num_partitions)
            for i in range(args.num_actors)
        ]
        search = DistributedAnnSearch(actors=actors, owned_ids_per_actor=owned)

        rng = np.random.default_rng(11)
        queries = rng.standard_normal((args.measure_queries, args.dim)).astype(np.float32)

        print(f"[measure] {args.measure_queries} queries, k={args.k}, nprobes={args.nprobes}")
        latencies_ms = []
        for q in queries:
            t = time.perf_counter()
            search.search(q, k=args.k, nprobes=args.nprobes,
                          metric="l2", columns=["id"])
            latencies_ms.append((time.perf_counter() - t) * 1000.0)
        print(f"[measure] p50={percentile(latencies_ms, 50):.2f}ms "
              f"p99={percentile(latencies_ms, 99):.2f}ms "
              f"mean={statistics.mean(latencies_ms):.2f}ms "
              f"n={len(latencies_ms)}")

        for s in ray.get([a.stats.remote() for a in actors]):
            print(f"  actor {s['actor_index']}: owned={s['owned_partition_count']} "
                  f"probes={s['probe_count']} "
                  f"probe_p50_ms={s['probe_latency_p50_ms']} "
                  f"probe_p99_ms={s['probe_latency_p99_ms']} "
                  f"wrong_routes={s['wrong_partition_probes_total']}")

        if args.simulate_invalidation:
            print("[invalidate] driving reload → invalidate → prewarm")
            t = time.perf_counter()
            InvalidateOrchestrator(actors=actors).on_index_update(
                old_index_addr=index_uuid, new_index_name="vec_idx",
            )
            print(f"[invalidate] wall={time.perf_counter()-t:.2f}s")
            t = time.perf_counter()
            for q in queries[: args.measure_queries // 5]:
                search.search(q, k=args.k, nprobes=args.nprobes,
                              metric="l2", columns=["id"])
            print(f"[invalidate] post-rehydrate {args.measure_queries // 5} "
                  f"queries wall={time.perf_counter()-t:.2f}s")
    finally:
        for a in actors:
            ray.kill(a, no_restart=True)
        ray.shutdown()

if __name__ == "__main__":
    main()
```

Run it (local Ray, single host — defaults give a ~5 GB float32
dataset at 1M × 128 dims):

```bash
python ./benchmark/run_distributed_bench.py \
    --uri /mnt/data/bench.lance \
    --scale 1000000 --dim 128 \
    --num-partitions 256 --num-sub-vectors 16 \
    --num-actors 4 --index-cache-mb 2048 \
    --k 10 --nprobes 16 \
    --measure-queries 500
```

Add `--simulate-invalidation` on the first run against a new pylance
build — it drives the canonical reload → invalidate → prewarm
sequence end-to-end and rehydrates the cache, so it catches Lance-side
regressions in the invalidate / prewarm contract that the smoke test
above does not exercise.

Single-node run artifacts (raw stdout, the smoke-test log, and a
per-actor results summary) are saved under
[`./benchmark/results/`](./results/) — see
[`results/README.md`](./results/README.md) for the configuration that
produced the committed numbers and the host/RAM budget used.

## 5. Optional: object-store backend

The smoke test and the benchmark above both store the dataset on local
disk. To benchmark the cache against object storage (S3 / MinIO),
write the dataset to `s3://bucket/path` and pass the bucket as
`--uri`. `IvfShardActor` accepts a `storage_options` dict; expose it
from the driver by replacing the constructor call with:

```python
# storage_options is a constructor argument on IvfShardActor, so it
# goes on .remote(...). If you also want to pin the actor to a Ray
# resource, wrap with .options(resources=...) — never put resources
# on .remote(), because .remote() forwards everything to __init__.
(
    IvfShardActor
    .options(resources={"search_actor_node": 1.0})  # optional placement
    .remote(
        dataset_uri="s3://bucket/bench.lance",
        actor_index=i,
        num_actors=args.num_actors,
        index_name="vec_idx",
        index_cache_size_bytes=args.index_cache_mb * 1024 * 1024,
        storage_options={"endpoint": "http://minio:9000",
                         "access_key_id": "...", "secret_access_key": "..."},
    )
)
```

`build_dataset` also needs `lance.write_dataset(..., storage_options=...)`
and `lance.dataset(..., storage_options=...)`. With object storage as
the source of truth, prewarm latency reflects real fetch cost and the
post-prewarm steady-state shows the per-actor moka-cache win.

## 6. Multi-node placement

For physical multi-node placement (one head + N actor nodes), start
Ray with custom resources on each actor node, attach the driver to
the head, and pin actors with `IvfShardActor.options(resources=...).remote(...)`.
The lance-ray module does not pin actors itself, so placement is the
driver's responsibility.

### 6.1 Start Ray on every node

On the head node (replace `10.42.0.10` with the head's LAN IP):

```bash
ray start --head \
    --node-ip-address=10.42.0.10 \
    --port=6379 \
    --dashboard-host=0.0.0.0
```

On each actor node, attach to the head and advertise one unit of a
custom resource that the driver will require per actor (use a single
name for every actor node so any actor can land on any actor host):

```bash
ray start --address=10.42.0.10:6379 \
    --node-ip-address=10.42.0.11 \
    --resources='{"search_actor_node": 1}'
```

Verify the cluster sees all nodes from the head:

```bash
ray status   # or: ray list nodes
```

### 6.2 Attach the driver to the running cluster

Run the benchmark from the head node (or any host that can reach
`10.42.0.10:6379`). The driver attaches to the existing cluster
instead of starting a local Ray instance — pass `--ray-address` or
export `RAY_ADDRESS` before invoking the script. Use
`--actor-resource` to require the custom resource on every
`IvfShardActor`; the driver wires that into
`IvfShardActor.options(resources={"<name>": 1.0}).remote(...)`, which
is the correct way to pass actor resources in Ray (passing
`resources=` directly to `.remote(...)` would forward it to the actor
constructor and raise `TypeError`).

```bash
export RAY_ADDRESS=10.42.0.10:6379
# Or: RAY_ADDRESS=auto when running on the head itself.

python ./benchmark/run_distributed_bench.py \
    --ray-address "$RAY_ADDRESS" \
    --actor-resource search_actor_node \
    --uri s3://bucket/bench.lance \
    --scale 10000000 --dim 1024 \
    --num-partitions 1024 --num-sub-vectors 32 \
    --num-actors 4 --index-cache-mb 4096 \
    --k 10 --nprobes 32 \
    --measure-queries 1000
```

`--num-actors N` with `--actor-resource search_actor_node` schedules
N actors across nodes that each advertised
`"search_actor_node": 1` — one actor per actor node when N equals the
node count, or stack multiple actors per node by advertising
`"search_actor_node": <K>` on that node's `ray start`.

See the historical
[`REAL_CLUSTER.md`](https://github.com/geserdugarov/ray-open-source/blob/cache-lance-ray-only/benchmarks/lance_hybrid_cache/REAL_CLUSTER.md)
for a worked example of the firewall / port / venv layout — every
section except the v6-specific `Session.with_distributed_cache` /
NVMe L2 directory bits still applies.

## Caveats and known limitations

- **v1 is RAM-only.** There is no NVMe L2 tier in the lance-ray-side
  cache. Sizing the per-actor `index_cache_size_bytes` against
  `owned_partition_count * partition_size` is the only knob; if the
  slice spills, queries pay an OBS round-trip per evicted partition.
  Phase 2 (NVMe L2) needs two more pylance hooks
  (`dump_index_partition` / `load_index_partition`) and is not yet on
  the lance-open-source branch.
- **Broadcast-to-all-actors search.** `DistributedAnnSearch` sends
  every query to every actor. Recall is exact (each actor probes
  only its owned subset; merge picks global top-K), but per-query
  scatter cost grows with `num_actors`. A centroid-aware coordinator
  routing variant is deferred to Phase 2 (`plans/distributed-cache-lance-ray-side.md`
  §4.5).
- **No hit/miss counters.** Pylance 6.0 does not expose per-Session
  cache statistics. The benchmark reports `wrong_partition_probes_total`
  (routing invariant) and per-actor `probe_latency_p99_ms`; treat the
  warm vs. cold p99 gap as the cache-effectiveness signal.
- **Single-replica.** No worker-level failover. If an `IvfShardActor`
  dies, its partition slice is unavailable until you restart the
  cluster.

## References

- lance-ray distributed cache plan:
  `~/git/lance-ray-open-source/plans/distributed-cache-lance-ray-side.md`
- lance-ray example: `~/git/lance-ray-open-source/examples/distributed_ivf_cache.py`
- Lance branch with the three Phase 0 entry points:
  `~/git/lance-open-source`, branch `private-cache-6.0-ver-1`
- Historical Ray-side v6 benchmark (removed from this repo, preserved
  at tag `cache-lance-ray-only`):
  `benchmarks/lance_hybrid_cache/README.md` and `REAL_CLUSTER.md`
