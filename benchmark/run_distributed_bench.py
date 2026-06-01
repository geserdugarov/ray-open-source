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
