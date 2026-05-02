"""Distributed Lance hybrid-cache benchmark.

Spawns N Ray actors, each holding its own Lance Session on a unique L2
subdirectory under `--nvme-dir`. Optionally prewarms (per-actor, in
parallel), then partitions the measure queries across actors and runs
them concurrently. Reports aggregate latency percentiles plus per-actor
stats so you can see whether cache state diverges by worker.

Differences from `run_bench.py`:

* Multi-actor topology — `--num-actors N`. Each actor is a `HybridSearchActor`
  (see `distributed_actor.py`), not the single-shot `ScenarioActor`.
* DRAM/L2 budgets are **per-actor**, not aggregate. With `--num-actors 4
  --dram-gb 1 --l2-gb 8` total resource use is 4 GiB DRAM + 32 GiB NVMe.
  Sized this way intentionally — to compare against `run_bench.py`'s 4 GiB /
  30 GiB single-actor run, set the per-actor budgets to 1 GiB / 8 GiB.
* Adds `--prewarm forced` which calls Lance's `dataset.prewarm_index(name)`
  on every actor in parallel — this is the distributed-prewarm path that
  `run_bench.py`'s natural warmup does not exercise.
* Adds `--prewarm sharded` which assigns disjoint partition slices to
  actors (round-robin `partition_id % num_actors`) and routes each actor
  through `dataset.prewarm_vector_cache(...)` with a deterministic
  placement policy picked from `--scenario`: `hybrid_tiered` places every
  owned vector partition into L2 and intentionally leaves foyer L1 cold
  (DRAM is filled later by ordinary query traffic promoting decoded
  partitions out of L2 — there is no L1→L2 writeback for vector
  partitions); `moka_ram_cap` loads until `--dram-gb` is full and stops;
  `no-cache` is a no-op that just registers ownership. Per-actor prewarm
  cost stays flat as `--num-actors` grows, but per-query recall becomes
  partial in `--mode replicated` since results are not merged across
  actors — this is a benchmark of the per-actor primitives, not of
  full-recall cluster search.
* Adds `--mode sharded` which spins up a `CoordinatorActor` to scatter-
  gather `search_partitions` across the same partition-sharded workers
  and merge per-query top-K — full recall, with the slowest fan-out leg
  setting per-query wall-time. Forces `--prewarm=sharded`. The default
  `--mode replicated` keeps the existing every-actor-sees-everything
  layout for A/B comparison.
* Adds optional Ray custom-resource placement flags so real cluster runs
  can pin the coordinator to one node and workers to separate actor nodes.
* Reports both aggregate (across actors) and per-actor latency tables, plus
  cache stats per actor, so you can see whether queries fan-out evenly.

Caveats on a single-machine cluster:

* All actors share one NVMe, one MinIO process, and one CPU pool. With
  `--num-actors 4` on a 12-CPU host you have headroom; `--num-actors 12`+
  will see CPU contention distorting latencies. This bench measures the
  Ray-side fan-out *plumbing*, not a real multi-node cluster.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import ray

from scenarios import GIB, MIB, build_scenario_specs

HERE = Path(__file__).resolve().parent

from _hybrid_cache_helpers import (  # noqa: E402
    DatasetSpec,
    ensure_dataset,
    format_latency_row,
    make_query_vectors,
    percentiles,
)

from check_partition_residency import (  # noqa: E402
    DEFAULT_NAMESPACE,
    run_residency_check,
    write_residency_jsonl,
)
from distributed_actor import CoordinatorActor, HybridSearchActor  # noqa: E402
from l2_inspect import format_bytes  # noqa: E402


def _deterministic_prewarm_params(
    scenario: str,
    dram_bytes: int,
) -> tuple[str, int]:
    """Map ``scenario`` → ``(policy, ram_bytes)`` for sharded prewarm.

    * moka — ``policy='moka_ram_cap'``, ``ram_bytes`` is the full
      per-actor DRAM budget (Moka has no L2 tier).
    * hybrid — ``policy='hybrid_tiered'``, which places every requested
      partition into L2 and never admits anything to L1. Lance's
      ``hybrid_tiered`` ignores ``ram_bytes`` (the orchestrator does
      not consult any L1 budget), so we pass ``0`` for clarity —
      L1 stays cold by construction and ordinary query traffic is what
      promotes decoded partitions out of L2.
    """
    if scenario == "moka":
        return "moka_ram_cap", int(dram_bytes)
    if scenario == "hybrid":
        return "hybrid_tiered", 0
    raise ValueError(
        f"_deterministic_prewarm_params: scenario={scenario!r} has no "
        "deterministic prewarm policy"
    )


def _post_prewarm_l1_baseline(
    *,
    scenario: str,
    prewarm: str,
    num_actors: int,
    pre_measure_residency: List[Dict[str, Any]],
) -> tuple[Dict[int, set[int]], str | None]:
    """Return the L1 baseline used by the post-measure shift report.

    The preferred baseline is the optional post-prewarm residency probe. For
    hybrid sharded prewarm, we can still report movement without running that
    probe: ``hybrid_tiered`` is validated above to admit zero vector partitions
    into L1, so the post-prewarm L1 set is empty by construction.
    """
    if pre_measure_residency:
        observed = {
            int(r["actor_id"]): set(int(p) for p in r["in_l1"])
            for r in pre_measure_residency
        }
        return (
            observed,
            "observed by post-prewarm probe",
        )
    if prewarm == "sharded" and scenario == "hybrid":
        return (
            {i: set() for i in range(num_actors)},
            "inferred empty from hybrid_tiered prewarm",
        )
    return {}, None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    # Dataset (must match the URI you intend to reuse via --skip-setup).
    p.add_argument("--scale", type=int, default=10_000_000)
    p.add_argument("--dim", type=int, default=1024)
    p.add_argument("--num-partitions", type=int, default=3000)
    p.add_argument("--num-bits", type=int, default=8)

    # Topology + per-actor caches.
    p.add_argument(
        "--mode",
        type=str,
        default="replicated",
        choices=["replicated", "sharded"],
        help=(
            "replicated=every actor sees every partition (existing layout); "
            "queries are split round-robin by the driver and each actor "
            "answers independently. "
            "sharded=spawn a CoordinatorActor that owns the IVF centroid "
            "step and a partition→actor mapping (id %% num_actors); "
            "per-query top-K is gathered and merged across actors via "
            "search_partitions, so recall is full but per-query wall-time "
            "is bounded below by the slowest fan-out leg. Forces "
            "--prewarm=sharded (workers must own a known partition slice "
            "before routing). See README §Coordinator-driven sharded mode."
        ),
    )
    p.add_argument(
        "--scenario",
        type=str,
        default="hybrid",
        choices=["no-cache", "moka", "hybrid"],
        help="All actors share the same scenario.",
    )
    p.add_argument(
        "--num-actors",
        type=int,
        default=4,
        help="Parallel Ray actors, each with its own Session and L2 subdir.",
    )
    p.add_argument(
        "--dram-gb",
        type=float,
        default=1.0,
        help="DRAM budget PER ACTOR (GiB). Aggregate = num_actors × dram-gb.",
    )
    p.add_argument(
        "--codecless-mb",
        type=int,
        default=None,
        help="PER ACTOR codec-less Moka budget (MiB) for the hybrid scenario. "
        "When set, switches each actor's session to with_hybrid_cache_advanced; "
        "foyer L1 = --dram-gb − --codecless-mb. Total per-actor DRAM stays "
        "at --dram-gb, so a moka vs hybrid comparison at the same --dram-gb "
        "stays apples-to-apples on DRAM.",
    )
    p.add_argument(
        "--l2-gb",
        type=float,
        default=8.0,
        help="L2 NVMe budget PER ACTOR (GiB). Aggregate = num_actors × l2-gb.",
    )
    p.add_argument(
        "--metadata-mb",
        type=float,
        default=None,
        help="Lance metadata cache PER ACTOR (MiB). Default uses Lance's default.",
    )
    p.add_argument(
        "--nvme-dir",
        type=str,
        default="/mnt/nvme/lance-l2/distributed",
        help="Parent of per-actor L2 subdirs (`<nvme-dir>/actor-<i>`).",
    )
    p.add_argument(
        "--actor-resource",
        type=str,
        default=None,
        help=(
            "Optional Ray custom resource name required by each HybridSearchActor. "
            "Use with `ray start --resources` to pin workers to physical actor "
            "nodes. Each actor reserves 1.0 of this resource."
        ),
    )
    p.add_argument(
        "--coordinator-resource",
        type=str,
        default=None,
        help=(
            "Optional Ray custom resource name required by the CoordinatorActor "
            "in --mode sharded. Use with `ray start --resources` to pin the "
            "coordinator to the head/coordinator node. The coordinator reserves "
            "1.0 of this resource."
        ),
    )

    # Prewarm strategy.
    p.add_argument(
        "--prewarm",
        type=str,
        default="natural",
        choices=["natural", "forced", "sharded", "none"],
        help=(
            "natural=split warmup queries across actors; "
            "forced=every actor calls dataset.prewarm_index(...) in parallel; "
            "sharded=actor i deterministically prewarms partitions "
            "{i, i+N, i+2N, ...} via dataset.prewarm_vector_cache(...) "
            "with policy chosen from --scenario: hybrid_tiered for "
            "--scenario hybrid (places every owned partition into L2 "
            "and leaves foyer L1 cold — query traffic later promotes "
            "decoded partitions out of L2 into volatile L1, with no L1→L2 "
            "writeback path for vector partitions), moka_ram_cap for "
            "--scenario moka (load until --dram-gb is full, then stop), "
            "no-op for --scenario no-cache (registers ownership only). "
            "The measure phase uses search_partitions over each actor's "
            "owned slice (per-query partial recall — see README); "
            "none=skip prewarm entirely (cold first query)."
        ),
    )
    p.add_argument(
        "--index-name",
        type=str,
        default="vector_idx",
        help="Index name passed to dataset.prewarm_index when --prewarm forced.",
    )
    p.add_argument(
        "--prewarm-ram-fraction",
        type=float,
        default=1.0,
        help=(
            "Legacy no-op. Previously scaled the per-actor foyer L1 budget "
            "passed to hybrid_tiered deterministic prewarm, when hybrid "
            "prewarm filled L1 first and the rest spilled to L2 via foyer's "
            "WriteOnEviction policy. The current Lance hybrid_tiered policy "
            "places every requested partition into L2 only and never "
            "admits anything to L1 during prewarm, so there is no L1 "
            "budget to scale. Accepted for backward compatibility; values "
            "other than 1.0 are flagged as a no-op."
        ),
    )
    p.add_argument(
        "--pre-measure-residency-probe",
        action="store_true",
        help=(
            "Run the per-partition DRAM residency probe between prewarm "
            "and measure. Off by default: although the probe is no-load "
            "(prewarm_vector_cache with ram_bytes=0 short-circuits before "
            "any storage read), it still goes through the cache access "
            "path once per owned partition, which can shift the "
            "replacement-policy state (recency/frequency/admission) "
            "immediately before the measured workload — the counter "
            "subtraction in the measure phase does not undo that. Enable "
            "only when investigating prewarm placement and you accept "
            "that the measure phase observes a post-residency-scan cache "
            "rather than the raw post-prewarm cache. The post-measure "
            "probe (after queries) is always run for sharded non-no-cache "
            "scenarios since it cannot pollute the measurement. When this "
            "flag is off, the printed residency-shift comparison uses the "
            "known cold-L1 baseline for hybrid_tiered prewarm and is "
            "skipped for moka because there is no pre-measure L1 snapshot."
        ),
    )

    # Workload.
    p.add_argument("--k-list", type=str, default="1000")
    p.add_argument("--nprobes", type=int, default=32)
    p.add_argument("--warmup-queries", type=int, default=256)
    p.add_argument("--measure-queries", type=int, default=1000)

    # Storage + admin.
    p.add_argument("--bucket", type=str, default="lance-bench")
    p.add_argument("--endpoint-url", type=str, default="http://127.0.0.1:9000")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=str, default=str(HERE / "out"))
    p.add_argument(
        "--skip-setup",
        action="store_true",
        help="Reuse the existing dataset + IVF_RQ index at the URI.",
    )
    return p.parse_args()


def parse_k_list(spec: str) -> List[int]:
    return [int(x) for x in spec.split(",") if x.strip()]


def build_per_actor_spec(
    scenario: str,
    actor_l2_dir: str,
    dram_bytes: int,
    l2_bytes: int,
    metadata_bytes: int | None,
    codecless_bytes: int | None = None,
) -> Dict:
    """Reuse `build_scenario_specs` validation + spec-shape, then pick the
    one spec matching the chosen scenario."""
    specs = build_scenario_specs(
        [scenario],
        dram_bytes=dram_bytes,
        l2_bytes=l2_bytes,
        nvme_dir=actor_l2_dir,
        metadata_bytes=metadata_bytes,
        codecless_bytes=codecless_bytes,
    )
    if not specs:
        raise ValueError(f"build_scenario_specs returned nothing for {scenario!r}")
    return specs[0]


def main() -> int:
    args = parse_args()
    k_list = parse_k_list(args.k_list)
    out_dir = Path(args.out_dir)

    # Coord mode is meaningless without a sharded prewarm — the
    # coordinator routes to actors based on partition ownership, and
    # only sharded prewarm sets up the per-actor partition slice. Force
    # it so users don't end up routing to actors that never loaded the
    # partitions they're being asked about.
    if args.mode == "sharded" and args.prewarm != "sharded":
        print(
            f"[driver] --mode=sharded forces --prewarm=sharded "
            f"(was {args.prewarm!r})"
        )
        args.prewarm = "sharded"

    spec = DatasetSpec(
        scale=args.scale,
        dim=args.dim,
        num_partitions=args.num_partitions,
        num_bits=args.num_bits,
        seed=args.seed,
    )

    minio_env = {
        "AWS_ENDPOINT_URL": args.endpoint_url,
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }
    for k, v in minio_env.items():
        os.environ[k] = v

    if args.skip_setup:
        uri = spec.uri(args.bucket)
        print(f"[driver] skipping setup; using {uri}")
    else:
        uri = ensure_dataset(spec, bucket=args.bucket, endpoint_url=args.endpoint_url)

    warmup_qs = make_query_vectors(args.warmup_queries, args.dim, seed=args.seed + 1)
    measure_qs = make_query_vectors(args.measure_queries, args.dim, seed=args.seed + 2)
    print(
        f"[driver] num_actors={args.num_actors} scenario={args.scenario} "
        f"prewarm={args.prewarm}"
    )
    print(
        f"[driver] queries: warmup={len(warmup_qs)} measure={len(measure_qs)} "
        f"k_list={k_list} nprobes={args.nprobes}"
    )
    print(
        f"[driver] per-actor budgets: dram={args.dram_gb} GiB l2={args.l2_gb} GiB "
        f"metadata={args.metadata_mb} MiB"
    )
    if args.actor_resource or args.coordinator_resource:
        print(
            f"[driver] placement resources: actor={args.actor_resource!r} "
            f"coordinator={args.coordinator_resource!r}"
        )

    # Parent for actor L2 subdirs. The `_validate` in scenarios.py only
    # checks the parent of the L2 path, so for hybrid runs we need this dir
    # to exist before constructing the per-actor spec. Moka/no-cache runs do
    # not touch L2 and should not require /mnt/nvme on the driver node.
    if args.scenario == "hybrid":
        Path(args.nvme_dir).mkdir(parents=True, exist_ok=True)

    metadata_bytes = (
        int(args.metadata_mb * MIB) if args.metadata_mb is not None else None
    )
    dram_bytes = int(args.dram_gb * GIB)
    l2_bytes = int(args.l2_gb * GIB)
    codecless_bytes = (
        int(args.codecless_mb * MIB) if args.codecless_mb is not None else None
    )
    if codecless_bytes is not None:
        foyer_mib = (dram_bytes - codecless_bytes) // MIB
        print(
            f"[driver] hybrid advanced split per actor: foyer L1 = {foyer_mib} MiB, "
            f"codec-less Moka = {args.codecless_mb} MiB "
            f"(per-actor DRAM = {args.dram_gb} GiB)"
        )

    # Explicit namespace so check_partition_residency.py — which starts a
    # separate Ray job — can resolve our named actors via ray.get_actor().
    # Without this the driver lands in the job's anonymous namespace and
    # ray.get_actor() from another shell looks in a different one, so the
    # documented "attach from another shell" workflow silently fails.
    ray.init(
        runtime_env={
            "working_dir": str(HERE),
            "env_vars": minio_env,
        },
        namespace=DEFAULT_NAMESPACE,
        ignore_reinit_error=True,
    )
    ray_node = ray._private.worker._global_node
    if ray_node is not None:
        print(f"[driver] Ray temp dir: {ray_node.get_temp_dir_path()}")
        print(f"[driver] Ray session dir: {ray_node.get_session_dir_path()}")

    # Named actor handles let the CoordinatorActor resolve workers via
    # ray.get_actor(...) instead of being passed handles through __init__.
    # Worker name set unconditionally — Ray actor names are unique within
    # the cluster, and the bench tears down on exit, so naming the
    # replicated-mode actors costs nothing and keeps spawn paths uniform.
    worker_names = [f"hybrid-search-actor-{i}" for i in range(args.num_actors)]
    actors = []
    for i in range(args.num_actors):
        actor_l2_dir = os.path.join(args.nvme_dir, f"actor-{i}")
        # Pre-create only when hybrid; for moka/no-cache the L2 path is unused.
        if args.scenario == "hybrid":
            Path(actor_l2_dir).mkdir(parents=True, exist_ok=True)
        per_actor_spec = build_per_actor_spec(
            args.scenario,
            actor_l2_dir=actor_l2_dir,
            dram_bytes=dram_bytes,
            l2_bytes=l2_bytes,
            metadata_bytes=metadata_bytes,
            codecless_bytes=codecless_bytes,
        )
        per_actor_spec["name"] = f"{args.scenario}-actor-{i}"
        actor_options: Dict[str, Any] = {"name": worker_names[i]}
        if args.actor_resource:
            actor_options["resources"] = {args.actor_resource: 1}
        actor = HybridSearchActor.options(**actor_options).remote(
            actor_id=i,
            spec=per_actor_spec,
            uri=uri,
            endpoint_url=args.endpoint_url,
            nprobes=args.nprobes,
        )
        actors.append(actor)

    t_start = time.time()

    # ── Phase 1: prewarm ──
    if args.prewarm == "forced":
        print(
            f"[driver] forced prewarm — {args.num_actors} actors "
            f"call dataset.prewarm_index({args.index_name!r}) in parallel"
        )
        t0 = time.time()
        prewarm_results = ray.get(
            [a.prewarm_index.remote(args.index_name) for a in actors]
        )
        print(f"[driver] forced prewarm done in {time.time() - t0:.1f}s")
        for r in prewarm_results:
            s = r["stats_post_prewarm"]
            print(
                f"  actor={r['actor_id']} prewarm={r['duration_s']:.1f}s "
                f"entries={s.get('num_entries', '?')} "
                f"bytes={s.get('size_bytes', '?'):,}"
            )
    elif args.prewarm == "natural":
        chunks = np.array_split(warmup_qs, args.num_actors)
        print(
            f"[driver] natural prewarm — splitting {len(warmup_qs)} warmup "
            f"queries across {args.num_actors} actors"
        )
        t0 = time.time()
        warmup_results = ray.get(
            [
                actors[i].warmup_natural.remote(chunks[i].tolist())
                for i in range(args.num_actors)
            ]
        )
        print(f"[driver] natural prewarm done in {time.time() - t0:.1f}s")
        for r in warmup_results:
            print(
                f"  actor={r['actor_id']} warmup_n={r['n_queries']} "
                f"duration={r['duration_s']:.1f}s"
            )
    elif args.prewarm == "sharded":
        # Round-robin assignment by partition id. Centroids are uniformly
        # learned over the whole dataset, so partition ids carry no spatial
        # ordering — striding by num_actors balances load without needing
        # a count-based scheme. If a future workload shows skew, swap this
        # for an assignment that weights by per-partition row count.
        partitions_for_actor = [
            list(range(i, args.num_partitions, args.num_actors))
            for i in range(args.num_actors)
        ]
        sizes = [len(p) for p in partitions_for_actor]
        if args.scenario == "no-cache":
            # No cache to fill; loading partitions through a no-op cache
            # only burns MinIO traffic. Still register the slice on each
            # actor so coord-mode routing has a partition map.
            print(
                f"[driver] sharded prewarm (no-cache) — registering "
                f"partition ownership only "
                f"(round-robin mod {args.num_actors}) over "
                f"{args.num_partitions} partitions; per-actor counts: {sizes}"
            )
            prewarm_results = ray.get(
                [
                    actors[i].set_owned_partitions.remote(
                        args.index_name, partitions_for_actor[i]
                    )
                    for i in range(args.num_actors)
                ]
            )
        else:
            policy, ram_bytes_budget = _deterministic_prewarm_params(
                args.scenario,
                dram_bytes,
            )
            if args.scenario == "hybrid" and args.prewarm_ram_fraction != 1.0:
                # Hybrid prewarm no longer fills foyer L1, so the L1
                # fraction knob has no effect — call it out so users do
                # not assume their per-actor cache layout changed.
                print(
                    f"[driver] --prewarm-ram-fraction={args.prewarm_ram_fraction} "
                    "ignored under --scenario hybrid: hybrid_tiered places "
                    "every owned partition into L2 and leaves L1 cold; "
                    "there is no L1 budget to scale."
                )
            if args.scenario == "hybrid":
                print(
                    f"[driver] sharded prewarm (deterministic, policy={policy!r}) "
                    "— placing every owned vector partition into L2; foyer L1 "
                    "remains cold (query traffic will promote partitions "
                    "from L2 into volatile L1). Partition assignment "
                    f"(round-robin mod {args.num_actors}) over "
                    f"{args.num_partitions} partitions; per-actor counts: "
                    f"{sizes}"
                )
            else:
                print(
                    f"[driver] sharded prewarm (deterministic, policy={policy!r}, "
                    f"ram_budget={format_bytes(ram_bytes_budget)} per actor) — "
                    f"partition assignment (round-robin mod {args.num_actors}) "
                    f"over {args.num_partitions} partitions; per-actor counts: "
                    f"{sizes}"
                )
            t0 = time.time()
            prewarm_results = ray.get(
                [
                    actors[i].prewarm_partitions_deterministic.remote(
                        args.index_name,
                        partitions_for_actor[i],
                        policy=policy,
                        ram_bytes=ram_bytes_budget,
                        wait_for_disk=True,
                    )
                    for i in range(args.num_actors)
                ]
            )
            print(f"[driver] sharded prewarm done in {time.time() - t0:.1f}s")
            for r in prewarm_results:
                s = r["stats_post_prewarm"]
                ps = r["prewarm_stats"]
                stopped = ps.get("stopped_before")
                stopped_str = "—" if stopped is None else str(stopped)
                loaded_ram = int(ps.get("loaded_to_ram", 0))
                loaded_disk = int(ps.get("loaded_to_disk", 0))
                skipped = int(ps.get("skipped_existing", 0))
                spills = int(ps.get("disk_bytes_unknown_spills", 0))
                owned_n = int(r["n_partitions"])
                if args.scenario == "hybrid":
                    # Hybrid_tiered is L2-only: every owned partition is
                    # placed in L2 (or already there from a prior run), L1
                    # stays cold, and there is no L1→L2 spill counter to
                    # report. Validate the shape so a stale pylance build
                    # (one that still admits to L1, or that reports
                    # phantom spills) surfaces loudly here rather than
                    # silently shifting the cache footprint.
                    if loaded_ram != 0:
                        raise RuntimeError(
                            f"actor={r['actor_id']}: hybrid_tiered prewarm "
                            f"reported loaded_to_ram={loaded_ram} ≠ 0; "
                            "this pylance build still admits vector "
                            "partitions to L1 during hybrid prewarm — "
                            "expected the no-vector-L1-writeback policy "
                            "where every partition is placed in L2 only"
                        )
                    if spills != 0:
                        raise RuntimeError(
                            f"actor={r['actor_id']}: hybrid_tiered prewarm "
                            f"reported disk_bytes_unknown_spills={spills} ≠ 0; "
                            "expected zero under the no-writeback policy "
                            "(no L1 admissions means nothing can spill)"
                        )
                    if loaded_disk + skipped != owned_n:
                        raise RuntimeError(
                            f"actor={r['actor_id']}: hybrid_tiered prewarm "
                            f"placed {loaded_disk} + skipped {skipped} = "
                            f"{loaded_disk + skipped} partitions but owns "
                            f"{owned_n}; some owned partitions are missing "
                            "from L2 after wait_for_disk=True"
                        )
                    print(
                        f"  actor={r['actor_id']} prewarm={r['duration_s']:.1f}s "
                        f"owned={owned_n} "
                        f"ram={loaded_ram} "
                        f"disk={loaded_disk} "
                        f"skipped_l2={skipped} "
                        f"ram_deep={format_bytes(int(ps.get('ram_bytes_deep_size', 0)))} "
                        f"disk_serialized={format_bytes(int(ps.get('disk_bytes_serialized', 0)))} "
                        f"cache_entries={s.get('num_entries', '?')} "
                        f"cache_bytes={s.get('size_bytes', '?'):,}"
                    )
                else:
                    print(
                        f"  actor={r['actor_id']} prewarm={r['duration_s']:.1f}s "
                        f"owned={owned_n} "
                        f"ram={loaded_ram} "
                        f"disk={loaded_disk} "
                        f"skipped={skipped} "
                        f"stopped_before={stopped_str} "
                        f"ram_deep={format_bytes(int(ps.get('ram_bytes_deep_size', 0)))} "
                        f"disk_serialized={format_bytes(int(ps.get('disk_bytes_serialized', 0)))} "
                        f"cache_entries={s.get('num_entries', '?')} "
                        f"cache_bytes={s.get('size_bytes', '?'):,}"
                    )
    # Capture a post-prewarm L2 snapshot for hybrid runs. Diffed against
    # a second snapshot taken after measurement to surface query-driven
    # L2 writes — under the no-vector-L1-writeback policy, vector
    # partition entries evicted from L1 are dropped (not spilled back to
    # L2), so the L2 file/byte totals should stay stable across the
    # measure phase. This is a coarse fallback: foyer can recycle blocks
    # on overwrite, so stable byte totals only rule out *visible*
    # growth and file-count churn, not arbitrary overwrite churn. The
    # actor owns the snapshot so it works in real-cluster mode where the
    # L2 dir lives on the actor node and is unreachable from the driver.
    post_prewarm_l2_snaps: List[Dict[str, Any]] = []
    actor_l2_dirs: List[str] = []
    if args.prewarm == "sharded" and args.scenario == "hybrid":
        actor_l2_dirs = [
            os.path.join(args.nvme_dir, f"actor-{i}")
            for i in range(args.num_actors)
        ]
        post_prewarm_l2_snaps = ray.get(
            [
                actors[i].snapshot_l2_dir.remote(actor_l2_dirs[i])
                for i in range(args.num_actors)
            ]
        )
        print("[driver] L2 snapshot (post-prewarm):")
        for snap in post_prewarm_l2_snaps:
            if not snap.get("exists"):
                print(
                    f"  actor={snap['actor_id']} L2 dir missing "
                    f"({snap.get('path')})"
                )
                continue
            print(
                f"  actor={snap['actor_id']} L2 dir={snap['path']} "
                f"files={snap['file_count']} "
                f"apparent={format_bytes(int(snap['apparent_bytes']))} "
                f"disk={format_bytes(int(snap['disk_bytes']))}"
            )
    if args.prewarm not in ("sharded", "forced", "natural"):
        print("[driver] prewarm=none — actors run measure cold")

    # ── Phase 1.5: post-prewarm residency check ──
    # Only meaningful when prewarm actually placed partitions and the
    # per-actor ownership is known. Sharded prewarm sets ownership and
    # leaves the cache populated according to scenario+policy; the other
    # prewarm modes either populate every actor with everything (forced /
    # natural) or skip prewarm (none). Probing 3000 partitions on every
    # actor for `forced`/`natural` adds wall-time for limited extra
    # signal, so we restrict the probe to the sharded path. The no-cache
    # scenario is also skipped because the probe calls prewarm_vector_cache
    # (an API the no-cache path does not otherwise need) on every owned
    # partition, which would add cache work the benchmark deliberately
    # avoids and could perturb the measured behavior.
    #
    # The post-measure probe in Phase 2.5 cannot perturb the measurement
    # (it runs after it), so it fires whenever sharded prewarm placed
    # owned partitions. The *pre-measure* probe is gated additionally on
    # --pre-measure-residency-probe: although the probe is no-load
    # (pass 2 short-circuits on ram_bytes=0 without touching storage),
    # it still walks the cache once per partition immediately before the
    # measured workload, which can shift recency/frequency/admission
    # state. The hit/miss counter subtraction below cancels the count
    # bump but not the replacement-policy bump, so by default we leave
    # the cache untouched between prewarm and measure.
    eligible_for_residency_probe = (
        args.prewarm == "sharded" and args.scenario != "no-cache"
    )
    do_pre_residency_probe = (
        eligible_for_residency_probe and args.pre_measure_residency_probe
    )
    do_post_residency_probe = eligible_for_residency_probe
    residency_jsonl_path = (
        Path(args.out_dir) / "partition_residency.jsonl"
        if args.out_dir and do_post_residency_probe
        else None
    )
    # Truncate any leftover file from a previous run so the two new
    # labels are the only ones written this session.
    if residency_jsonl_path is not None:
        residency_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        residency_jsonl_path.write_text("")
    residency_nvme_dir = (
        args.nvme_dir if args.scenario == "hybrid" else None
    )
    pre_measure_residency: List[Dict[str, Any]] = []
    if do_pre_residency_probe:
        pre_measure_residency = run_residency_check(
            actors=actors,
            partitions_for_actor=partitions_for_actor,
            index_name=args.index_name,
            nvme_dir=residency_nvme_dir,
            label="post-prewarm",
        )
        if residency_jsonl_path is not None:
            write_residency_jsonl(
                pre_measure_residency,
                str(residency_jsonl_path),
                label="post-prewarm",
            )
    elif eligible_for_residency_probe:
        print(
            "[driver] pre-measure residency probe skipped — pass "
            "--pre-measure-residency-probe to opt in. The probe walks "
            "the cache once per owned partition and can shift "
            "replacement-policy state before the measured workload, "
            "so it is off by default. The post-measure probe still runs; "
            "hybrid_tiered shift reporting will use its validated cold-L1 "
            "post-prewarm baseline."
        )
    elif args.prewarm == "sharded" and args.scenario == "no-cache":
        print(
            "[driver] residency check skipped — --scenario=no-cache "
            "populates no cache; probing would invoke prewarm_vector_cache "
            "the no-cache path does not otherwise exercise and could "
            "perturb the measurement"
        )
    else:
        print(
            f"[driver] residency check skipped — prewarm={args.prewarm!r} "
            "does not set per-actor partition ownership "
            "(check requires --prewarm sharded)"
        )

    # ── Phase 2: measure ──
    coord_result: Dict[str, Any] | None = None
    coord = None
    if args.mode == "sharded":
        # Coord mode routes every query through one CoordinatorActor; the
        # workers no longer time anything end-to-end and instead serve
        # search_partitions on demand. Per-query latency is owned by the
        # coord (centroid + scatter + per-worker partial probe + merge);
        # per-actor cache stats are still polled separately for the
        # per-actor table.
        print(
            f"[driver] coord-driven sharded measure — "
            f"{len(measure_qs)} queries × {len(k_list)} k-values, "
            f"fan-out across {args.num_actors} actors"
        )
        coord_options: Dict[str, Any] = {"name": "coordinator"}
        if args.coordinator_resource:
            coord_options["resources"] = {args.coordinator_resource: 1}
        coord = CoordinatorActor.options(**coord_options).remote(
            dataset_uri=uri,
            endpoint_url=args.endpoint_url,
            index_name=args.index_name,
            nprobes=args.nprobes,
            num_actors=args.num_actors,
            worker_names=worker_names,
            metadata_bytes=metadata_bytes,
        )
        # Block on coord __init__ (opens the dataset over MinIO, resolves
        # worker handles) so its setup time is not charged to measure_wall_s.
        ray.get(coord.ready.remote())
        # Force-open the coordinator's top-level vector index outside the
        # measure timer. __init__ opens the dataset but the vector index
        # itself is loaded lazily on the first compute_partition_ids
        # call; without this, the first measured query pays the
        # index-open + IVF-centroid deserialisation cost. Deterministic
        # prewarm above covers per-worker partition entries and per-worker
        # top-level index objects, but not the coordinator's copy.
        # Use a dedicated throwaway query so this path works even when
        # --measure-queries=0 or --warmup-queries=0 (both arrays may be
        # empty).
        warmup_routing_q = make_query_vectors(1, args.dim, seed=args.seed + 3)[0]
        t_warm = time.time()
        ray.get(coord.warmup_routing.remote(warmup_routing_q.tolist()))
        print(
            f"[driver] coord routing warmup done in "
            f"{time.time() - t_warm:.1f}s"
        )
        # No random-query warmup pass: deterministic sharded prewarm
        # already populated the codec-bearing IVF partition entries (the
        # dominant cache footprint) and the codec-less top-level vector
        # index objects on each worker. If --warmup-queries was set, log
        # that we are ignoring it so users notice the policy change
        # instead of silently losing the query budget.
        if len(warmup_qs) > 0:
            print(
                f"[driver] coord-driven random warmup skipped: "
                f"deterministic sharded prewarm already populated "
                f"partition + index caches (was {len(warmup_qs)} warmup "
                f"queries)"
            )
        # Snapshot per-actor cache counters before measure so the
        # post-measure hit_ratio reflects measure traffic only, not the
        # cumulative counters that include warmup MinIO loads.
        pre_measure_stats = ray.get([a.cache_stats.remote() for a in actors])
        t_measure_start = time.time()
        coord_result = ray.get(
            coord.search_batch.remote(measure_qs.tolist(), k_list)
        )
        measure_wall_s = time.time() - t_measure_start
        # Pull per-actor cache stats post-measure — coord doesn't have
        # them since it never touched partition data.
        per_actor_results = ray.get([a.cache_stats.remote() for a in actors])
        # Subtract pre-measure baseline so the reported hits/misses
        # describe the measure phase in isolation.
        pre_by_id = {r["actor_id"]: r["stats_post"] for r in pre_measure_stats}
        for r in per_actor_results:
            base = pre_by_id.get(r["actor_id"], {})
            post = r["stats_post"]
            post["hits"] = int(post.get("hits", 0)) - int(base.get("hits", 0))
            post["misses"] = int(post.get("misses", 0)) - int(base.get("misses", 0))
    else:
        chunks = np.array_split(measure_qs, args.num_actors)
        n_per_actor = [len(c) for c in chunks]
        measure_method = "measure_sharded" if args.prewarm == "sharded" else "measure"
        print(
            f"[driver] measure ({measure_method}) — query slice sizes per actor: "
            f"{n_per_actor} (× {len(k_list)} k-values each)"
        )
        # When the pre-measure residency probe ran, it called
        # prewarm_vector_cache once per owned partition; even in no-load
        # mode that still bumps the session's cache counters. Snapshot
        # per-actor stats here so the post-measure totals can be reduced
        # to measure-phase deltas, the same way coord mode handles it
        # above. The counter delta cancels the *count* bump from the
        # probe but not its effect on replacement-policy state — that
        # is why the probe is opt-in (see --pre-measure-residency-probe).
        pre_measure_stats: List[Dict[str, Any]] = []
        if do_pre_residency_probe:
            pre_measure_stats = ray.get([a.cache_stats.remote() for a in actors])
        t_measure_start = time.time()
        if args.prewarm == "sharded":
            # measure_sharded uses each actor's owned partition slice (set by
            # prewarm_partitions); per-query result is partial top-K within
            # that slice, so the aggregate latency table reflects per-actor
            # work, not full-recall query cost. See README §sharded.
            futures = [
                actors[i].measure_sharded.remote(chunks[i].tolist(), k_list)
                for i in range(args.num_actors)
            ]
        else:
            futures = [
                actors[i].measure.remote(chunks[i].tolist(), k_list)
                for i in range(args.num_actors)
            ]
        per_actor_results = ray.get(futures)
        measure_wall_s = time.time() - t_measure_start
        if pre_measure_stats:
            pre_by_id = {r["actor_id"]: r["stats_post"] for r in pre_measure_stats}
            for r in per_actor_results:
                base = pre_by_id.get(r["actor_id"], {})
                post = r["stats_post"]
                post["hits"] = int(post.get("hits", 0)) - int(base.get("hits", 0))
                post["misses"] = int(post.get("misses", 0)) - int(base.get("misses", 0))

    # ── Phase 2.5: post-measure residency check ──
    # Same probe shape as the (optional) post-prewarm one, so when both
    # snapshots exist the comparison below shows query-driven L1 churn:
    # any partition that moved out of L1 during measure appears in this
    # snapshot's ``not_in_l1`` but the previous one's ``in_l1``. Under
    # the no-vector-L1-writeback policy the displaced entry is dropped
    # from RAM only — vector partition L2 entries already exist from
    # deterministic prewarm, so an eviction does not change L2 state.
    # Per-partition L2 residency is not yet exposed through pylance, so
    # ``still_in_l2`` and ``missing_from_l2`` carry the actor's
    # ``l2_residency_source``: hybrid actors use the wait_for_disk
    # prewarm-validated owned set, cross-checked against the
    # post-measure L2 directory snapshot below. Runs
    # unconditionally for sharded non-no-cache scenarios because it runs
    # *after* measure and so cannot pollute the measured workload —
    # unlike the pre-measure probe which is opt-in. Done before actor
    # close so the cache is still in its post-measure state.
    post_measure_residency: List[Dict[str, Any]] = []
    if do_post_residency_probe:
        post_measure_residency = run_residency_check(
            actors=actors,
            partitions_for_actor=partitions_for_actor,
            index_name=args.index_name,
            nvme_dir=residency_nvme_dir,
            label="post-measure",
        )
        if residency_jsonl_path is not None:
            write_residency_jsonl(
                post_measure_residency,
                str(residency_jsonl_path),
                label="post-measure",
            )
        # Cross-check: how did residency shift across measure? Partitions
        # that were L1-resident post-prewarm but not post-measure were
        # evicted by query traffic — and under the no-vector-L1-writeback
        # policy that eviction drops the entry from RAM without spilling
        # back to L2 (the L2 entry already exists from prewarm).
        # Promotions are the reverse: a partition that was not L1-resident
        # at the post-prewarm probe but is now, which implies an L2 → L1
        # promotion driven by the query path decoding an L2 entry.
        pre_by_id, baseline_source = _post_prewarm_l1_baseline(
            scenario=args.scenario,
            prewarm=args.prewarm,
            num_actors=args.num_actors,
            pre_measure_residency=pre_measure_residency,
        )
        if baseline_source is not None:
            print("\n=== Residency shift (post-prewarm → post-measure) ===")
            print(f"  baseline: post-prewarm L1 {baseline_source}")
            for r in post_measure_residency:
                aid = r["actor_id"]
                pre_set = pre_by_id.get(aid, set())
                post_set = set(r["in_l1"])
                stayed = len(pre_set & post_set)
                evicted = len(pre_set - post_set)
                promoted = len(post_set - pre_set)
                still_in_l2 = len(r.get("in_l2", []))
                missing_from_l2 = len(r.get("missing", []))
                l2_source = r.get("l2_residency_source", "unknown")
                print(
                    f"  actor-{aid:<3} stayed_in_l1={stayed:<5} "
                    f"evicted_from_l1={evicted:<5} "
                    f"promoted_into_l1={promoted:<5} "
                    f"still_in_l2={still_in_l2:<5} "
                    f"missing_from_l2={missing_from_l2:<5} "
                    f"l2_source={l2_source}"
                )

    # Post-measure L2 directory snapshot — diff against the post-prewarm
    # snapshot above. Stable file/byte totals are a coarse signal that
    # query L1 churn did not cause extra L2 writes (no vector-partition
    # eviction writeback). Foyer can still recycle blocks on overwrite,
    # so this only rules out *visible* growth and file-count churn —
    # not silent in-place overwrite — see l2_inspect.py.
    if (
        args.prewarm == "sharded"
        and args.scenario == "hybrid"
        and post_prewarm_l2_snaps
    ):
        from l2_inspect import diff_snapshots  # local import to keep module deps lean

        post_measure_l2_snaps = ray.get(
            [
                actors[i].snapshot_l2_dir.remote(actor_l2_dirs[i])
                for i in range(args.num_actors)
            ]
        )
        pre_by_id = {s["actor_id"]: s for s in post_prewarm_l2_snaps}
        print("\n=== L2 directory snapshot (post-measure) ===")
        for snap in post_measure_l2_snaps:
            aid = snap["actor_id"]
            if not snap.get("exists"):
                print(
                    f"  actor={aid} L2 dir missing ({snap.get('path')})"
                )
                continue
            pre = pre_by_id.get(aid)
            delta = diff_snapshots(pre, snap) if pre else {}
            delta_str = ""
            if delta:
                delta_str = (
                    f"  Δ(apparent={format_bytes(delta['apparent_bytes_delta'])}, "
                    f"disk={format_bytes(delta['disk_bytes_delta'])}, "
                    f"files={delta['file_count_delta']:+d})"
                )
            print(
                f"  actor={aid} L2 dir={snap['path']} "
                f"files={snap['file_count']} "
                f"apparent={format_bytes(int(snap['apparent_bytes']))} "
                f"disk={format_bytes(int(snap['disk_bytes']))}"
                + delta_str
            )

    # ── Phase 3: close + cleanup ──
    ray.get([a.close.remote() for a in actors])
    for a in actors:
        ray.kill(a)
    if coord is not None:
        ray.kill(coord)
    ray.shutdown()

    # ── Aggregate ──
    aggregated: Dict[int, List[float]] = {k: [] for k in k_list}
    total_hits = 0
    total_misses = 0

    if coord_result is not None:
        # Coord owns the per-query timing; per-actor results carry only
        # cache stats (no latencies_by_k). Aggregate latencies come from
        # the single coord return value.
        for k, lats in coord_result["latencies_by_k"].items():
            aggregated[int(k)].extend(lats)
        for r in per_actor_results:
            total_hits += r["stats_post"]["hits"]
            total_misses += r["stats_post"]["misses"]
    else:
        for r in per_actor_results:
            for k, lats in r["latencies_by_k"].items():
                aggregated[int(k)].extend(lats)
            total_hits += r["stats_post"]["hits"]
            total_misses += r["stats_post"]["misses"]

    total_queries = len(measure_qs) * len(k_list)
    print(
        f"\n=== Distributed summary ({args.num_actors} actors, "
        f"mode={args.mode}, scenario={args.scenario}, prewarm={args.prewarm}) ==="
    )
    print(
        f"measure wall-time: {measure_wall_s:.1f}s   "
        f"aggregate throughput: {total_queries / measure_wall_s:.1f} q/s"
    )
    total = total_hits + total_misses
    agg_hr = (total_hits / total) if total else 0.0
    print(
        f"aggregate cache stats: hit_ratio={agg_hr:.2%} "
        f"({total_hits} hits / {total} accesses)"
    )
    if coord_result is not None:
        # Coord-side breakdown: where in the per-query path the time goes.
        # centroid is the IVF routing matvec, scatter is the parallel
        # worker fan-out, merge is the top-K reduction across partials.
        print(
            f"coord per-query mean: "
            f"centroid={coord_result['centroid_s_mean'] * 1000:.2f} ms  "
            f"scatter={coord_result['scatter_s_mean'] * 1000:.2f} ms  "
            f"merge={coord_result['merge_s_mean'] * 1000:.2f} ms  "
            f"workers_invoked≈{coord_result['mean_workers_invoked_per_query']:.2f}"
            f"/{args.num_actors}  "
            f"routed_partitions≈{coord_result['mean_routed_partitions_per_query']:.1f}"
            f"/{args.nprobes}"
        )
    for k in k_list:
        pct = percentiles(aggregated[k])
        print(format_latency_row("aggregate", k, pct))

    print("\nPer-actor:")
    for r in per_actor_results:
        s = r["stats_post"]
        t = s["hits"] + s["misses"]
        hr = (s["hits"] / t) if t else 0.0
        if coord_result is not None:
            # In coord mode workers don't time per-query; report cache
            # health and how many search_partitions calls each handled
            # so fan-out balance is visible.
            print(
                f"  actor-{r['actor_id']:<5} "
                f"hit={hr:.1%}  "
                f"entries={s.get('num_entries', '?')}  "
                f"bytes={s.get('size_bytes', '?'):,}  "
                f"owned={r['owned_partitions']}  "
                f"calls_handled={r['n_searches_handled']}"
            )
            continue
        # Sharded actors report their owned-partition slice and the mean
        # number of routed-and-owned partitions per query — i.e. how much
        # of the 32-nprobes routing budget actually landed in this actor.
        # Tag the row so a small mean here explains a low aggregate q/s
        # without scrolling back to the prewarm log.
        sharded_tag = ""
        if "owned_partitions" in r:
            sharded_tag = (
                f"  owned={r['owned_partitions']}"
                f"  routed_owned≈{r['mean_owned_routed_per_query']:.1f}"
            )
        for k_str, lats in r["latencies_by_k"].items():
            pct = percentiles(lats)
            print(
                format_latency_row(f"actor-{r['actor_id']}", int(k_str), pct)
                + f"  hit={hr:.1%}"
                + f"  dur={r['duration_s']:.1f}s"
                + sharded_tag
            )

    # ── Persist ──
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "distributed_results.jsonl").open("w") as f:
        # In coord mode write the coord's aggregate row first so reader
        # can find latencies regardless of mode. Per-actor rows follow
        # with cache_stats only.
        if coord_result is not None:
            cr = dict(coord_result)
            cr["actor_id"] = "coordinator"
            cr["latencies_by_k"] = {
                str(k): v for k, v in coord_result["latencies_by_k"].items()
            }
            f.write(json.dumps(cr) + "\n")
        for r in per_actor_results:
            rr = dict(r)
            if "latencies_by_k" in rr:
                rr["latencies_by_k"] = {
                    str(k): v for k, v in rr["latencies_by_k"].items()
                }
            f.write(json.dumps(rr) + "\n")

    print(
        f"\n[driver] wrote {out_dir}/distributed_results.jsonl "
        f"(total wall-time {(time.time() - t_start) / 60:.1f} min)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
