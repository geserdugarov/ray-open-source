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
  through the v6 strict `dataset.prewarm_index(name, partition_ids=...)`
  path. For `--scenario distributed` that writes one
  `part-ivf-<id>.bin` per partition under `{l2_dir}/v1/{prefix}/`
  atomically; the actor walks the L2 dir post-prewarm and the driver
  hard-fails on any missing / extra partitions vs. the expected slice.
  For `--scenario no-cache` the call is a no-op that only registers
  ownership. The v4 `policy` / `ram_bytes` knobs (`hybrid_tiered`,
  `moka_ram_cap`) are gone — the v6 distributed cache controls
  placement itself. Per-actor prewarm cost stays flat as `--num-actors`
  grows; per-query recall is partial under `--mode replicated` since
  results are not merged across actors. Use `--mode sharded` for full
  recall via the coordinator topology.
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

from scenarios import (  # noqa: F401
    GIB,
    MIB,
    build_scenario_spec,
    build_scenario_specs,
    is_eligible_for_residency_probe,
)

HERE = Path(__file__).resolve().parent

from _hybrid_cache_helpers import (  # noqa: E402
    DatasetSpec,
    ensure_dataset,
    format_latency_row,
    load_index_partition_sizes,
    make_query_vectors,
    percentiles,
)

from check_l2_residency import (  # noqa: E402
    DEFAULT_NAMESPACE,
    L1_SIZE_UNKNOWN,
    run_l2_residency_check,
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
    if scenario == "distributed":
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
) -> tuple[Dict[int, int], str | None]:
    """Return the per-actor L1 ``size_bytes`` baseline for the shift report.

    Aggregate-only under v6: the v4 per-partition L1 sets are gone (the
    no-load probe API ``prewarm_vector_cache(..., ram_bytes=0)`` has no
    Lance 7.0 equivalent). The preferred baseline is the optional
    post-prewarm residency probe's ``l1_size_bytes_at_probe``. For
    hybrid ``sharded`` prewarm we can still report movement without
    running that probe: ``hybrid_tiered`` admits zero vector partitions
    into L1, so the L1 byte baseline is 0 by construction.
    """
    if pre_measure_residency:
        observed = {
            int(r["actor_id"]): int(r.get("l1_size_bytes_at_probe", L1_SIZE_UNKNOWN))
            for r in pre_measure_residency
        }
        return (observed, "observed by post-prewarm probe")
    if prewarm == "sharded" and scenario == "distributed":
        return (
            dict.fromkeys(range(num_actors), 0),
            "inferred zero from hybrid_tiered prewarm",
        )
    return {}, None


def _assert_l2_validation_clean(prewarm_results: List[Dict[str, Any]]) -> None:
    """Hard-fail the run if any actor's sharded prewarm dropped L2 files.

    ``HybridSearchActor.prewarm_partitions_deterministic`` returns an
    ``l2_validation`` block per actor (empty for non-distributed
    sessions, which carry no L2 tier). The driver passes only non-empty
    IVF partitions into sharded prewarm, based on Lance index statistics.
    For distributed sessions the v6 strict
    ``dataset.prewarm_index(name, partition_ids=...)`` path should
    persist every requested non-empty partition or raise ``LanceError``.
    Any non-zero ``missing_count`` here is therefore a backend regression
    and must abort the run rather than continue into a measure phase that
    would silently scan partitions still served from MinIO.
    ``extra_count != 0`` flags a stale-prefix collision (two live
    ``v1/{prefix}/`` subdirs at the same ``l2_dir``); equally fatal
    because the residency probe later refuses to claim a residency under
    that ambiguity.
    """
    failures: List[str] = []
    for r in prewarm_results:
        v = r.get("l2_validation") or {}
        if not v:
            continue
        actor_id = r.get("actor_id", "?")
        missing = int(v.get("missing_count", 0))
        extra = int(v.get("extra_count", 0))
        expected = int(v.get("expected_count", 0))
        file_count = int(v.get("l2_file_count", 0))
        if missing or extra or file_count < expected:
            failures.append(
                f"actor={actor_id} expected={expected} "
                f"l2_files={file_count} missing={missing} extra={extra} "
                f"missing_sample={v.get('missing', [])[:8]} "
                f"extra_sample={v.get('extra', [])[:8]} "
                f"prefix_dirs={v.get('l2_prefix_dirs', [])}"
            )
    if failures:
        raise RuntimeError(
            "L2 placement check failed after sharded prewarm — the v6 "
            "strict prewarm_index path should persist every requested "
            "partition or raise LanceError; "
            f"{len(failures)} actor(s) reported drift:\n  " + "\n  ".join(failures)
        )


def _partition_ids_by_actor(
    partition_ids: List[int],
    num_actors: int,
) -> List[List[int]]:
    """Assign valid IVF partition ids to actors by ``partition_id % N``."""
    if num_actors <= 0:
        raise ValueError(f"num_actors must be > 0; got {num_actors}")
    owned: List[List[int]] = [[] for _ in range(num_actors)]
    for pid in sorted({int(p) for p in partition_ids}):
        owned[pid % num_actors].append(pid)
    return owned


def _nonneg_int(value: str) -> int:
    """argparse type: reject negative values for v6 L1 sizing flags.

    `--partition-l1-mb 0` disables the partition L1 tier; negative
    values are not a valid disable spelling and would have been
    silently mapped to None under the old `> 0` guard, hiding a typo.
    """
    iv = int(value)
    if iv < 0:
        raise argparse.ArgumentTypeError(
            f"value must be >= 0; got {iv} (pass 0 to disable the partition L1 tier)"
        )
    return iv


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
        default="distributed",
        choices=["no-cache", "moka", "distributed", "hybrid"],
        help="All actors share the same scenario. 'hybrid' is a deprecated "
        "alias for 'distributed' (v6 rename).",
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
        "--metadata-l1-mb",
        type=_nonneg_int,
        default=64,
        help="v6 metadata-L1 budget PER ACTOR (MiB) for the distributed "
        "scenario. Holds IvfIndexState, IndexMetadata, FragReuseIndex, "
        "ScalarIndexDetails, etc.; sizing too small defeats the per-query "
        "routing path. Default 64.",
    )
    p.add_argument(
        "--partition-l1-mb",
        type=_nonneg_int,
        default=1024,
        help="v6 decoded-partition L1 budget PER ACTOR (MiB) for the "
        "distributed scenario. Pass 0 to disable the partition-L1 tier "
        "(every decode hits L2); negative values are rejected. Default 1024.",
    )
    p.add_argument(
        "--codecless-mb",
        type=int,
        default=None,
        help="Deprecated v4 hybrid knob. The v6 distributed cache has no "
        "codec-less Moka tier; passing this flag prints a warning and is "
        "otherwise ignored.",
    )
    p.add_argument(
        "--l2-gb",
        type=float,
        default=8.0,
        help="Deprecated v4 hybrid L2-capacity knob. v6 has no L2 capacity "
        "bookkeeping; size the actor's NVMe filesystem yourself. Ignored "
        "for --scenario distributed; still affects display defaults.",
    )
    p.add_argument(
        "--metadata-mb",
        type=float,
        default=None,
        help="Session-wide `metadata_cache_size_bytes` for the no-cache / "
        "moka scenarios (MiB). Ignored for --scenario distributed (use "
        "--metadata-l1-mb instead). Default uses Lance's default.",
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
            "{i, i+N, i+2N, ...} via the v6 strict "
            "dataset.prewarm_index(name, partition_ids=...) path. For "
            "--scenario distributed the strict path writes one "
            "part-ivf-<id>.bin per partition under {l2_dir}/v1/{prefix}/ "
            "atomically (LanceError on any L2 write failure / mid-prewarm "
            "generation change); the actor walks the L2 dir post-prewarm "
            "and the driver hard-fails on any missing/extra partitions "
            "vs the expected slice. For --scenario no-cache it is a "
            "no-op that only registers ownership. The v4 placement "
            "policy knobs (hybrid_tiered, moka_ram_cap) are gone in v6 "
            "— the distributed cache controls placement itself. The "
            "measure phase uses search_partitions over each actor's "
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
        "--simulate-invalidation",
        action="store_true",
        help=(
            "After Phase 2 measure, exercise the Lance v6 freshness path: "
            "call Session.invalidate_index_cache(uri, index_addr) on every "
            "actor (and the coordinator under --mode sharded) with one "
            "retry on IOError; verify the per-prefix L2 subdir is gone or "
            "renamed to .{sanitize(prefix)}.deleting-{nonce}/ via the "
            "L2 snapshot helper; re-run sharded prewarm (the 'cold L2 "
            "rehydration cost'); re-run measure; write out/invalidation.json "
            "with first/second per-k latency summaries, per-actor "
            "invalidate times, rehydrate-prewarm time, and percentage "
            "deltas. Requires --scenario distributed and --prewarm sharded; "
            "moka / no-cache sessions have no v6 distributed cache to "
            "invalidate, and only the strict sharded prewarm rehydrates "
            "the L2 prefix deterministically."
        ),
    )
    p.add_argument(
        "--pre-measure-residency-probe",
        action="store_true",
        help=(
            "Also run the v6 aggregate-only residency probe between "
            "prewarm and measure. Off by default for symmetry with the "
            "v4 narrative: the probe itself is side-effect-free under "
            "v6 (one filesystem walk under {l2_dir}/v1/{prefix}/ plus "
            "Session.size_bytes() per actor, returned in a single RPC), "
            "but a single flag controls both the pre-measure and the "
            "post-measure probes so they are written symmetrically to "
            "partition_residency.jsonl. The v4 no-load per-partition L1 "
            "probe has no v6 equivalent — the L2 directory walk plus "
            "aggregate Session.size_bytes() is the replacement. The "
            "post-measure probe always runs for forced/sharded prewarm "
            "with --scenario other than no-cache because it cannot "
            "pollute the measurement."
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
    actor_id: int,
    nvme_dir: str,
    dram_bytes: int,
    metadata_l1_bytes: int,
    partition_l1_bytes: int | None,
    metadata_bytes: int | None = None,
) -> Dict:
    """Build one v6 spec for a single actor.

    Thin wrapper that resolves the per-actor L2 subdirectory and routes
    through `build_scenario_spec`. `actor_id` is what differentiates
    actors in the distributed scenario; moka / no-cache ignore it.
    Callers must pass `metadata_l1_bytes` and `partition_l1_bytes`
    explicitly so v6 cache sizing is operator-controlled rather than
    hidden behind a default that does not match the CLI.
    """
    if scenario == "hybrid":
        scenario = "distributed"
    return build_scenario_spec(
        scenario,
        actor_id=actor_id,
        dram_bytes=dram_bytes,
        nvme_dir=nvme_dir,
        metadata_l1_bytes=metadata_l1_bytes,
        partition_l1_bytes=partition_l1_bytes,
        metadata_cache_size_bytes=metadata_bytes,
    )


def _normalize_scenario_alias(scenario: str) -> str:
    """Map deprecated v4 scenario name to its v6 equivalent.

    Pure helper so the alias rule is unit-testable; main() runs this
    immediately after `parse_args` so everything downstream sees the
    normalized v6 name.
    """
    if scenario == "hybrid":
        print(
            "[driver] --scenario=hybrid is deprecated; aliasing to "
            "'distributed' (v6 rename).",
            file=sys.stderr,
        )
        return "distributed"
    return scenario


def _format_per_actor_summary_lines(
    per_actor_results: List[Dict[str, Any]],
    coord_result: Dict[str, Any] | None,
) -> List[str]:
    """Format the `Per-actor:` block printed at the end of main().

    Extracted from main() so the format string can be unit-tested without
    spinning up Ray. Returns the lines as a list (including the header)
    rather than printing them so tests can inspect the output. Lance 7.0
    exposes only `Session.size_bytes()`; the v4 hit/miss counters are
    gone, so per-actor rows report `bytes=` (the cumulative session
    footprint), not `hit=`.
    """
    lines: List[str] = ["\nPer-actor:"]
    for r in per_actor_results:
        s = r["stats_post"]
        size_bytes_val = int(s.get("size_bytes", 0))
        if coord_result is not None:
            # In coord mode workers don't time per-query; report cache
            # footprint and how many search_partitions calls each
            # handled so fan-out balance is visible.
            lines.append(
                f"  actor-{r['actor_id']:<5} "
                f"bytes={size_bytes_val:,}  "
                f"owned={r['owned_partitions']}  "
                f"calls_handled={r['n_searches_handled']}"
            )
            continue
        # Sharded actors report their owned-partition slice and the mean
        # number of routed-and-owned partitions per query — i.e. how
        # much of the routing budget actually landed in this actor.
        sharded_tag = ""
        if "owned_partitions" in r:
            sharded_tag = (
                f"  owned={r['owned_partitions']}"
                f"  routed_owned≈{r['mean_owned_routed_per_query']:.1f}"
            )
        for k_str, lats in r["latencies_by_k"].items():
            pct = percentiles(lats)
            lines.append(
                format_latency_row(f"actor-{r['actor_id']}", int(k_str), pct)
                + f"  bytes={size_bytes_val:,}"
                + f"  dur={r['duration_s']:.1f}s"
                + sharded_tag
            )
    return lines


def _run_measure_pass(
    *,
    mode: str,
    actors: List[Any],
    coord: Any | None,
    measure_qs: np.ndarray,
    k_list: List[int],
    num_actors: int,
    prewarm: str,
) -> tuple[List[Dict[str, Any]], Dict[str, Any] | None, float]:
    """Run one measure pass and return (per_actor_results, coord_result, wall_s).

    Extracted so the optional ``--simulate-invalidation`` drill can rerun
    measure after the post-invalidation rehydrate without duplicating
    the per-mode branching. In ``--mode sharded`` per-query latency is
    owned by the coord's ``search_batch``; per-actor results are pulled
    via ``cache_stats`` so the per-actor footprint table still
    populates. In ``--mode replicated`` the driver splits the query
    list round-robin and each actor times its own slice via
    ``measure`` / ``measure_sharded``.
    """
    if mode == "sharded":
        if coord is None:
            raise ValueError(
                "_run_measure_pass: coord must be set under --mode sharded"
            )
        t_measure_start = time.time()
        coord_result = ray.get(coord.search_batch.remote(measure_qs.tolist(), k_list))
        measure_wall_s = time.time() - t_measure_start
        per_actor_results = ray.get([a.cache_stats.remote() for a in actors])
        return per_actor_results, coord_result, measure_wall_s

    chunks = np.array_split(measure_qs, num_actors)
    t_measure_start = time.time()
    if prewarm == "sharded":
        futures = [
            actors[i].measure_sharded.remote(chunks[i].tolist(), k_list)
            for i in range(num_actors)
        ]
    else:
        futures = [
            actors[i].measure.remote(chunks[i].tolist(), k_list)
            for i in range(num_actors)
        ]
    per_actor_results = ray.get(futures)
    measure_wall_s = time.time() - t_measure_start
    return per_actor_results, None, measure_wall_s


def _aggregate_latencies_by_k(
    per_actor_results: List[Dict[str, Any]],
    coord_result: Dict[str, Any] | None,
    k_list: List[int],
) -> Dict[int, List[float]]:
    """Flatten per-actor / coord latency lists into a single per-k list.

    Mirrors the aggregate construction at the end of ``main()``. Pulled
    out so the invalidation drill can compute summaries for the second
    measure pass without rerunning the main aggregation block.
    """
    aggregated: Dict[int, List[float]] = {k: [] for k in k_list}
    if coord_result is not None:
        for k, lats in coord_result["latencies_by_k"].items():
            aggregated[int(k)].extend(lats)
    else:
        for r in per_actor_results:
            for k, lats in r.get("latencies_by_k", {}).items():
                aggregated[int(k)].extend(lats)
    return aggregated


def _pct_delta(after: float, before: float) -> float:
    """Return ``(after - before) / before * 100`` with a zero-baseline guard.

    The invalidation drill reports the second-measure latency delta as
    a percentage; when the first measure recorded a zero baseline (no
    queries ran), the percentage is undefined and we return 0.0 rather
    than dividing by zero. Negative deltas (second pass faster) are
    preserved as-is so the operator can tell rehydrate-warm vs.
    fully-cold apart.
    """
    if before == 0.0:
        return 0.0
    return (after - before) / before * 100.0


def _run_invalidation_drill(
    *,
    args: argparse.Namespace,
    actors: List[Any],
    coord: Any | None,
    measure_qs: np.ndarray,
    k_list: List[int],
    partitions_for_actor: List[List[int]],
    dram_bytes: int,
    uri: str,
    first_pass_aggregated: Dict[int, List[float]],
    actor_l2_dirs: List[str],
    out_dir: Path,
) -> Dict[str, Any]:
    """Run the optional --simulate-invalidation drill (plan Phase 2.7).

    Sequence (per the distributed-cache benchmark plan's
    *Invalidation drill* section):

    1. Invalidate per actor (and coord under ``--mode sharded``) via
       ``Session.invalidate_index_cache(uri, index_addr)`` with one
       IOError retry. Worker actor methods resolve ``index_addr`` from
       the index name locally so the driver does not need to know the
       Lance-side address.
    2. Verify each actor's L2 prefix subdir is gone or in a
       ``.{prefix}.deleting-{nonce}/`` sentinel state via the v6
       ``snapshot_l2_dir`` helper. Any live non-deleting prefix is a
       freshness-contract violation and aborts the run.
    3. Re-run sharded prewarm to time the "cold L2 rehydration cost".
    4. Re-run the measure phase and compute per-k percentage deltas
       against the first-pass summaries.

    Writes ``out/invalidation.json`` with first/second per-k latency
    summaries, per-actor invalidate times, rehydrate-prewarm time,
    and percentage deltas. Returns the same payload so callers can
    log it inline.
    """
    print("\n=== Phase 2.7: invalidation drill ===")
    measure1_summary = {int(k): percentiles(first_pass_aggregated[k]) for k in k_list}

    # Step 1: invalidate per worker; coord too in --mode sharded.
    print(
        f"[driver] invalidating index cache on {args.num_actors} workers"
        f"{' + coordinator' if coord is not None else ''}"
        f" (index={args.index_name!r})"
    )
    t_inv = time.time()
    inv_results = ray.get(
        [a.invalidate_index_cache.remote(uri, args.index_name) for a in actors]
    )
    coord_inv_result: Dict[str, Any] | None = None
    if coord is not None:
        coord_inv_result = ray.get(
            coord.invalidate_index_cache.remote(uri, args.index_name)
        )
    invalidate_wall_s = time.time() - t_inv
    invalidate_per_actor_s = [float(r["duration_s"]) for r in inv_results]

    for r in inv_results:
        retried_tag = "  RETRIED" if r.get("retried") else ""
        print(
            f"  actor={r['actor_id']} duration={r['duration_s']:.3f}s "
            f"attempts={r['attempts']} index_addr={r['index_addr']}"
            f"{retried_tag}"
        )
    if coord_inv_result is not None:
        print(
            f"  coordinator duration={coord_inv_result['duration_s']:.3f}s "
            f"attempts={coord_inv_result['attempts']} "
            f"index_addr={coord_inv_result['index_addr']}"
            f"{'  RETRIED' if coord_inv_result.get('retried') else ''}"
        )

    # Step 2: verify L2 prefix dropped or in a .deleting-<nonce> sentinel
    # state on every actor. The freshness contract is: post-call, either
    # the per-prefix subdir under v1/ is gone, OR it has been atomically
    # renamed to .{prefix}.deleting-{nonce}/ for background removal.
    # Anything else (a live non-deleting subdir survives) means the
    # rename failed silently and the next query may hit stale L2.
    failed_invalidations: List[str] = []
    invalidation_verifications: List[Dict[str, Any]] = []
    for r in inv_results:
        snap = r.get("l2_snapshot") or {}
        prefix_dirs = snap.get("prefix_dirs") or []
        live = [pd for pd in prefix_dirs if not pd.get("deleting")]
        deleting = [pd for pd in prefix_dirs if pd.get("deleting")]
        ok = len(live) == 0
        invalidation_verifications.append(
            {
                "actor_id": r["actor_id"],
                "ok": ok,
                "live_prefixes": [pd.get("name") for pd in live],
                "deleting_prefixes": [pd.get("name") for pd in deleting],
                "tombstones_present": bool(snap.get("tombstones_present")),
                "l2_file_count": int(snap.get("file_count", 0)),
            }
        )
        if not ok:
            failed_invalidations.append(
                f"actor={r['actor_id']} live={[pd.get('name') for pd in live]}"
            )
    if failed_invalidations:
        raise RuntimeError(
            "Invalidation verification failed — Session.invalidate_index_cache "
            "returned without raising but the per-prefix L2 subdir is still "
            "live (not in a .deleting-<nonce> sentinel state) on "
            f"{len(failed_invalidations)} actor(s); the v6 freshness contract "
            "is violated and the next query may hit stale L2:\n  "
            + "\n  ".join(failed_invalidations)
        )
    print("[driver] invalidation verified — per-actor L2 prefixes dropped / deleting")

    # Step 3: re-run sharded prewarm. This is the "cold L2 rehydration"
    # cost the drill exists to measure. We reuse the same partition slice
    # the original prewarm used (sharded round-robin) so the rehydrated
    # cache is byte-for-byte equivalent to the post-prewarm state of
    # Phase 1.
    policy, ram_bytes_budget = _deterministic_prewarm_params(args.scenario, dram_bytes)
    print(
        f"[driver] re-running sharded prewarm (policy={policy!r}) — cold L2 rehydration"
    )
    t_rehyd = time.time()
    rehyd_results = ray.get(
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
    _assert_l2_validation_clean(rehyd_results)
    rehydrate_prewarm_s = time.time() - t_rehyd
    print(f"[driver] rehydrate prewarm done in {rehydrate_prewarm_s:.1f}s")

    # If the coord owned an IvfIndexState in metadata L1, we just dropped
    # it in step 1. Re-warmup the coord routing path so the first second-
    # pass query does not pay the index-open cost.
    if coord is not None:
        warmup_routing_q = make_query_vectors(1, args.dim, seed=args.seed + 3)[0]
        t_warm = time.time()
        ray.get(coord.warmup_routing.remote(warmup_routing_q.tolist()))
        print(f"[driver] coord routing rewarmup done in {time.time() - t_warm:.1f}s")

    # Step 4: re-run measure. Drives the same query plan as the first
    # pass so percentile deltas are apples-to-apples.
    print("[driver] re-running measure (post-invalidation)")
    second_per_actor_results, second_coord_result, measure2_wall_s = _run_measure_pass(
        mode=args.mode,
        actors=actors,
        coord=coord,
        measure_qs=measure_qs,
        k_list=k_list,
        num_actors=args.num_actors,
        prewarm=args.prewarm,
    )
    second_aggregated = _aggregate_latencies_by_k(
        second_per_actor_results, second_coord_result, k_list
    )
    measure2_summary = {int(k): percentiles(second_aggregated[k]) for k in k_list}
    print(f"[driver] measure2 wall-time: {measure2_wall_s:.1f}s")
    for k in k_list:
        print(format_latency_row("measure2", k, measure2_summary[k]))

    # Step 5: compute deltas + write invalidation.json.
    primary_k = int(k_list[0])
    m1 = measure1_summary[primary_k]
    m2 = measure2_summary[primary_k]
    delta_p50_pct = _pct_delta(m2["p50"], m1["p50"])
    delta_p95_pct = _pct_delta(m2["p95"], m1["p95"])
    delta_p99_pct = _pct_delta(m2["p99"], m1["p99"])
    delta_mean_pct = _pct_delta(m2["mean"], m1["mean"])

    delta_by_k = {
        str(k): {
            "delta_p50_pct": _pct_delta(
                measure2_summary[k]["p50"], measure1_summary[k]["p50"]
            ),
            "delta_p95_pct": _pct_delta(
                measure2_summary[k]["p95"], measure1_summary[k]["p95"]
            ),
            "delta_p99_pct": _pct_delta(
                measure2_summary[k]["p99"], measure1_summary[k]["p99"]
            ),
            "delta_mean_pct": _pct_delta(
                measure2_summary[k]["mean"], measure1_summary[k]["mean"]
            ),
        }
        for k in k_list
    }

    # Optional: snapshot L2 after rehydrate so the JSON carries the
    # rehydrated footprint per actor (lets operators correlate
    # rehydrate-cost-vs-bytes without a follow-up walk).
    post_rehydrate_l2 = []
    if actor_l2_dirs:
        post_rehydrate_l2 = ray.get(
            [
                actors[i].snapshot_l2_dir.remote(actor_l2_dirs[i])
                for i in range(args.num_actors)
            ]
        )

    payload: Dict[str, Any] = {
        "primary_k": primary_k,
        "k_list": k_list,
        "num_actors": args.num_actors,
        "mode": args.mode,
        "scenario": args.scenario,
        "prewarm": args.prewarm,
        # Plan-style flat fields (primary k) for the canonical schema.
        "measure1_p50_s": float(m1["p50"]),
        "measure1_p95_s": float(m1["p95"]),
        "invalidate_per_actor_s": invalidate_per_actor_s,
        "invalidate_wall_s": float(invalidate_wall_s),
        "rehydrate_prewarm_s": float(rehydrate_prewarm_s),
        "measure2_p50_s": float(m2["p50"]),
        "measure2_p95_s": float(m2["p95"]),
        "delta_p50_pct": float(delta_p50_pct),
        "delta_p95_pct": float(delta_p95_pct),
        "delta_p99_pct": float(delta_p99_pct),
        "delta_mean_pct": float(delta_mean_pct),
        # Per-k breakdown for callers that drive --k-list with multiple ks.
        "measure1_summary_by_k": {str(k): measure1_summary[k] for k in k_list},
        "measure2_summary_by_k": {str(k): measure2_summary[k] for k in k_list},
        "delta_by_k": delta_by_k,
        # Per-actor invalidation detail + L2 verification.
        "invalidations": [
            {
                "actor_id": r["actor_id"],
                "index_addr": r["index_addr"],
                "duration_s": float(r["duration_s"]),
                "attempts": int(r["attempts"]),
                "retried": bool(r.get("retried", False)),
                "retry_error": r.get("retry_error"),
            }
            for r in inv_results
        ],
        "coordinator_invalidation": (
            {
                "duration_s": float(coord_inv_result["duration_s"]),
                "attempts": int(coord_inv_result["attempts"]),
                "index_addr": coord_inv_result["index_addr"],
                "retried": bool(coord_inv_result.get("retried", False)),
                "retry_error": coord_inv_result.get("retry_error"),
            }
            if coord_inv_result is not None
            else None
        ),
        "invalidation_verifications": invalidation_verifications,
        "post_rehydrate_l2": [
            {
                "actor_id": snap.get("actor_id"),
                "exists": bool(snap.get("exists")),
                "file_count": int(snap.get("file_count", 0)),
                "apparent_bytes": int(snap.get("apparent_bytes", 0)),
                "disk_bytes": int(snap.get("disk_bytes", 0)),
                "tombstones_present": bool(snap.get("tombstones_present")),
            }
            for snap in post_rehydrate_l2
        ],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "invalidation.json"
    with out_path.open("w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(f"[driver] wrote {out_path}")

    return payload


def main() -> int:
    args = parse_args()
    k_list = parse_k_list(args.k_list)
    out_dir = Path(args.out_dir)

    # v6 rename: accept 'hybrid' on the CLI for back-compat, but normalize
    # everywhere downstream so the rest of the driver only sees
    # 'distributed' (matching the spec dict's `kind` from build_session).
    args.scenario = _normalize_scenario_alias(args.scenario)

    # Lance 7.0 distributed-cache port: `measure_sharded` and
    # `CoordinatorActor` depend on the Python `compute_partition_ids` /
    # `search_partitions` APIs. Per the issue body those APIs are
    # treated as verified; the driver no longer pre-blocks the path. The
    # actor (`HybridSearchActor.measure_sharded`,
    # `HybridSearchActor.search_partitions`, `CoordinatorActor.__init__`)
    # gates each call via ``hasattr`` and raises a clear error if a
    # pylance build is missing them — failing in the actor on the first
    # use rather than after a misleading driver-side allow.

    # Coord mode is meaningless without a sharded prewarm — the
    # coordinator routes to actors based on partition ownership, and
    # only sharded prewarm sets up the per-actor partition slice. Force
    # it so users don't end up routing to actors that never loaded the
    # partitions they're being asked about.
    if args.mode == "sharded" and args.prewarm != "sharded":
        print(
            f"[driver] --mode=sharded forces --prewarm=sharded (was {args.prewarm!r})"
        )
        args.prewarm = "sharded"

    # The invalidation drill only makes sense when there is a v6
    # distributed cache to invalidate (--scenario distributed) and a
    # deterministic per-actor partition slice to rehydrate (--prewarm
    # sharded). Reject other combinations early so we do not waste a
    # 10M-vector measure pass before discovering the drill cannot run.
    if args.simulate_invalidation:
        if args.scenario != "distributed":
            raise SystemExit(
                f"--simulate-invalidation requires --scenario=distributed "
                f"(got {args.scenario!r}); moka / no-cache sessions have no "
                "v6 distributed cache to invalidate"
            )
        if args.prewarm != "sharded":
            raise SystemExit(
                f"--simulate-invalidation requires --prewarm=sharded "
                f"(got {args.prewarm!r}); only the strict sharded prewarm "
                "rehydrates the L2 prefix deterministically after "
                "invalidation"
            )

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

    partition_sizes = load_index_partition_sizes(
        uri,
        args.endpoint_url,
        args.index_name,
        expected_num_partitions=args.num_partitions,
    )
    non_empty_partition_ids = [
        pid for pid, size in enumerate(partition_sizes) if int(size) > 0
    ]
    empty_partition_ids = [
        pid for pid, size in enumerate(partition_sizes) if int(size) == 0
    ]
    if empty_partition_ids:
        print(
            f"[driver] index stats: {len(empty_partition_ids)} empty IVF "
            "partition(s) will be excluded from sharded ownership, L2 "
            "validation, and coord routing; "
            f"sample={empty_partition_ids[:32]}"
        )
    print(
        f"[driver] index stats: non_empty_partitions="
        f"{len(non_empty_partition_ids)}/{args.num_partitions}"
    )

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
    partition_l1_mb = int(args.partition_l1_mb)
    metadata_l1_bytes = int(args.metadata_l1_mb) * MIB
    partition_l1_bytes = (partition_l1_mb * MIB) if partition_l1_mb > 0 else None
    print(
        f"[driver] per-actor v6 budgets: dram={args.dram_gb} GiB "
        f"metadata_l1={args.metadata_l1_mb} MiB "
        f"partition_l1={partition_l1_mb} MiB"
        f"{' (disabled)' if partition_l1_bytes is None else ''}"
    )
    if args.actor_resource or args.coordinator_resource:
        print(
            f"[driver] placement resources: actor={args.actor_resource!r} "
            f"coordinator={args.coordinator_resource!r}"
        )

    # No driver-side mkdir under v6: each HybridSearchActor creates its
    # own per-actor L2 subdirectory in-process before constructing the
    # session (see distributed_actor.HybridSearchActor.__init__). In a
    # real cluster the worker's NVMe is not visible from the driver
    # host, so mkdir-on-driver would be a misleading success.
    if args.scenario == "distributed":
        print(
            f"[driver] distributed L2 base: {args.nvme_dir} "
            f"(per-actor subdirs <nvme-dir>/actor-<i> are created by each actor)",
            file=sys.stderr,
        )

    metadata_bytes = (
        int(args.metadata_mb * MIB) if args.metadata_mb is not None else None
    )
    dram_bytes = int(args.dram_gb * GIB)
    if args.codecless_mb is not None:
        print(
            "[driver] --codecless-mb is a v4 hybrid knob with no v6 analog; ignored.",
            file=sys.stderr,
        )

    # Explicit namespace so check_l2_residency.py — which starts a
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
        # Per-actor L2 path; mkdir is the actor's responsibility (in
        # `HybridSearchActor.__init__`), not the driver's.
        per_actor_spec = build_per_actor_spec(
            args.scenario,
            actor_id=i,
            nvme_dir=args.nvme_dir,
            dram_bytes=dram_bytes,
            metadata_l1_bytes=metadata_l1_bytes,
            partition_l1_bytes=partition_l1_bytes,
            metadata_bytes=metadata_bytes,
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
    # ``partitions_for_actor`` is the per-actor "expected" non-empty
    # partition set consumed by sharded routing and by the post-prewarm /
    # post-measure residency probe. Empty IVF partitions retain centroid
    # ids but have no partition payload file, so they are intentionally
    # absent from ownership and L2 validation.
    #   * forced: every actor caches every non-empty partition.
    #   * sharded: non-empty partition ids assigned by id % num_actors.
    # Modes without a defined expectation (natural, none) leave the
    # default empty slices and the probe is gated off below.
    partitions_for_actor: List[List[int]] = [[] for _ in range(args.num_actors)]

    if args.prewarm == "forced":
        partitions_for_actor = [
            list(non_empty_partition_ids) for _ in range(args.num_actors)
        ]
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
                f"bytes={int(s.get('size_bytes', 0)):,}"
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
        # Round-robin assignment by partition id, restricted to non-empty
        # IVF partitions. Centroids are uniformly learned over the whole
        # dataset, so partition ids carry no spatial ordering — striding
        # by num_actors balances load without needing a count-based
        # scheme. If a future workload shows skew, swap this for an
        # assignment that weights by per-partition row count.
        partitions_for_actor = _partition_ids_by_actor(
            non_empty_partition_ids,
            args.num_actors,
        )
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
            if args.scenario == "distributed" and args.prewarm_ram_fraction != 1.0:
                # Hybrid prewarm no longer fills foyer L1, so the L1
                # fraction knob has no effect — call it out so users do
                # not assume their per-actor cache layout changed.
                print(
                    f"[driver] --prewarm-ram-fraction={args.prewarm_ram_fraction} "
                    "ignored under --scenario hybrid: hybrid_tiered places "
                    "every owned partition into L2 and leaves L1 cold; "
                    "there is no L1 budget to scale."
                )
            if args.scenario == "distributed":
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
            # Hard-fail before measure if any actor's L2 dir is missing
            # files for partitions the strict v6 path was supposed to
            # persist. Done before the per-actor status print so the
            # traceback carries the missing/extra partition samples and
            # is the first thing the operator sees.
            _assert_l2_validation_clean(prewarm_results)
            for r in prewarm_results:
                s = r["stats_post_prewarm"]
                ps = r["prewarm_stats"]
                stopped = ps.get("stopped_before")
                stopped_str = "—" if stopped is None else str(stopped)
                loaded_ram = int(ps.get("loaded_to_ram", 0))
                loaded_disk = int(ps.get("loaded_to_disk", 0))
                skipped = int(ps.get("skipped_existing", 0))
                owned_n = int(r["n_partitions"])
                if args.scenario == "distributed":
                    # Lance 7.0 strict `prewarm_index(name,
                    # partition_ids=...)` returns no Python-visible
                    # counters; success means every requested partition
                    # was persisted to L2 atomically (or it raised
                    # LanceError). The L2 file-count cross-check
                    # (verified by `_assert_l2_validation_clean` above)
                    # is the v6 analog of the v4 counter check; surface
                    # the count here so a successful run still prints
                    # how many `part-ivf-<id>.bin` files landed.
                    v = r.get("l2_validation") or {}
                    print(
                        f"  actor={r['actor_id']} prewarm={r['duration_s']:.1f}s "
                        f"owned={owned_n} "
                        f"l2_files={int(v.get('l2_file_count', 0))} "
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
                        f"cache_bytes={int(s.get('size_bytes', 0)):,}"
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
    if args.prewarm in ("sharded", "forced") and args.scenario == "distributed":
        actor_l2_dirs = [
            os.path.join(args.nvme_dir, f"actor-{i}") for i in range(args.num_actors)
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
                print(f"  actor={snap['actor_id']} L2 dir missing ({snap.get('path')})")
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
    # v6 aggregate-only probe: a filesystem walk under
    # ``{l2_dir}/v1/{sanitize(prefix)}/`` plus ``Session.size_bytes()``,
    # run on the actor via RPC. The probe is meaningful whenever the
    # driver has a defined "expected" per-actor partition set:
    #   * forced: every actor caches every partition.
    #   * sharded: per-actor round-robin slice.
    # ``no-cache`` is excluded because there is no cache to probe;
    # ``natural`` / ``none`` are excluded because the expected set is
    # undefined. ``distributed`` IS eligible -- this is the scenario
    # the v6 probe was designed for. The probe is side-effect-free
    # (filesystem walk only), so it can fire pre-measure without
    # perturbing the cache; ``--pre-measure-residency-probe`` keeps the
    # same flag name for back-compat.
    eligible_for_residency_probe = is_eligible_for_residency_probe(
        args.scenario, args.prewarm
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
    residency_nvme_dir = args.nvme_dir if args.scenario == "distributed" else None
    pre_measure_residency: List[Dict[str, Any]] = []
    if do_pre_residency_probe:
        pre_measure_residency = run_l2_residency_check(
            actors=actors,
            partitions_for_actor=partitions_for_actor,
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
            "--pre-measure-residency-probe to opt in. The v6 aggregate-"
            "only probe is side-effect-free (filesystem walk + "
            "Session.size_bytes()), but is gated symmetrically with the "
            "post-measure probe so a single flag controls both."
        )
    elif args.scenario == "no-cache":
        print(
            "[driver] residency check skipped — --scenario=no-cache "
            "populates no cache, so there is nothing to probe."
        )
    else:
        print(
            f"[driver] residency check skipped — prewarm={args.prewarm!r} "
            "has no defined per-actor expected partition set "
            "(probe requires --prewarm forced or sharded)"
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
            valid_partition_ids=non_empty_partition_ids,
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
        print(f"[driver] coord routing warmup done in {time.time() - t_warm:.1f}s")
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
    else:
        chunks = np.array_split(measure_qs, args.num_actors)
        n_per_actor = [len(c) for c in chunks]
        measure_method = "measure_sharded" if args.prewarm == "sharded" else "measure"
        print(
            f"[driver] measure ({measure_method}) — query slice sizes per actor: "
            f"{n_per_actor} (× {len(k_list)} k-values each)"
        )

    # v6 cache stats are cumulative `size_bytes` only; there is no
    # hit / miss counter to baseline-subtract here, so the v4
    # pre-measure snapshot the driver used to take is dropped.
    # Downstream callers wanting a delta should snapshot
    # `cache_stats` themselves outside the measure window.
    per_actor_results, coord_result, measure_wall_s = _run_measure_pass(
        mode=args.mode,
        actors=actors,
        coord=coord,
        measure_qs=measure_qs,
        k_list=k_list,
        num_actors=args.num_actors,
        prewarm=args.prewarm,
    )

    # ── Phase 2.5: post-measure residency check ──
    # Same probe shape as the (optional) post-prewarm one, so when both
    # snapshots exist the comparison below shows query-driven L1 churn
    # via the ``l1_size_bytes_at_probe`` delta. Under the
    # no-vector-L1-writeback policy a displaced L1 entry is dropped
    # from RAM only — vector partition L2 entries already exist from
    # deterministic prewarm, so an eviction does not change L2 state.
    # The L2 half of the check is exact under v6: file presence
    # one-to-one maps to L2 residency (see ``check_l2_residency.py``).
    # Runs unconditionally for sharded non-no-cache scenarios because
    # it runs *after* measure and so cannot pollute the measured
    # workload — unlike the pre-measure probe which is opt-in. Done
    # before actor close so the session is still alive to report
    # ``Session.size_bytes()``.
    post_measure_residency: List[Dict[str, Any]] = []
    if do_post_residency_probe:
        post_measure_residency = run_l2_residency_check(
            actors=actors,
            partitions_for_actor=partitions_for_actor,
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
            # v6 aggregate-only shift report: per-partition stayed /
            # evicted / promoted counts are gone (no no-load L1 probe).
            # Report L1 byte delta + L2 file/byte totals instead. A
            # non-zero L1 delta with stable L2 totals is the expected
            # signature of query-driven L1 churn under the
            # no-vector-L1-writeback policy.
            print("\n=== Residency shift (post-prewarm → post-measure) ===")
            print(f"  baseline: post-prewarm L1 {baseline_source}")
            for r in post_measure_residency:
                aid = r["actor_id"]
                pre_l1 = pre_by_id.get(aid, L1_SIZE_UNKNOWN)
                post_l1 = int(r.get("l1_size_bytes_at_probe", L1_SIZE_UNKNOWN))
                if pre_l1 == L1_SIZE_UNKNOWN or post_l1 == L1_SIZE_UNKNOWN:
                    delta_str = "?"
                else:
                    delta_str = format_bytes(post_l1 - pre_l1)
                still_in_l2 = len(r.get("in_l2", []))
                missing_from_l2 = len(r.get("missing", []))
                print(
                    f"  actor-{aid:<3} "
                    f"l1_pre={format_bytes(pre_l1) if pre_l1 != L1_SIZE_UNKNOWN else '?'} "
                    f"l1_post={format_bytes(post_l1) if post_l1 != L1_SIZE_UNKNOWN else '?'} "
                    f"Δl1={delta_str} "
                    f"l2_files={r['l2_file_count']:<5} "
                    f"in_l2={still_in_l2:<5} missing_from_l2={missing_from_l2:<5}"
                )

    # Post-measure L2 directory snapshot — diff against the post-prewarm
    # snapshot above. Stable file/byte totals are a coarse signal that
    # query L1 churn did not cause extra L2 writes (no vector-partition
    # eviction writeback). Foyer can still recycle blocks on overwrite,
    # so this only rules out *visible* growth and file-count churn —
    # not silent in-place overwrite — see l2_inspect.py.
    if (
        args.prewarm in ("sharded", "forced")
        and args.scenario == "distributed"
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
                print(f"  actor={aid} L2 dir missing ({snap.get('path')})")
                continue
            pre = pre_by_id.get(aid)
            delta = diff_snapshots(pre, snap) if pre else {}
            delta_str = ""
            if delta:
                pieces = [
                    f"apparent={format_bytes(delta['apparent_bytes_delta'])}",
                    f"disk={format_bytes(delta['disk_bytes_delta'])}",
                    f"files={delta['file_count_delta']:+d}",
                ]
                if delta.get("tombstones_added"):
                    pieces.append("tombstones_added=True")
                delta_str = "  Δ(" + ", ".join(pieces) + ")"
            print(
                f"  actor={aid} L2 dir={snap['path']} "
                f"files={snap['file_count']} "
                f"apparent={format_bytes(int(snap['apparent_bytes']))} "
                f"disk={format_bytes(int(snap['disk_bytes']))}" + delta_str
            )

    # ── Phase 2.7: optional invalidation drill ──
    # Only fires under --simulate-invalidation. The drill exercises the
    # Lance v6 freshness contract end-to-end: invalidate every actor's
    # (and coord's) cache, verify the per-prefix L2 subdir went away or
    # is in a .deleting-<nonce>/ sentinel state, re-prewarm to time the
    # cold-L2 rehydration cost, then re-measure to confirm warm latency
    # returns to first-pass numbers. The early CLI guard restricts this
    # combination to (distributed, sharded); the drill itself assumes
    # those preconditions hold.
    if args.simulate_invalidation:
        first_pass_aggregated = _aggregate_latencies_by_k(
            per_actor_results, coord_result, k_list
        )
        _run_invalidation_drill(
            args=args,
            actors=actors,
            coord=coord,
            measure_qs=measure_qs,
            k_list=k_list,
            partitions_for_actor=partitions_for_actor,
            dram_bytes=dram_bytes,
            uri=uri,
            first_pass_aggregated=first_pass_aggregated,
            actor_l2_dirs=actor_l2_dirs,
            out_dir=out_dir,
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
    total_size_bytes_post = 0

    if coord_result is not None:
        # Coord owns the per-query timing; per-actor results carry only
        # cache stats (no latencies_by_k). Aggregate latencies come from
        # the single coord return value.
        for k, lats in coord_result["latencies_by_k"].items():
            aggregated[int(k)].extend(lats)
        for r in per_actor_results:
            total_size_bytes_post += int(r["stats_post"].get("size_bytes", 0))
    else:
        for r in per_actor_results:
            for k, lats in r["latencies_by_k"].items():
                aggregated[int(k)].extend(lats)
            total_size_bytes_post += int(r["stats_post"].get("size_bytes", 0))

    total_queries = len(measure_qs) * len(k_list)
    print(
        f"\n=== Distributed summary ({args.num_actors} actors, "
        f"mode={args.mode}, scenario={args.scenario}, prewarm={args.prewarm}) ==="
    )
    print(
        f"measure wall-time: {measure_wall_s:.1f}s   "
        f"aggregate throughput: {total_queries / measure_wall_s:.1f} q/s"
    )
    # Lance 7.0 exposes only `Session.size_bytes()`; hit / miss counters
    # are gone, so the v4 aggregate hit_ratio has no v6 replacement.
    # Report cumulative L1 footprint across actors as the closest analog.
    print(
        f"aggregate session size_bytes (post-measure): "
        f"{format_bytes(total_size_bytes_post)} across {args.num_actors} actors"
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

    for line in _format_per_actor_summary_lines(per_actor_results, coord_result):
        print(line)

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
