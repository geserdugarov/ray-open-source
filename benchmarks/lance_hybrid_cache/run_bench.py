"""Driver for the Lance hybrid-cache IVF_RQ benchmark.

Uses the benchmark-local ScenarioActor + helpers and wires them with the
byte values and defaults used by the Ray benchmark harness.

Prereqs: MinIO up on :9000, bucket `lance-bench` exists, `tc netem`
applied to port 9000, NVMe dir writable. See README.md.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import ray

from l2_inspect import (
    diff_snapshots,
    format_bytes,
    format_l2_summary_line,
    snapshot_l2_dir,
)
from scenarios import GIB, MIB, build_scenario_specs

HERE = Path(__file__).resolve().parent

from _hybrid_cache_helpers import (  # noqa: E402
    DatasetSpec,
    ensure_dataset,
    make_query_vectors,
)
from bench_hybrid_cache_ivf_rq import (  # noqa: E402
    ScenarioActor,
    hybrid_l2_dir,
    maybe_drop_page_cache,
    print_summary,
    write_results,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scale", type=int, default=10_000_000)
    p.add_argument("--dim", type=int, default=1024)
    p.add_argument("--num-partitions", type=int, default=3000)
    p.add_argument("--num-bits", type=int, default=8)
    p.add_argument(
        "--dram-gb",
        type=float,
        default=4.0,
        help="L1 / moka cache size (GiB). 4 GiB << ~10 GB index forces eviction. "
        "Applies as the total DRAM budget to BOTH moka and hybrid so the "
        "comparison is same-DRAM (hybrid's only extra resource is L2 NVMe).",
    )
    p.add_argument(
        "--codecless-mb",
        type=int,
        default=None,
        help="When set, switch the hybrid scenario to with_hybrid_cache_advanced "
        "and reserve this many MiB of --dram-gb for the codec-less embedded "
        "Moka. The remainder becomes the foyer DRAM tier (in front of L2). "
        "Total hybrid DRAM stays at --dram-gb, matching moka. Use this to "
        "override Lance's default 90/10 foyer/Moka split when the codec-less "
        "working set (top-level index objects, scalar index pages, …) needs "
        "more headroom than the 10%% default.",
    )
    p.add_argument(
        "--l2-gb",
        type=float,
        default=30.0,
        help="Hybrid L2 (NVMe) capacity in GiB. Must be >= 1 GiB.",
    )
    p.add_argument(
        "--nvme-dir",
        type=str,
        default="/mnt/nvme/lance-l2",
        help="Parent dir for per-repeat L2 subdirectories on the NVMe mount.",
    )
    p.add_argument(
        "--metadata-mb",
        type=float,
        default=None,
        help="Lance metadata cache size in MiB. Applies to ALL scenarios "
        "(no-cache/moka/hybrid) so the comparison stays fair. Leave unset "
        "to use Lance's default. Set to 0 to make `no-cache` truly cold.",
    )
    p.add_argument("--k-list", type=str, default="10,100,1000")
    p.add_argument("--nprobes", type=int, default=32)
    p.add_argument("--warmup-queries", type=int, default=1024)
    p.add_argument("--measure-queries", type=int, default=5000)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument(
        "--scenarios",
        type=str,
        default="no-cache,moka,hybrid",
    )
    p.add_argument("--bucket", type=str, default="lance-bench")
    p.add_argument("--endpoint-url", type=str, default="http://127.0.0.1:9000")
    p.add_argument(
        "--drop-page-cache",
        action="store_true",
        help="sync && echo 3 > /proc/sys/vm/drop_caches between scenarios. "
        "Needs passwordless sudo; silently skipped otherwise.",
    )
    p.add_argument(
        "--reuse-l2",
        action="store_true",
        help="Keep hybrid L2 dir across repeats to measure warm-restart hit rate.",
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=str, default=str(HERE / "out"))
    p.add_argument("--skip-setup", action="store_true")
    return p.parse_args()


def parse_k_list(spec: str) -> List[int]:
    return [int(x) for x in spec.split(",") if x.strip()]


def print_l2_summary(results: List[dict]) -> None:
    """Per-scenario L2 footprint and warmup-phase hit ratio.

    The warmup pre-stats are captured by `ScenarioActor` in
    `stats_pre` but never printed; surfacing them lets us tell whether
    a high `stats_post.hit_ratio` came from a warm L2 (stats_pre.hits>0
    on entry) or from the warmup phase populating it.
    """
    hybrid_results = [r for r in results if "l2_post_snapshot" in r]
    if hybrid_results:
        print("\n=== L2 directory footprint ===")
        for r in hybrid_results:
            pre = r.get("l2_pre_snapshot") or {}
            post = r["l2_post_snapshot"]
            print(format_l2_summary_line(r["name"], r["repeat"], post, r.get("l2_delta")))
            if pre.get("exists") and pre.get("file_count", 0) > 0:
                print(
                    f"      (pre-snapshot: files={pre['file_count']}, "
                    f"apparent={format_bytes(pre['apparent_bytes'])}, "
                    f"disk={format_bytes(pre['disk_bytes'])})"
                )

    print("\n=== Warmup-phase counters (stats_pre) ===")
    for r in results:
        pre = r.get("stats_pre", {})
        post = r.get("stats_post", {})
        pre_hits = int(pre.get("hits", 0))
        pre_misses = int(pre.get("misses", 0))
        pre_total = pre_hits + pre_misses
        pre_ratio = (pre_hits / pre_total) if pre_total else 0.0
        # Measure-phase deltas: subtracting pre from post isolates what
        # happened during the timed run vs. what the warmup paid for.
        delta_hits = int(post.get("hits", 0)) - pre_hits
        delta_misses = int(post.get("misses", 0)) - pre_misses
        delta_total = delta_hits + delta_misses
        delta_ratio = (delta_hits / delta_total) if delta_total else 0.0
        print(
            f"  [{r['name']} r{r['repeat']}] "
            f"warmup hits={pre_hits} misses={pre_misses} ratio={pre_ratio:.2%}  "
            f"measure hits={delta_hits} misses={delta_misses} ratio={delta_ratio:.2%}"
        )


def write_l2_inventory(out_dir: Path, results: List[dict]) -> None:
    """One CSV row per scenario × repeat for the L2 directory snapshot.

    Separate from `summary.csv` (which is keyed by k) so each row is
    unambiguous and the columns don't repeat across k values.
    """
    rows: List[Dict[str, Any]] = []
    for r in results:
        post = r.get("l2_post_snapshot")
        if not post:
            continue
        delta = r.get("l2_delta") or {}
        rows.append(
            {
                "scenario": r["name"],
                "repeat": r["repeat"],
                "l2_path": post.get("path", ""),
                "l2_exists": post.get("exists", False),
                "l2_apparent_bytes": post.get("apparent_bytes", 0),
                "l2_disk_bytes": post.get("disk_bytes", 0),
                "l2_file_count": post.get("file_count", 0),
                "l2_apparent_bytes_delta": delta.get("apparent_bytes_delta", 0),
                "l2_disk_bytes_delta": delta.get("disk_bytes_delta", 0),
                "l2_file_count_delta": delta.get("file_count_delta", 0),
            }
        )
    if not rows:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "l2_inventory.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    args = parse_args()
    k_list = parse_k_list(args.k_list)
    out_dir = Path(args.out_dir)
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]

    dram_bytes = int(args.dram_gb * GIB)
    l2_bytes = int(args.l2_gb * GIB)
    metadata_bytes = (
        int(args.metadata_mb * MIB) if args.metadata_mb is not None else None
    )
    codecless_bytes = (
        int(args.codecless_mb * MIB) if args.codecless_mb is not None else None
    )
    scenario_specs = build_scenario_specs(
        scenarios,
        dram_bytes=dram_bytes,
        l2_bytes=l2_bytes,
        nvme_dir=args.nvme_dir,
        metadata_bytes=metadata_bytes,
        codecless_bytes=codecless_bytes,
    )
    if codecless_bytes is not None:
        foyer_mib = (dram_bytes - codecless_bytes) // MIB
        print(
            f"[driver] hybrid advanced split: foyer L1 = {foyer_mib} MiB, "
            f"codec-less Moka = {args.codecless_mb} MiB "
            f"(total DRAM = {args.dram_gb} GiB, same as moka)"
        )
    if not scenario_specs:
        print(f"[driver] ERROR: no valid scenarios in {args.scenarios!r}", file=sys.stderr)
        return 2

    spec = DatasetSpec(
        scale=args.scale,
        dim=args.dim,
        num_partitions=args.num_partitions,
        num_bits=args.num_bits,
        seed=args.seed,
    )

    # MinIO credentials for the driver process (ensure_dataset) and actors.
    # Set unconditionally so any pre-existing AWS_* envvars (e.g. a developer
    # logged into a real S3 account) don't hijack the local MinIO calls in
    # the driver while the actors get forced credentials via runtime_env
    # below — both sides must agree.
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

    # Identical seeded queries across scenarios; warmup disjoint from measure.
    warmup_qs = make_query_vectors(args.warmup_queries, args.dim, seed=args.seed + 1)
    measure_qs = make_query_vectors(args.measure_queries, args.dim, seed=args.seed + 2)
    print(
        f"[driver] queries: warmup={len(warmup_qs)} measure={len(measure_qs)} "
        f"k_list={k_list} nprobes={args.nprobes}"
    )

    ray.init(
        runtime_env={
            "working_dir": str(HERE),
            "env_vars": minio_env,
        },
        ignore_reinit_error=True,
    )

    all_results = []
    t0 = time.time()
    for scenario in scenario_specs:
        for repeat in range(args.repeats):
            maybe_drop_page_cache(args.drop_page_cache, print)

            spec_for_actor = dict(scenario)
            if scenario["kind"] == "hybrid":
                spec_for_actor["l2_dir"] = hybrid_l2_dir(
                    args.nvme_dir, scenario["name"], repeat, args.reuse_l2
                )
                print(f"[driver] hybrid L2 dir: {spec_for_actor['l2_dir']}")

            print(
                f"\n[driver] {scenario['name']} repeat {repeat + 1}/{args.repeats}"
            )
            l2_pre = (
                snapshot_l2_dir(spec_for_actor["l2_dir"])
                if scenario["kind"] == "hybrid"
                else None
            )
            actor = ScenarioActor.remote()
            future = actor.run.remote(
                scenario["name"],
                spec_for_actor,
                uri,
                warmup_qs.tolist(),
                measure_qs.tolist(),
                k_list,
                args.nprobes,
                args.endpoint_url,
            )
            result = ray.get(future)
            result["repeat"] = repeat
            if scenario["kind"] == "hybrid":
                # Snapshot after the actor has called sess.close(), so foyer
                # has flushed and released its O_DIRECT fds.
                l2_post = snapshot_l2_dir(spec_for_actor["l2_dir"])
                result["l2_pre_snapshot"] = l2_pre
                result["l2_post_snapshot"] = l2_post
                result["l2_delta"] = diff_snapshots(l2_pre, l2_post)
            all_results.append(result)
            ray.kill(actor)

    ray.shutdown()

    write_results(out_dir, all_results)
    write_l2_inventory(out_dir, all_results)
    print_summary(all_results)
    print_l2_summary(all_results)
    print(
        f"\n[driver] wrote {out_dir}/results.jsonl, {out_dir}/summary.csv, "
        f"{out_dir}/l2_inventory.csv "
        f"(total wall-time {(time.time() - t0) / 60:.1f} min)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
