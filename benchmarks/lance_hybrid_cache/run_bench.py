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
from scenarios import GIB, MIB, build_scenario_specs, distributed_l2_dir_for_repeat

HERE = Path(__file__).resolve().parent

from _hybrid_cache_helpers import (  # noqa: E402
    DatasetSpec,
    ensure_dataset,
    make_query_vectors,
)
from bench_hybrid_cache_ivf_rq import (  # noqa: E402
    ScenarioActor,
    maybe_drop_page_cache,
    print_summary,
    write_results,
)


def _nonneg_int(value: str) -> int:
    """argparse type: reject negative values for v6 L1 sizing flags.

    `--partition-l1-mb 0` disables the partition L1 tier; negative
    values would silently disable under a `> 0` guard, hiding typos.
    """
    iv = int(value)
    if iv < 0:
        raise argparse.ArgumentTypeError(f"value must be >= 0; got {iv}")
    return iv


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
        help="DRAM budget for the `moka` scenario (GiB). Ignored for "
        "`--scenarios distributed` (whose DRAM is sized by --metadata-l1-mb + "
        "--partition-l1-mb).",
    )
    p.add_argument(
        "--metadata-l1-mb",
        type=_nonneg_int,
        default=64,
        help="v6 metadata-L1 budget for the distributed scenario (MiB). Holds "
        "IvfIndexState / IndexMetadata / FragReuseIndex / ScalarIndexDetails. "
        "Default 64.",
    )
    p.add_argument(
        "--partition-l1-mb",
        type=_nonneg_int,
        default=1024,
        help="v6 decoded-partition L1 budget for the distributed scenario "
        "(MiB). Pass 0 to disable the partition-L1 tier (every decode hits "
        "L2); negative values are rejected. Default 1024.",
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
        default=30.0,
        help="Deprecated v4 hybrid L2-capacity knob. v6 has no L2 capacity "
        "bookkeeping; size the actor's NVMe filesystem yourself. Ignored.",
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
        default="no-cache,moka,distributed",
        help="Comma-separated subset of {no-cache, moka, distributed}. "
        "Accepts 'hybrid' as a deprecated alias for 'distributed'.",
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
    """Per-scenario L2 footprint and session-size snapshots.

    Lance 7.0 exposes only `Session.size_bytes()`, so per-phase hit/miss
    decomposition (the v4 "Warmup-phase counters" block) has no v6 analog.
    The session-size delta (post - pre) is reported as the cumulative
    growth signal; treat it as informative, not a hit-ratio replacement.
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

    print("\n=== Session size (Lance 7.0 size_bytes) ===")
    for r in results:
        pre = int(r.get("stats_pre", {}).get("size_bytes", 0))
        post = int(r.get("stats_post", {}).get("size_bytes", 0))
        print(
            f"  [{r['name']} r{r['repeat']}] "
            f"session_size: pre={pre:,} -> post={post:,}  delta={post - pre:+,}"
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
    metadata_bytes = (
        int(args.metadata_mb * MIB) if args.metadata_mb is not None else None
    )
    partition_l1_mb = int(args.partition_l1_mb)
    metadata_l1_bytes = int(args.metadata_l1_mb) * MIB
    partition_l1_bytes = (partition_l1_mb * MIB) if partition_l1_mb > 0 else None
    if args.codecless_mb is not None:
        print(
            "[driver] --codecless-mb is a v4 hybrid knob with no v6 analog; ignored.",
            file=sys.stderr,
        )
    scenario_specs = build_scenario_specs(
        scenarios,
        dram_bytes=dram_bytes,
        l2_bytes=int(args.l2_gb * GIB),
        nvme_dir=args.nvme_dir,
        metadata_bytes=metadata_bytes,
        metadata_l1_bytes=metadata_l1_bytes,
        partition_l1_bytes=partition_l1_bytes,
    )
    print(
        f"[driver] v6 distributed budgets: dram(moka)={args.dram_gb} GiB "
        f"metadata_l1={args.metadata_l1_mb} MiB "
        f"partition_l1={partition_l1_mb} MiB"
        f"{' (disabled)' if partition_l1_bytes is None else ''}"
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
            # v6 distributed: with `--reuse-l2`, all repeats share the
            # per-actor L2 dir so steady-state is observable; otherwise
            # each repeat gets a fresh timestamped subdir so cold-start
            # latency is measured honestly. The actor mkdirs it
            # in-process before constructing the session.
            if scenario["kind"] == "distributed":
                spec_for_actor["l2_dir"] = distributed_l2_dir_for_repeat(
                    args.nvme_dir,
                    actor_id=0,
                    repeat=repeat,
                    reuse_l2=args.reuse_l2,
                )
                print(f"[driver] distributed L2 dir: {spec_for_actor['l2_dir']}")

            print(
                f"\n[driver] {scenario['name']} repeat {repeat + 1}/{args.repeats}"
            )
            l2_pre = (
                snapshot_l2_dir(spec_for_actor["l2_dir"])
                if scenario["kind"] == "distributed"
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
            if scenario["kind"] == "distributed":
                # Snapshot after the actor returns. On the v6 distributed cache
                # L2 writes are durable at prewarm time (wait_for_disk), not at
                # close, so the post-run snapshot is valid even though v6 has no
                # Session.close() (see ScenarioActor.run / HybridSearchActor.close).
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
