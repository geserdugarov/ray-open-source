# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Benchmark: Lance distributed DRAM + NVMe cache on IVF_RQ vector search.

Compares three cache configurations on the same 10M × 1024-d dataset
hosted on MinIO:

  * no-cache  — `index_cache_size_bytes=0`, every partition load
                hits MinIO.
  * moka      — DRAM-only Moka cache sized smaller than the working
                set so it thrashes on eviction.
  * distributed — metadata L1 + decoded-partition L1 + NVMe L2 via
                  `Session.with_distributed_cache`.

One Ray actor is spawned per scenario × repeat; each runs in a
fresh Python process so cache state starts cold (or, for `--reuse-l2`
on the distributed scenario, warmed from the NVMe tier).

See README.md in this directory for full run instructions.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import ray

sys.path.insert(0, str(Path(__file__).parent))
from _hybrid_cache_helpers import (  # noqa: E402
    GIB,
    MIB,
    DatasetSpec,
    ScenarioResult,
    ensure_dataset,
    format_latency_row,
    make_query_vectors,
    percentiles,
)
from scenarios import (  # noqa: E402
    build_scenario_spec,
    distributed_l2_dir_for_repeat,
    per_actor_l2_dir,
)


def _nonneg_int(value: str) -> int:
    """argparse type: reject negative values for v6 L1 sizing flags.

    `--partition-l1-mb 0` disables the partition L1 tier; negative
    values are not a valid disable spelling and would have been
    silently mapped to None under the old `> 0` guard, hiding a typo.
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
    p.add_argument("--num-bits", type=int, default=1)
    p.add_argument(
        "--dram-mb",
        type=int,
        default=128,
        help="DRAM cap for moka (index_cache_size) and L1 for hybrid. Sized so "
        "not all partitions fit.",
    )
    p.add_argument(
        "--metadata-l1-mb",
        type=_nonneg_int,
        default=64,
        help="v6 metadata-L1 budget for the distributed scenario (MiB). Holds "
        "IvfIndexState, IndexMetadata, FragReuseIndex, ScalarIndexDetails, "
        "etc.; sizing too small defeats the per-query routing path. Default 64.",
    )
    p.add_argument(
        "--partition-l1-mb",
        type=_nonneg_int,
        default=1024,
        help="v6 decoded-partition L1 budget for the distributed scenario (MiB). "
        "Pass 0 to disable the partition-L1 tier (every decode hits L2); "
        "negative values are rejected. Default 1024.",
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
        default=4.0,
        help="Deprecated v4 hybrid L2-capacity knob. v6 has no L2 capacity "
        "bookkeeping; size the filesystem yourself. Ignored.",
    )
    p.add_argument(
        "--nvme-dir",
        type=str,
        default="/mnt/nvme/lance-l2",
        help="Filesystem path on the NVMe device for the hybrid L2 tier.",
    )
    p.add_argument("--k-list", type=str, default="10,100,1000")
    p.add_argument("--nprobes", type=int, default=32)
    p.add_argument("--warmup-queries", type=int, default=512)
    p.add_argument("--measure-queries", type=int, default=500)
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
        help="Run `sync && echo 3 > /proc/sys/vm/drop_caches` between scenarios. "
        "Requires passwordless sudo; silently skipped otherwise.",
    )
    p.add_argument(
        "--reuse-l2",
        action="store_true",
        help="Keep the hybrid scenario's L2 directory between repeats to test "
        "warm-restart hit rate.",
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out-dir", type=str, default="./bench_out")
    p.add_argument(
        "--skip-setup",
        action="store_true",
        help="Assume the dataset + index already exist at the URI.",
    )
    return p.parse_args()


def parse_k_list(spec: str) -> list[int]:
    return [int(x) for x in spec.split(",") if x.strip()]


def maybe_drop_page_cache(enabled: bool, log) -> None:
    if not enabled:
        return
    try:
        subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            check=True,
            timeout=10,
        )
        log("[driver] dropped page cache")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        log("[driver] WARNING: could not drop page cache (need passwordless sudo?)")


def build_scenario_specs(args: argparse.Namespace) -> list[dict]:
    """Build one v6 spec per scenario in --scenarios for this single-actor driver.

    Aliases 'hybrid' -> 'distributed' for back-compat with v4 invocations;
    --codecless-mb / --l2-gb are accepted for CLI compatibility but ignored
    under the v6 distributed cache.
    """
    wanted = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    if args.codecless_mb is not None:
        print(
            "[driver] --codecless-mb is a v4 hybrid knob with no v6 analog; ignored.",
            file=sys.stderr,
        )
    specs: list[dict] = []
    aliased_hybrid = False
    for raw in wanted:
        name = raw
        if name == "hybrid":
            if not aliased_hybrid:
                print(
                    "[driver] --scenarios: aliasing 'hybrid' -> 'distributed' "
                    "(v6 rename).",
                    file=sys.stderr,
                )
                aliased_hybrid = True
            name = "distributed"
        partition_l1_mb = int(getattr(args, "partition_l1_mb", 1024))
        specs.append(
            build_scenario_spec(
                name,
                actor_id=0,
                dram_bytes=args.dram_mb * MIB,
                nvme_dir=args.nvme_dir,
                metadata_l1_bytes=int(args.metadata_l1_mb) * MIB,
                partition_l1_bytes=(partition_l1_mb * MIB) if partition_l1_mb > 0 else None,
            )
        )
    return specs


@ray.remote
class ScenarioActor:
    """Runs one scenario in its own process (one Python, one Lance, one cache)."""

    def run(
        self,
        name: str,
        session_spec: dict,
        uri: str,
        warmup_vectors,
        measure_vectors,
        k_list: list[int],
        nprobes: int,
        endpoint_url: str,
    ) -> dict:
        import os as _os
        import time as _time

        import lance  # noqa: F401
        from _hybrid_cache_helpers import (
            build_session,
            measure,
            minio_storage_options,
            size_bytes_stats,
            warmup,
        )

        t_start = _time.time()
        # The v6 distributed-cache session takes an exclusive lock on
        # `{l2_dir}/lance-distributed.lock` and rejects a missing dir.
        # Create the per-actor dir in-process — driver-side mkdir would
        # only see the head-node filesystem in a multi-node cluster.
        if session_spec.get("kind") == "distributed":
            _os.makedirs(session_spec["l2_dir"], exist_ok=True)
        sess = build_session(session_spec)

        storage_options = minio_storage_options(endpoint_url)
        ds = lance.dataset(uri, session=sess, storage_options=storage_options)

        warmup(ds, warmup_vectors, nprobes=nprobes)
        stats_pre = size_bytes_stats(sess)
        latencies_by_k = measure(ds, measure_vectors, k_list, nprobes=nprobes)
        stats_post = size_bytes_stats(sess)

        # `Session.close()` existed in the v4 cache fork but was removed across
        # the Lance 7.0 distributed-cache line and newer: the
        # exclusive `{l2_dir}/lance-distributed.lock` flock is released when the
        # Session is dropped and the actor process exits (`ray.kill` follows in
        # the driver). Guard the call so the bench runs on the v6 builds that
        # have no `close()` and still closes explicitly on any build retaining it.
        if hasattr(sess, "close"):
            sess.close()
        return {
            "name": name,
            "stats_pre": stats_pre,
            "stats_post": stats_post,
            "latencies_by_k": {int(k): v for k, v in latencies_by_k.items()},
            "duration_s": _time.time() - t_start,
        }


def hybrid_l2_dir(base: str, scenario_name: str, repeat: int, reuse_l2: bool) -> str:
    if reuse_l2:
        return str(Path(base) / scenario_name)
    return str(Path(base) / f"{scenario_name}-r{repeat}-{int(time.time())}")


def write_results(out_dir: Path, results: list[dict]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "results.jsonl").open("w") as f:
        for r in results:
            # JSON requires string keys; the in-memory dict keys are ints.
            rr = dict(r)
            rr["latencies_by_k"] = {
                str(k): v for k, v in r["latencies_by_k"].items()
            }
            f.write(json.dumps(rr) + "\n")

    rows: list[dict] = []
    for r in results:
        pre = int(r.get("stats_pre", {}).get("size_bytes", 0))
        post = int(r.get("stats_post", {}).get("size_bytes", 0))
        for k_str, lats in r["latencies_by_k"].items():
            k = int(k_str)
            pct = percentiles(lats)
            rows.append(
                {
                    "scenario": r["name"],
                    "repeat": r["repeat"],
                    "k": k,
                    "p50_s": pct["p50"],
                    "p95_s": pct["p95"],
                    "p99_s": pct["p99"],
                    "mean_s": pct["mean"],
                    "n": pct["n"],
                    # v6: `Session.size_bytes()` is the only cumulative
                    # session-size accessor; v4 hit / miss / entry counters
                    # are gone from the Python-visible stats surface.
                    "session_size_bytes_pre": pre,
                    "session_size_bytes_post": post,
                    "session_size_bytes_delta": post - pre,
                    "duration_s": r["duration_s"],
                }
            )

    fieldnames = list(rows[0].keys()) if rows else []
    with (out_dir / "summary.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def print_summary(results: list[dict]) -> None:
    print("\n=== Summary ===")
    for r in results:
        pre = int(r.get("stats_pre", {}).get("size_bytes", 0))
        post = int(r.get("stats_post", {}).get("size_bytes", 0))
        # v6: only `Session.size_bytes()` is exposed; the v4
        # `hit_ratio` / `cache_entries` indicators are unavailable.
        print(
            f"\n[{r['name']} r{r['repeat']}]  "
            f"duration={r['duration_s']:.1f}s  "
            f"session_size: pre={pre:,} -> post={post:,}  "
            f"delta={post - pre:+,}"
        )
        for k_str, lats in r["latencies_by_k"].items():
            pct = percentiles(lats)
            print(format_latency_row(r["name"], int(k_str), pct))


def main() -> int:
    args = parse_args()
    k_list = parse_k_list(args.k_list)
    out_dir = Path(args.out_dir)

    spec = DatasetSpec(
        scale=args.scale,
        dim=args.dim,
        num_partitions=args.num_partitions,
        num_bits=args.num_bits,
        seed=args.seed,
    )

    # Hoist the (hours-long) dataset+index creation out of any actor so
    # it runs exactly once per `(scale, dim, num_partitions, num_bits)`
    # tuple. Subsequent runs reuse the URI.
    if args.skip_setup:
        uri = spec.uri(args.bucket)
        print(f"[driver] skipping setup; using {uri}")
    else:
        os.environ.setdefault("AWS_ENDPOINT_URL", args.endpoint_url)
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
        os.environ.setdefault("AWS_REGION", "us-east-1")
        os.environ.setdefault("AWS_ALLOW_HTTP", "true")
        uri = ensure_dataset(
            spec, bucket=args.bucket, endpoint_url=args.endpoint_url
        )

    # Same query vectors across scenarios so differences are pure
    # cache effect. Warm-up + measurement are seeded independently
    # so the measurement set isn't the same set the cache was warmed
    # on.
    warmup_qs = make_query_vectors(args.warmup_queries, args.dim, seed=args.seed + 1)
    measure_qs = make_query_vectors(args.measure_queries, args.dim, seed=args.seed + 2)

    ray.init(
        runtime_env={
            "working_dir": str(Path(__file__).parent),
            "env_vars": {
                "AWS_ENDPOINT_URL": args.endpoint_url,
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin",
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true",
            },
        },
        ignore_reinit_error=True,
    )

    scenario_specs = build_scenario_specs(args)
    if not scenario_specs:
        print(f"ERROR: no valid scenarios in {args.scenarios!r}", file=sys.stderr)
        return 2

    # No driver-side mkdir under v6: the actor creates its own L2 dir
    # in-process (see ScenarioActor.run). In a multi-node cluster the
    # head node's filesystem is not the worker's NVMe, so mkdir-on-driver
    # would be misleading.
    if any(s["kind"] == "distributed" for s in scenario_specs):
        print(
            f"[driver] distributed L2 base: {args.nvme_dir} "
            f"(actor process will mkdir {per_actor_l2_dir(args.nvme_dir, 0)})"
        )

    all_results: list[dict] = []
    for scenario in scenario_specs:
        for repeat in range(args.repeats):
            maybe_drop_page_cache(args.drop_page_cache, print)

            spec_for_actor = dict(scenario)
            if scenario["kind"] == "distributed":
                # With `--reuse-l2`, share `<nvme-dir>/actor-0/` across
                # repeats so steady-state is observable; without it, each
                # repeat gets a fresh timestamped subdir so cold-start
                # latency is honest. Actor mkdirs the dir in-process
                # before constructing the session.
                spec_for_actor["l2_dir"] = distributed_l2_dir_for_repeat(
                    args.nvme_dir,
                    actor_id=0,
                    repeat=repeat,
                    reuse_l2=args.reuse_l2,
                )
                print(
                    f"[driver] distributed L2 dir (repeat {repeat}): "
                    f"{spec_for_actor['l2_dir']}"
                )

            print(
                f"\n[driver] running scenario {scenario['name']} (repeat {repeat + 1}"
                f"/{args.repeats})"
            )
            actor = ScenarioActor.remote()
            future = actor.run.remote(
                scenario["name"],
                spec_for_actor,
                uri,
                warmup_qs,
                measure_qs,
                k_list,
                args.nprobes,
                args.endpoint_url,
            )
            result = ray.get(future)
            result["repeat"] = repeat
            all_results.append(result)
            ray.kill(actor)

    ray.shutdown()

    write_results(out_dir, all_results)
    print_summary(all_results)
    print(f"\n[driver] wrote results to {out_dir}/results.jsonl and {out_dir}/summary.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
