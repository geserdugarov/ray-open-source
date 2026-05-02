# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Benchmark: hybrid DRAM + NVMe cache on IVF_RQ vector search.

Compares three cache configurations on the same 10M × 1024-d dataset
hosted on MinIO:

  * no-cache  — `index_cache_size_bytes=0`, every partition load
                hits MinIO.
  * moka      — DRAM-only Moka cache sized smaller than the working
                set so it thrashes on eviction.
  * hybrid    — DRAM + NVMe L2 via `Session.with_hybrid_cache`.

One Ray actor is spawned per scenario × repeat; each runs in a
fresh Python process so cache state starts cold (or, for `--reuse-l2`
on the hybrid scenario, warmed from the NVMe tier).

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
        "--codecless-mb",
        type=int,
        default=None,
        help="When set, switches the hybrid scenario to with_hybrid_cache_advanced "
        "and reserves this many MiB of --dram-mb for the codec-less embedded "
        "Moka. The remainder goes to the foyer DRAM tier (L1). Use to override "
        "Lance's default 90/10 foyer/Moka split when the codec-less working set "
        "needs a different reserve — e.g. --dram-mb 4096 --codecless-mb 64 "
        "yields ~3.94 GiB foyer L1 and 64 MiB Moka.",
    )
    p.add_argument(
        "--l2-gb",
        type=float,
        default=4.0,
        help="Hybrid L2 (NVMe) capacity in GiB. Must be >=1 GiB for the default "
        "256 MiB block size.",
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
        default="no-cache,moka,hybrid",
        help="Comma-separated subset of {no-cache, moka, hybrid}.",
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
    wanted = {s.strip() for s in args.scenarios.split(",") if s.strip()}
    specs: list[dict] = []
    if "no-cache" in wanted:
        specs.append({"name": "no-cache", "kind": "no-cache"})
    if "moka" in wanted:
        specs.append(
            {
                "name": "moka",
                "kind": "moka",
                "index_cache_size_bytes": args.dram_mb * MIB,
            }
        )
    if "hybrid" in wanted:
        hybrid_spec: dict = {
            "name": "hybrid",
            "kind": "hybrid",
            "l2_dir": args.nvme_dir,
            "l2_capacity_bytes": int(args.l2_gb * GIB),
        }
        if args.codecless_mb is None:
            # Combined L1 budget; with_hybrid_cache splits it 90/10
            # between foyer L1 and codec-less Moka internally (foyer
            # gets the bulk).
            hybrid_spec["l1_capacity_bytes"] = args.dram_mb * MIB
        else:
            # Independent sizing via with_hybrid_cache_advanced. --dram-mb stays
            # the total DRAM budget across scenarios; --codecless-mb carves off
            # a slice for the codec-less Moka, leaving the rest for foyer L1.
            codecless = args.codecless_mb * MIB
            total_dram = args.dram_mb * MIB
            if codecless >= total_dram:
                raise ValueError(
                    f"--codecless-mb={args.codecless_mb} must be smaller than "
                    f"--dram-mb={args.dram_mb}; nothing left for foyer L1"
                )
            hybrid_spec["l1_capacity_bytes"] = total_dram - codecless
            hybrid_spec["codecless_capacity_bytes"] = codecless
        specs.append(hybrid_spec)
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
        import time as _time

        import lance  # noqa: F401
        from _hybrid_cache_helpers import (
            build_session,
            measure,
            minio_storage_options,
            warmup,
        )

        t_start = _time.time()
        sess = build_session(session_spec)

        storage_options = minio_storage_options(endpoint_url)
        ds = lance.dataset(uri, session=sess, storage_options=storage_options)

        warmup(ds, warmup_vectors, nprobes=nprobes)
        stats_pre = sess.index_cache_stats()
        latencies_by_k = measure(ds, measure_vectors, k_list, nprobes=nprobes)
        stats_post = sess.index_cache_stats()

        sess.close()
        return {
            "name": name,
            "stats_pre": dict(stats_pre),
            "stats_post": dict(stats_post),
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
        for k_str, lats in r["latencies_by_k"].items():
            k = int(k_str)
            pct = percentiles(lats)
            total = r["stats_post"]["hits"] + r["stats_post"]["misses"]
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
                    "hits": r["stats_post"]["hits"],
                    "misses": r["stats_post"]["misses"],
                    "hit_ratio": (r["stats_post"]["hits"] / total) if total else 0.0,
                    "num_entries": r["stats_post"]["num_entries"],
                    "cache_size_bytes": r["stats_post"]["size_bytes"],
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
        total = r["stats_post"]["hits"] + r["stats_post"]["misses"]
        hit_ratio = (r["stats_post"]["hits"] / total) if total else 0.0
        print(
            f"\n[{r['name']} r{r['repeat']}]  "
            f"duration={r['duration_s']:.1f}s  "
            f"hit_ratio={hit_ratio:.2%}  "
            f"cache_entries={r['stats_post']['num_entries']}  "
            f"cache_bytes={r['stats_post']['size_bytes']:,}"
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

    if any(s["kind"] == "hybrid" for s in scenario_specs):
        Path(args.nvme_dir).mkdir(parents=True, exist_ok=True)

    all_results: list[dict] = []
    for scenario in scenario_specs:
        for repeat in range(args.repeats):
            maybe_drop_page_cache(args.drop_page_cache, print)

            spec_for_actor = dict(scenario)
            if scenario["kind"] == "hybrid":
                spec_for_actor["l2_dir"] = hybrid_l2_dir(
                    args.nvme_dir, scenario["name"], repeat, args.reuse_l2
                )
                print(f"[driver] hybrid L2 directory: {spec_for_actor['l2_dir']}")

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
