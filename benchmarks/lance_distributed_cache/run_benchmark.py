#!/usr/bin/env python3
"""Benchmark Lance 12's per-worker memory + local-file cache with Ray actors."""

from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import inspect
import json
import os
import re
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Sequence

import numpy as np
from benchmark_lib import (
    MIB,
    SCENARIOS,
    DatasetSpec,
    SessionSpec,
    aggregate_phase,
    build_session,
    ensure_dataset,
    make_query_vectors,
    minio_storage_options,
    parse_int_list,
    parse_partition_selection,
    resolve_index_addresses,
    validate_result_digests,
)
from cache_inspect import (
    diff_snapshots,
    format_bytes,
    format_snapshot,
    invalidation_errors,
    snapshot_l2_dir,
)

HERE = Path(__file__).resolve().parent
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
TARGET_LANCE_COMMITS = {
    "implementation": "bf6fa8d757d9525dbaa9b9b490417084c60288cf",
    "usage_docs": "e5375e298",
    "design_docs": "ee7c7c6c2",
}


def _compact_snapshot(snapshot: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if snapshot is None:
        return None
    compact = dict(snapshot)
    compact.pop("files", None)
    for key in ("namespace_dirs", "deleting_dirs"):
        compact[key] = [
            {name: value for name, value in entry.items() if name not in {"files"}}
            for entry in compact.get(key, [])
        ]
    return compact


class CacheWorker:
    """One long-lived Lance session hosted in a Ray actor process."""

    def __init__(
        self,
        actor_id: int,
        session_spec: Dict[str, Any],
        dataset_uri: str,
        storage_options: Dict[str, str],
        nprobes: int,
    ):
        import lance

        import ray

        self.actor_id = int(actor_id)
        self.session_spec = SessionSpec(**session_spec)
        self.dataset_uri = dataset_uri
        self.storage_options = storage_options
        self.nprobes = int(nprobes)
        self.l2_dir = self.session_spec.l2_dir
        if self.l2_dir is not None:
            Path(self.l2_dir).mkdir(parents=True, exist_ok=True)
        self.session = build_session(self.session_spec, lance)
        self.dataset = lance.dataset(
            dataset_uri,
            session=self.session,
            storage_options=storage_options,
        )
        self.lance_version = lance.__version__
        self.node_id = ray.get_runtime_context().get_node_id()

    def _snapshot(self) -> Dict[str, Any] | None:
        if self.l2_dir is None:
            return None
        return _compact_snapshot(snapshot_l2_dir(self.l2_dir))

    @staticmethod
    def _disk_read_bytes() -> int | None:
        try:
            fields = dict(
                line.split(":", 1)
                for line in Path("/proc/self/io").read_text().splitlines()
            )
        except OSError:
            return None
        return int(fields["read_bytes"])

    def info(self, index_name: str) -> Dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "node_id": self.node_id,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "lance_version": self.lance_version,
            "thread_environment": {
                key: os.environ.get(key)
                for key in (
                    "LANCE_CPU_THREADS",
                    "LANCE_IO_THREADS",
                    "OMP_NUM_THREADS",
                    "OPENBLAS_NUM_THREADS",
                    "MKL_NUM_THREADS",
                )
            },
            "index_addresses": resolve_index_addresses(self.dataset, index_name),
            "session_size_bytes": int(self.session.size_bytes()),
            "l2_snapshot": self._snapshot(),
        }

    def _run_query(self, query: np.ndarray, k: int):
        return self.dataset.to_table(
            columns=["_distance"],
            with_row_id=True,
            nearest={
                "column": "vector",
                "q": np.asarray(query, dtype=np.float32),
                "k": int(k),
                "nprobes": self.nprobes,
            },
        )

    def prewarm(
        self,
        mode: str,
        index_name: str,
        partition_ids: list[int],
        warmup_queries: np.ndarray,
        warmup_k: int,
    ) -> Dict[str, Any]:
        before = self._snapshot()
        io_before = self.dataset.io_stats_snapshot()
        size_before = int(self.session.size_bytes())
        started = time.perf_counter()
        if mode == "strict":
            self.dataset.prewarm_index(index_name, partition_ids=partition_ids)
        elif mode == "best-effort":
            self.dataset.prewarm_index(index_name)
        elif mode == "natural":
            for query in warmup_queries:
                self._run_query(query, warmup_k)
        elif mode != "none":
            raise ValueError(f"unknown prewarm mode: {mode!r}")
        duration_s = time.perf_counter() - started
        io_after = self.dataset.io_stats_snapshot()
        index_cache_weight_bytes = int(self.session.index_cache_size_bytes())
        after = self._snapshot()
        return {
            "event": "prewarm",
            "actor_id": self.actor_id,
            "mode": mode,
            "partition_selection": "all" if not partition_ids else partition_ids,
            "duration_s": duration_s,
            "source_read_bytes": int(io_after.read_bytes - io_before.read_bytes),
            "source_read_iops": int(io_after.read_iops - io_before.read_iops),
            "session_size_bytes_before": size_before,
            "session_size_bytes_after": int(self.session.size_bytes()),
            "index_cache_weight_bytes": index_cache_weight_bytes,
            "l2_before": before,
            "l2_after": after,
            "l2_delta": diff_snapshots(before, after) if before and after else None,
        }

    def measure(
        self,
        phase: str,
        queries: np.ndarray,
        k_values: list[int],
    ) -> Dict[str, Any]:
        latencies: Dict[str, list[float]] = {str(k): [] for k in k_values}
        returned_rows: Dict[str, int] = {str(k): 0 for k in k_values}
        row_ids_digests = {str(k): hashlib.sha256() for k in k_values}
        io_before = self.dataset.io_stats_snapshot()
        disk_before = self._disk_read_bytes()
        started = time.perf_counter()
        for query in queries:
            for k in k_values:
                query_started = time.perf_counter()
                table = self._run_query(query, k)
                latencies[str(k)].append(time.perf_counter() - query_started)
                if table.num_rows != k:
                    raise RuntimeError(
                        f"actor={self.actor_id}, k={k}: returned {table.num_rows} rows"
                    )
                distances = table["_distance"].to_numpy()
                if not np.all(np.isfinite(distances)):
                    raise RuntimeError(
                        f"actor={self.actor_id}, k={k}: non-finite distance"
                    )
                # Keep validation outside each query's latency measurement.
                row_ids = table["_rowid"].to_numpy().astype("<u8", copy=False)
                row_ids_digests[str(k)].update(row_ids.tobytes())
                returned_rows[str(k)] += table.num_rows
        duration_s = time.perf_counter() - started
        io_after = self.dataset.io_stats_snapshot()
        disk_after = self._disk_read_bytes()
        return {
            "event": "measure",
            "actor_id": self.actor_id,
            "phase": phase,
            "latencies_by_k": latencies,
            "returned_rows_by_k": returned_rows,
            "row_ids_sha256_by_k": {
                k: digest.hexdigest() for k, digest in row_ids_digests.items()
            },
            "duration_s": duration_s,
            "source_read_bytes": int(io_after.read_bytes - io_before.read_bytes),
            "source_read_iops": int(io_after.read_iops - io_before.read_iops),
            "process_disk_read_bytes": (
                disk_after - disk_before
                if disk_after is not None and disk_before is not None
                else None
            ),
        }

    def measurement_stats(self) -> Dict[str, Any]:
        # Moka maintenance and directory scans can dominate a short cached pass.
        # Collect them in a separate RPC after the driver stops its timer.
        index_cache_weight_bytes = int(self.session.index_cache_size_bytes())
        return {
            "session_size_bytes": int(self.session.size_bytes()),
            "index_cache_weight_bytes": index_cache_weight_bytes,
            "l2_snapshot": self._snapshot(),
        }

    def invalidate(self, index_name: str) -> Dict[str, Any]:
        before = self._snapshot()
        addresses = resolve_index_addresses(self.dataset, index_name)
        started = time.perf_counter()
        for address in addresses:
            self.session.invalidate_index_cache(self.dataset.uri, address)
        duration_s = time.perf_counter() - started
        after = self._snapshot()
        errors = invalidation_errors(after) if after is not None else []
        if errors:
            raise RuntimeError(
                "invalidation left unsafe L2 state: " + "; ".join(errors)
            )
        return {
            "event": "invalidate",
            "actor_id": self.actor_id,
            "index_addresses": addresses,
            "duration_s": duration_s,
            "session_size_bytes_after": int(self.session.size_bytes()),
            "l2_before": before,
            "l2_after": after,
            "l2_delta": diff_snapshots(before, after) if before and after else None,
        }

    def close(self, drop_l2_file_pages: bool = False) -> Dict[str, Any]:
        snapshot = self._snapshot()
        self.dataset = None
        self.session = None
        gc.collect()
        advised_files = 0
        advised_bytes = 0
        if drop_l2_file_pages and self.l2_dir is not None:
            for entry in snapshot_l2_dir(self.l2_dir)["files"]:
                with (Path(self.l2_dir) / entry["name"]).open("rb") as stream:
                    os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                advised_files += 1
                advised_bytes += entry["apparent_bytes"]
        return {
            "actor_id": self.actor_id,
            "l2_snapshot": snapshot,
            "l2_page_cache_advised_files": advised_files,
            "l2_page_cache_advised_bytes": advised_bytes,
        }


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError(f"value must be non-negative, got {parsed}")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"value must be positive, got {parsed}")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=_positive_int, default=10_000_000)
    parser.add_argument("--dim", type=_positive_int, default=1024)
    parser.add_argument("--num-partitions", type=_positive_int, default=3000)
    parser.add_argument("--num-bits", type=_positive_int, default=8)
    parser.add_argument("--index-name", default="vector_idx")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--dataset-uri",
        default=None,
        help="Existing/shared URI. By default a content-addressed MinIO URI is used.",
    )
    parser.add_argument("--bucket", default="lance-bench")
    parser.add_argument("--endpoint-url", default="http://127.0.0.1:9000")
    parser.add_argument("--skip-setup", action="store_true")
    parser.add_argument("--batch-rows", type=_positive_int, default=200_000)

    parser.add_argument(
        "--scenarios",
        default=",".join(SCENARIOS),
        help=f"Comma-separated subset of {SCENARIOS}.",
    )
    parser.add_argument("--num-actors", type=_positive_int, default=4)
    parser.add_argument("--cpus-per-actor", type=float, default=1.0)
    parser.add_argument("--actor-resource", default=None)
    parser.add_argument("--ray-address", default=os.environ.get("RAY_ADDRESS"))
    parser.add_argument("--metadata-l1-mb", type=_positive_int, default=64)
    parser.add_argument(
        "--partition-l1-mb",
        type=_nonnegative_int,
        default=1024,
        help="Decoded-partition L1 capacity; 0 disables this tier.",
    )
    parser.add_argument(
        "--memory-mb",
        type=_positive_int,
        default=None,
        help="Default QuickCache index capacity. Defaults to metadata + partition L1.",
    )
    parser.add_argument("--nvme-dir", default="/mnt/nvme/lance-cache-bench")
    parser.add_argument("--l2-run-id", default=None)

    parser.add_argument(
        "--prewarm",
        choices=("none", "natural", "best-effort", "strict"),
        default="strict",
    )
    parser.add_argument(
        "--prewarm-partitions",
        default="all",
        help="For strict prewarm: 'all' or comma-separated partition IDs.",
    )
    parser.add_argument("--warmup-queries", type=_nonnegative_int, default=256)
    parser.add_argument("--measure-queries", type=_positive_int, default=1000)
    parser.add_argument("--k-list", default="10,100,1000")
    parser.add_argument("--nprobes", type=_positive_int, default=32)
    parser.add_argument(
        "--measure-restart",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Recreate actors on the same nodes and measure the persistent L2.",
    )
    parser.add_argument("--invalidation-drill", action="store_true")
    parser.add_argument(
        "--drop-l2-page-cache-on-restart",
        action="store_true",
        help="Advise eviction of this run's persisted L2 file pages before restart.",
    )
    parser.add_argument("--lance-cpu-threads", type=_positive_int, default=None)
    parser.add_argument("--lance-io-threads", type=_positive_int, default=None)
    parser.add_argument(
        "--lance-build-commit", default=os.environ.get("LANCE_BUILD_COMMIT")
    )
    parser.add_argument("--out-dir", default=str(HERE / "out"))
    return parser.parse_args(argv)


def _scenario_names(value: str) -> list[str]:
    names = list(
        dict.fromkeys(item.strip() for item in value.split(",") if item.strip())
    )
    unknown = sorted(set(names) - set(SCENARIOS))
    if not names or unknown:
        raise ValueError(f"invalid scenarios {unknown or names}; expected {SCENARIOS}")
    return names


def _new_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{timestamp}-{os.getpid()}"


def _validate_args(args: argparse.Namespace) -> tuple[list[str], list[int], list[int]]:
    scenarios = _scenario_names(args.scenarios)
    k_values = parse_int_list(args.k_list)
    partition_ids = parse_partition_selection(args.prewarm_partitions)
    if args.dim % 8:
        raise ValueError("--dim must be divisible by 8 for IVF_RQ")
    if not 1 <= args.num_bits <= 8:
        raise ValueError("--num-bits must be in [1, 8]")
    if args.nprobes > args.num_partitions:
        raise ValueError("--nprobes must not exceed --num-partitions")
    if partition_ids and max(partition_ids) >= args.num_partitions:
        raise ValueError("--prewarm-partitions contains an out-of-range ID")
    if args.cpus_per_actor <= 0:
        raise ValueError("--cpus-per-actor must be positive")
    if args.measure_queries < args.num_actors:
        raise ValueError("--measure-queries must be at least --num-actors")
    if max(k_values) > args.scale:
        raise ValueError("--k-list must not exceed --scale")
    if args.prewarm != "strict" and args.prewarm_partitions != "all":
        raise ValueError(
            "--prewarm-partitions is only meaningful with --prewarm strict"
        )
    if args.invalidation_drill and not args.measure_restart:
        raise ValueError("--invalidation-drill requires --measure-restart")
    if args.drop_l2_page_cache_on_restart:
        if not args.measure_restart:
            raise ValueError(
                "--drop-l2-page-cache-on-restart requires --measure-restart"
            )
        if not hasattr(os, "posix_fadvise"):
            raise ValueError("--drop-l2-page-cache-on-restart requires posix_fadvise")
    if args.l2_run_id is not None and not _RUN_ID_RE.fullmatch(args.l2_run_id):
        raise ValueError(
            "--l2-run-id may contain only letters, digits, '.', '_' and '-'"
        )
    return scenarios, k_values, partition_ids


def _validate_lance_api(lance_module) -> None:
    required_parameters = {
        "Session.with_distributed_cache": (
            lance_module.Session.with_distributed_cache,
            {
                "l2_dir",
                "index_metadata_l1_capacity_bytes",
                "l1_partition_capacity_bytes",
            },
        ),
        "LanceDataset.prewarm_index": (
            lance_module.LanceDataset.prewarm_index,
            {"name", "partition_ids"},
        ),
        "Session.invalidate_index_cache": (
            lance_module.Session.invalidate_index_cache,
            {"dataset_uri", "index_addr"},
        ),
    }
    failures = []
    for label, (callable_object, required) in required_parameters.items():
        parameters = set(inspect.signature(callable_object).parameters)
        missing = sorted(required - parameters)
        if missing:
            failures.append(f"{label} is missing {missing}")
    if failures:
        raise RuntimeError(
            "installed pylance does not expose the Lance 12 distributed-cache "
            "API from bf6fa8d75: " + "; ".join(failures)
        )


def _session_spec(
    args: argparse.Namespace, scenario: str, actor_id: int, run_id: str
) -> SessionSpec:
    metadata_bytes = int(args.metadata_l1_mb) * MIB
    partition_bytes = int(args.partition_l1_mb) * MIB if args.partition_l1_mb else None
    memory_bytes = (
        int(args.memory_mb) * MIB
        if args.memory_mb is not None
        else metadata_bytes + (partition_bytes or 0)
    )
    l2_dir = None
    if scenario == "distributed":
        l2_dir = str(
            Path(args.nvme_dir).expanduser().absolute() / run_id / f"actor-{actor_id}"
        )
    return SessionSpec(
        scenario=scenario,
        metadata_capacity_bytes=metadata_bytes,
        partition_capacity_bytes=partition_bytes,
        memory_capacity_bytes=memory_bytes,
        l2_dir=l2_dir,
    )


def _actor_options(
    args: argparse.Namespace, *, node_id: str | None = None
) -> Dict[str, Any]:
    options: Dict[str, Any] = {"num_cpus": args.cpus_per_actor}
    if args.actor_resource:
        options["resources"] = {args.actor_resource: 1.0}
    if node_id is not None:
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            node_id=node_id, soft=False
        )
    return options


def _spawn_actors(
    *,
    ray_module,
    remote_worker,
    args: argparse.Namespace,
    specs: list[SessionSpec],
    dataset_uri: str,
    storage_options: Dict[str, str],
    node_ids: list[str] | None = None,
):
    actors = []
    for actor_id, spec in enumerate(specs):
        node_id = node_ids[actor_id] if node_ids is not None else None
        actors.append(
            remote_worker.options(**_actor_options(args, node_id=node_id)).remote(
                actor_id,
                spec.to_dict(),
                dataset_uri,
                storage_options,
                args.nprobes,
            )
        )
    return actors


def _close_actors(ray_module, actors, *, drop_l2_file_pages: bool = False):
    results = ray_module.get(
        [actor.close.remote(drop_l2_file_pages) for actor in actors]
    )
    for actor in actors:
        ray_module.kill(actor)
    return results


def _measure_phase(
    *,
    ray_module,
    actors,
    scenario: str,
    phase: str,
    query_chunks: Sequence[np.ndarray],
    k_values: list[int],
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    all_results = []
    all_rows = []
    # Each k needs its own concurrent pass and wall time for meaningful QPS.
    for k in k_values:
        started = time.perf_counter()
        actor_results = ray_module.get(
            [
                actor.measure.remote(phase, query_chunks[index], [k])
                for index, actor in enumerate(actors)
            ]
        )
        wall_time_s = time.perf_counter() - started
        snapshots = ray_module.get(
            [actor.measurement_stats.remote() for actor in actors]
        )
        for result, snapshot in zip(actor_results, snapshots):
            result.update(snapshot)
        all_results.extend(actor_results)
        all_rows.extend(
            aggregate_phase(
                scenario=scenario,
                phase=phase,
                actor_results=actor_results,
                k_values=[k],
                wall_time_s=wall_time_s,
            )
        )
    return all_results, all_rows


def _print_rows(rows: Sequence[Dict[str, Any]]) -> None:
    for row in rows:
        print(
            f"  {row['scenario']:<16} {row['phase']:<20} k={row['k']:<5} "
            f"p50={row['p50_s'] * 1000:8.2f} ms "
            f"p95={row['p95_s'] * 1000:8.2f} ms "
            f"p99={row['p99_s'] * 1000:8.2f} ms "
            f"throughput={row['throughput_qps']:8.2f} q/s "
            f"index_L1_weight={format_bytes(row['index_cache_weight_bytes'])} "
            f"L2={format_bytes(row['l2_apparent_bytes'])} "
            f"source={format_bytes(row['source_read_bytes'])}"
        )


def _write_outputs(
    output_dir: Path,
    config: Dict[str, Any],
    events: Sequence[Dict[str, Any]],
    rows: Sequence[Dict[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "run.json").write_text(json.dumps(config, indent=2, sort_keys=True))
    with (output_dir / "events.jsonl").open("w") as stream:
        for event in events:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
    with (output_dir / "summary.csv").open("w", newline="") as stream:
        fieldnames = list(rows[0]) if rows else []
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        scenarios, k_values, partition_ids = _validate_args(args)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    import ray

    run_id = args.l2_run_id or _new_run_id()
    output_dir = Path(args.out_dir) / run_id
    dataset_spec = DatasetSpec(
        scale=args.scale,
        dimension=args.dim,
        num_partitions=args.num_partitions,
        num_bits=args.num_bits,
        seed=args.seed,
        index_name=args.index_name,
    )
    storage_options = minio_storage_options(args.endpoint_url or None)
    dataset_uri = args.dataset_uri or dataset_spec.minio_uri(args.bucket)

    for key, value in {
        "AWS_ENDPOINT_URL": args.endpoint_url,
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }.items():
        if value:
            os.environ.setdefault(key, value)

    worker_env = {
        "AWS_ENDPOINT_URL": args.endpoint_url,
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
        "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS", "1"),
        "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS", "1"),
        "MKL_NUM_THREADS": os.environ.get("MKL_NUM_THREADS", "1"),
    }
    if args.lance_cpu_threads is not None:
        worker_env["LANCE_CPU_THREADS"] = str(args.lance_cpu_threads)
    if args.lance_io_threads is not None:
        worker_env["LANCE_IO_THREADS"] = str(args.lance_io_threads)
    worker_env = {key: value for key, value in worker_env.items() if value}

    ray.init(
        address=args.ray_address,
        runtime_env={
            "working_dir": str(HERE),
            "env_vars": worker_env,
            "excludes": [".venv/", "out/", "results/", "__pycache__/"],
        },
        ignore_reinit_error=True,
    )

    try:
        # Start Ray before importing pylance in the driver. Lance registers a
        # fork warning because its native runtime is not fork-safe; local Ray
        # startup may fork service processes, while actor workers import Lance
        # in their already-created processes.
        import lance
        import pyarrow

        _validate_lance_api(lance)

        if not args.skip_setup:
            ensure_dataset(
                dataset_spec,
                dataset_uri,
                storage_options=storage_options,
                batch_rows=args.batch_rows,
            )

        warmup_queries = make_query_vectors(
            args.warmup_queries, args.dim, args.seed + 1
        )
        measure_queries = make_query_vectors(
            args.measure_queries, args.dim, args.seed + 2
        )
        warmup_chunks = list(np.array_split(warmup_queries, args.num_actors))
        measure_chunks = list(np.array_split(measure_queries, args.num_actors))

        remote_worker = ray.remote(CacheWorker)
    except BaseException:
        ray.shutdown()
        raise

    events: list[Dict[str, Any]] = []
    summary_rows: list[Dict[str, Any]] = []
    scenario_specs: Dict[str, list[Dict[str, Any]]] = {}
    try:
        for scenario in scenarios:
            print(f"\n=== {scenario} ===")
            specs = [
                _session_spec(args, scenario, actor_id, run_id)
                for actor_id in range(args.num_actors)
            ]
            scenario_specs[scenario] = [spec.to_dict() for spec in specs]
            actors = _spawn_actors(
                ray_module=ray,
                remote_worker=remote_worker,
                args=args,
                specs=specs,
                dataset_uri=dataset_uri,
                storage_options=storage_options,
            )
            info = ray.get([actor.info.remote(args.index_name) for actor in actors])
            events.extend(
                {"scenario": scenario, "event": "actor_info", **item} for item in info
            )
            node_ids = [item["node_id"] for item in info]

            prewarm_started = time.perf_counter()
            prewarm_results = ray.get(
                [
                    actor.prewarm.remote(
                        args.prewarm,
                        args.index_name,
                        partition_ids,
                        warmup_chunks[index],
                        k_values[0],
                    )
                    for index, actor in enumerate(actors)
                ]
            )
            prewarm_wall_s = time.perf_counter() - prewarm_started
            events.append(
                {
                    "scenario": scenario,
                    "event": "prewarm_summary",
                    "mode": args.prewarm,
                    "wall_time_s": prewarm_wall_s,
                    "max_actor_time_s": max(
                        float(result["duration_s"]) for result in prewarm_results
                    ),
                }
            )
            print(f"  aggregate prewarm wall={prewarm_wall_s:.2f}s")
            for result in prewarm_results:
                result["scenario"] = scenario
                events.append(result)
                if result.get("l2_after"):
                    print(
                        f"  actor-{result['actor_id']} prewarm={result['duration_s']:.2f}s "
                        + format_snapshot(result["l2_after"])
                    )
                else:
                    print(
                        f"  actor-{result['actor_id']} prewarm={result['duration_s']:.2f}s "
                        f"L1={format_bytes(result['session_size_bytes_after'])}"
                    )

            for phase in ("initial", "l1_warm"):
                actor_results, rows = _measure_phase(
                    ray_module=ray,
                    actors=actors,
                    scenario=scenario,
                    phase=phase,
                    query_chunks=measure_chunks,
                    k_values=k_values,
                )
                events.extend({"scenario": scenario, **item} for item in actor_results)
                summary_rows.extend(rows)
                _print_rows(rows)

            if not args.measure_restart:
                _close_actors(ray, actors)
                continue

            close_results = _close_actors(
                ray,
                actors,
                drop_l2_file_pages=args.drop_l2_page_cache_on_restart,
            )
            events.extend(
                {"scenario": scenario, "event": "actor_close_before_restart", **item}
                for item in close_results
            )
            actors = _spawn_actors(
                ray_module=ray,
                remote_worker=remote_worker,
                args=args,
                specs=specs,
                dataset_uri=dataset_uri,
                storage_options=storage_options,
                node_ids=node_ids,
            )
            restarted_info = ray.get(
                [actor.info.remote(args.index_name) for actor in actors]
            )
            events.extend(
                {"scenario": scenario, "event": "restart_actor_info", **item}
                for item in restarted_info
            )
            actor_results, rows = _measure_phase(
                ray_module=ray,
                actors=actors,
                scenario=scenario,
                phase="process_restart",
                query_chunks=measure_chunks,
                k_values=k_values,
            )
            events.extend({"scenario": scenario, **item} for item in actor_results)
            summary_rows.extend(rows)
            _print_rows(rows)

            if args.invalidation_drill and scenario == "distributed":
                invalidation_started = time.perf_counter()
                invalidations = ray.get(
                    [actor.invalidate.remote(args.index_name) for actor in actors]
                )
                invalidation_wall_s = time.perf_counter() - invalidation_started
                events.append(
                    {
                        "scenario": scenario,
                        "event": "invalidation_summary",
                        "wall_time_s": invalidation_wall_s,
                        "max_actor_time_s": max(
                            float(result["duration_s"]) for result in invalidations
                        ),
                    }
                )
                print(f"  aggregate invalidation wall={invalidation_wall_s:.3f}s")
                for result in invalidations:
                    result["scenario"] = scenario
                    events.append(result)
                    print(
                        f"  actor-{result['actor_id']} invalidated "
                        f"{len(result['index_addresses'])} segment(s) in "
                        f"{result['duration_s']:.3f}s"
                    )
                rewarm_started = time.perf_counter()
                rewarm = ray.get(
                    [
                        actor.prewarm.remote(
                            "strict",
                            args.index_name,
                            partition_ids,
                            np.empty((0, args.dim), dtype=np.float32),
                            k_values[0],
                        )
                        for actor in actors
                    ]
                )
                rewarm_wall_s = time.perf_counter() - rewarm_started
                events.append(
                    {
                        "scenario": scenario,
                        "event": "rehydrate_summary",
                        "wall_time_s": rewarm_wall_s,
                        "max_actor_time_s": max(
                            float(result["duration_s"]) for result in rewarm
                        ),
                    }
                )
                print(f"  aggregate rehydrate wall={rewarm_wall_s:.2f}s")
                for result in rewarm:
                    result["scenario"] = scenario
                    result["event"] = "rehydrate"
                    events.append(result)
                actor_results, rows = _measure_phase(
                    ray_module=ray,
                    actors=actors,
                    scenario=scenario,
                    phase="post_invalidation_rehydrate",
                    query_chunks=measure_chunks,
                    k_values=k_values,
                )
                events.extend({"scenario": scenario, **item} for item in actor_results)
                summary_rows.extend(rows)
                _print_rows(rows)

            _close_actors(ray, actors)
    finally:
        ray.shutdown()

    config = {
        "run_id": run_id,
        "dataset_uri": dataset_uri,
        "dataset": dataset_spec.to_dict(),
        "arguments": vars(args),
        "session_specs": scenario_specs,
        "versions": {
            "ray": ray.__version__,
            "lance": lance.__version__,
            "numpy": np.__version__,
            "pyarrow": pyarrow.__version__,
            "python": sys.version,
        },
        "worker_environment": {
            key: value
            for key, value in worker_env.items()
            if not key.startswith("AWS_")
        },
        "lance_target_commits": TARGET_LANCE_COMMITS,
        "lance_build_commit": args.lance_build_commit,
        "lance_api": {
            "Session.with_distributed_cache": str(
                inspect.signature(lance.Session.with_distributed_cache)
            ),
            "LanceDataset.prewarm_index": str(
                inspect.signature(lance.LanceDataset.prewarm_index)
            ),
            "Session.invalidate_index_cache": str(
                inspect.signature(lance.Session.invalidate_index_cache)
            ),
        },
    }
    _write_outputs(output_dir, config, events, summary_rows)
    validate_result_digests(events)
    print("Query row IDs match across all scenarios and phases.")
    print(f"\nWrote {output_dir / 'run.json'}")
    print(f"Wrote {output_dir / 'events.jsonl'}")
    print(f"Wrote {output_dir / 'summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
