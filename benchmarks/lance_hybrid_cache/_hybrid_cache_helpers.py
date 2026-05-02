# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""Shared helpers for the hybrid-cache IVF_RQ benchmark.

Split out from the Ray driver so dataset generation, query utilities,
and percentile math can be unit-tested without a running Ray cluster.
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional

import numpy as np
import pyarrow as pa

import lance


MIB = 1024 * 1024
GIB = 1024 * MIB


@dataclass(frozen=True)
class DatasetSpec:
    """Parameters that identify a benchmark dataset + index uniquely."""

    scale: int
    dim: int
    num_partitions: int
    num_bits: int
    seed: int = 42

    def content_hash(self) -> str:
        raw = f"{self.scale}|{self.dim}|{self.num_partitions}|{self.num_bits}|{self.seed}"
        return hashlib.sha1(raw.encode()).hexdigest()[:12]

    def uri(self, bucket: str) -> str:
        return f"s3://{bucket}/ivf_rq_{self.content_hash()}/"


def minio_storage_options(endpoint_url: str) -> Dict[str, str]:
    """Storage options for `lance.dataset(..., storage_options=...)` against MinIO."""
    return {
        "aws_endpoint": endpoint_url,
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "aws_secret_access_key": os.environ.get(
            "AWS_SECRET_ACCESS_KEY", "minioadmin"
        ),
        "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
        "allow_http": "true",
    }


def _make_batch(num_rows: int, dim: int, seed: int) -> pa.RecordBatch:
    rng = np.random.default_rng(seed)
    flat = rng.standard_normal(num_rows * dim, dtype=np.float32)
    values = pa.array(flat, type=pa.float32())
    vectors = pa.FixedSizeListArray.from_arrays(values, dim)
    schema = pa.schema([pa.field("vector", vectors.type)])
    return pa.record_batch([vectors], schema=schema)


def _batch_generator(
    spec: DatasetSpec, batch_rows: int
) -> Generator[pa.RecordBatch, None, None]:
    remaining = spec.scale
    batch_idx = 0
    while remaining > 0:
        n = min(batch_rows, remaining)
        yield _make_batch(n, spec.dim, seed=spec.seed + batch_idx)
        remaining -= n
        batch_idx += 1


def _has_vector_index(ds: "lance.LanceDataset") -> bool:
    try:
        descs = ds.describe_indices()
    except Exception:
        return False
    for d in descs:
        idx_type = getattr(d, "index_type", "")
        if "ivf" in str(idx_type).lower():
            return True
    # Any index at all is fine — the bench is vector-only.
    return bool(descs)


def ensure_dataset(
    spec: DatasetSpec,
    bucket: str,
    endpoint_url: str,
    batch_rows: int = 200_000,
    force: bool = False,
    log=print,
) -> str:
    """Create the MinIO-hosted Lance dataset + IVF_RQ index if absent.

    Idempotent: a prior successful run is detected by opening the URI
    and checking for a vector index. Returns the canonical `s3://` URI.
    """
    uri = spec.uri(bucket)
    storage_options = minio_storage_options(endpoint_url)

    existing: Optional["lance.LanceDataset"] = None
    if not force:
        try:
            existing = lance.dataset(uri, storage_options=storage_options)
        except Exception as e:
            log(f"[setup] no existing dataset at {uri} ({e.__class__.__name__})")

    if existing is not None and _has_vector_index(existing):
        log(f"[setup] reusing existing dataset+index at {uri}")
        return uri

    if existing is None:
        log(
            f"[setup] writing {spec.scale:,} × {spec.dim}-d vectors to {uri} "
            f"(batch_rows={batch_rows:,})"
        )
        t0 = time.time()
        # Extract the schema from a throwaway sample to ensure the reader's
        # advertised schema matches the batches byte-for-byte. Mixing
        # `pa.list_(f32, dim)` with `FixedSizeListArray.from_arrays` can
        # produce types with different inner field names and trip the
        # RecordBatchReader schema check.
        sample = _make_batch(1, spec.dim, seed=spec.seed)
        schema = sample.schema
        reader = pa.RecordBatchReader.from_batches(
            schema, _batch_generator(spec, batch_rows)
        )
        lance.write_dataset(
            reader,
            uri,
            storage_options=storage_options,
            max_rows_per_file=1_000_000,
        )
        log(f"[setup] wrote dataset in {time.time() - t0:.1f}s")
        existing = lance.dataset(uri, storage_options=storage_options)
    else:
        log(f"[setup] dataset present but no vector index at {uri}; building index only")

    log(
        f"[setup] building IVF_RQ index "
        f"(num_partitions={spec.num_partitions}, num_bits={spec.num_bits}); "
        f"this is the expensive one-time step"
    )
    t0 = time.time()
    existing.create_index(
        column="vector",
        index_type="IVF_RQ",
        metric_type="L2",
        num_partitions=spec.num_partitions,
        num_bits=spec.num_bits,
    )
    log(f"[setup] built index in {time.time() - t0:.1f}s")
    return uri


def make_query_vectors(num_queries: int, dim: int, seed: int) -> np.ndarray:
    """Deterministic query vectors — identical across scenarios for fair comparison."""
    rng = np.random.default_rng(seed)
    return rng.standard_normal((num_queries, dim), dtype=np.float32)


def run_query(
    ds: "lance.LanceDataset",
    q: np.ndarray,
    k: int,
    nprobes: int,
) -> float:
    """Issue one nearest-neighbors query and return wall latency (seconds)."""
    t0 = time.perf_counter()
    _ = ds.to_table(
        columns=[],
        with_row_id=True,
        nearest={
            "column": "vector",
            "q": q,
            "k": k,
            "nprobes": nprobes,
        },
    )
    return time.perf_counter() - t0


def percentiles(latencies: List[float]) -> Dict[str, float]:
    """p50/p95/p99/mean/n over a list of seconds."""
    if not latencies:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "n": 0}
    arr = np.asarray(latencies, dtype=np.float64)
    return {
        "p50": float(np.percentile(arr, 50)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
        "mean": float(arr.mean()),
        "n": int(arr.size),
    }


def format_latency_row(scenario: str, k: int, pct: Dict[str, float]) -> str:
    def ms(s: float) -> str:
        return f"{s * 1000:8.2f}"

    return (
        f"  {scenario:<10} k={k:<5} "
        f"p50={ms(pct['p50'])} ms  "
        f"p95={ms(pct['p95'])} ms  "
        f"p99={ms(pct['p99'])} ms  "
        f"mean={ms(pct['mean'])} ms  "
        f"n={pct['n']}"
    )


@dataclass
class ScenarioResult:
    name: str
    stats_pre: Dict[str, int]
    stats_post: Dict[str, int]
    latencies_by_k: Dict[int, List[float]]
    duration_s: float

    def summary_rows(self) -> List[Dict[str, Any]]:
        rows = []
        for k, lats in self.latencies_by_k.items():
            pct = percentiles(lats)
            rows.append(
                {
                    "scenario": self.name,
                    "k": k,
                    **pct,
                    "hits": self.stats_post["hits"],
                    "misses": self.stats_post["misses"],
                    "hit_ratio": (
                        self.stats_post["hits"]
                        / max(1, self.stats_post["hits"] + self.stats_post["misses"])
                    ),
                    "num_entries": self.stats_post["num_entries"],
                    "cache_size_bytes": self.stats_post["size_bytes"],
                }
            )
        return rows


def build_session(spec: Dict[str, Any]):
    """Construct a lance.Session based on a scenario spec dict.

    Expected spec shapes:
      {"kind": "no-cache"}
      {"kind": "moka", "index_cache_size_bytes": int}
      {"kind": "hybrid",
       "l1_capacity_bytes": int,
       "l2_dir": str,
       "l2_capacity_bytes": int,
       # Optional: when present, dispatch to with_hybrid_cache_advanced
       # so foyer L1 / codec-less Moka are sized independently. In that
       # case `l1_capacity_bytes` is the foyer DRAM tier specifically
       # (not a combined budget split internally).
       "codecless_capacity_bytes": int}
    """
    kind = spec["kind"]
    if kind == "no-cache":
        return lance.Session(index_cache_size_bytes=0)
    if kind == "moka":
        return lance.Session(index_cache_size_bytes=int(spec["index_cache_size_bytes"]))
    if kind == "hybrid":
        if "codecless_capacity_bytes" in spec:
            return lance.Session.with_hybrid_cache_advanced(
                foyer_l1_capacity_bytes=int(spec["l1_capacity_bytes"]),
                codecless_capacity_bytes=int(spec["codecless_capacity_bytes"]),
                l2_dir=str(spec["l2_dir"]),
                l2_capacity_bytes=int(spec["l2_capacity_bytes"]),
            )
        return lance.Session.with_hybrid_cache(
            l1_capacity_bytes=int(spec["l1_capacity_bytes"]),
            l2_dir=str(spec["l2_dir"]),
            l2_capacity_bytes=int(spec["l2_capacity_bytes"]),
        )
    raise ValueError(f"unknown scenario kind: {kind!r}")


def warmup(
    ds: "lance.LanceDataset",
    queries: np.ndarray,
    nprobes: int,
    k: int = 10,
) -> None:
    """Populate the cache by running queries at the bench's real nprobes."""
    for q in queries:
        run_query(ds, q, k=k, nprobes=nprobes)


def measure(
    ds: "lance.LanceDataset",
    queries: np.ndarray,
    k_list: List[int],
    nprobes: int,
) -> Dict[int, List[float]]:
    """Run `measure_queries` × `len(k_list)` queries, return per-k latency lists."""
    results: Dict[int, List[float]] = {k: [] for k in k_list}
    for q in queries:
        for k in k_list:
            lat = run_query(ds, q, k=k, nprobes=nprobes)
            results[k].append(lat)
    return results
