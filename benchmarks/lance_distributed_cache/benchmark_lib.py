"""Shared, Ray-independent helpers for the Lance distributed-cache benchmark."""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Sequence


MIB = 1024 * 1024
GIB = 1024 * MIB
SCENARIOS = ("no-index-cache", "moka", "distributed")


@dataclass(frozen=True)
class DatasetSpec:
    scale: int
    dimension: int
    num_partitions: int
    num_bits: int
    seed: int = 42
    index_name: str = "vector_idx"

    def validate(self) -> None:
        if self.scale <= 0:
            raise ValueError("scale must be positive")
        if self.dimension <= 0 or self.dimension % 8:
            raise ValueError("IVF_RQ dimension must be positive and divisible by 8")
        if self.num_partitions <= 0:
            raise ValueError("num_partitions must be positive")
        if not 1 <= self.num_bits <= 8:
            raise ValueError("IVF_RQ num_bits must be in [1, 8]")
        if not self.index_name:
            raise ValueError("index_name must not be empty")

    def content_hash(self) -> str:
        fields = (
            self.scale,
            self.dimension,
            self.num_partitions,
            self.num_bits,
            self.seed,
            self.index_name,
        )
        return hashlib.sha256("|".join(map(str, fields)).encode()).hexdigest()[:12]

    def minio_uri(self, bucket: str) -> str:
        return f"s3://{bucket}/ivf_rq_{self.content_hash()}/"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SessionSpec:
    scenario: str
    metadata_capacity_bytes: int
    partition_capacity_bytes: int | None
    moka_capacity_bytes: int
    l2_dir: str | None = None

    def validate(self) -> None:
        if self.scenario not in SCENARIOS:
            raise ValueError(
                f"unknown scenario {self.scenario!r}; expected one of {SCENARIOS}"
            )
        if self.metadata_capacity_bytes <= 0:
            raise ValueError("metadata_capacity_bytes must be positive")
        if self.partition_capacity_bytes is not None:
            if self.partition_capacity_bytes <= 0:
                raise ValueError(
                    "partition_capacity_bytes must be positive or None to disable L1"
                )
        if self.moka_capacity_bytes <= 0:
            raise ValueError("moka_capacity_bytes must be positive")
        if self.scenario == "distributed":
            if not self.l2_dir:
                raise ValueError("distributed scenario requires l2_dir")
            if not Path(self.l2_dir).is_absolute():
                raise ValueError("distributed l2_dir must be absolute")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def minio_storage_options(endpoint_url: str | None) -> Dict[str, str]:
    if endpoint_url is None:
        return {}
    return {
        "aws_endpoint": endpoint_url,
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
        "allow_http": "true" if endpoint_url.startswith("http://") else "false",
    }


def build_session(spec: SessionSpec, lance_module=None):
    """Construct the requested Lance session using the Lance 12 API names."""
    spec.validate()
    if lance_module is None:
        import lance as lance_module  # noqa: PLC0415

    if spec.scenario == "no-index-cache":
        return lance_module.Session(
            index_cache_size_bytes=0,
            metadata_cache_size_bytes=spec.metadata_capacity_bytes,
        )
    if spec.scenario == "moka":
        return lance_module.Session(
            index_cache_size_bytes=spec.moka_capacity_bytes,
            metadata_cache_size_bytes=spec.metadata_capacity_bytes,
        )
    return lance_module.Session.with_distributed_cache(
        l2_dir=spec.l2_dir,
        index_metadata_l1_capacity_bytes=spec.metadata_capacity_bytes,
        l1_partition_capacity_bytes=spec.partition_capacity_bytes,
    )


def _make_batch(num_rows: int, dimension: int, seed: int):
    import numpy as np  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415

    rng = np.random.default_rng(seed)
    flat = rng.standard_normal(num_rows * dimension, dtype=np.float32)
    vectors = pa.FixedSizeListArray.from_arrays(pa.array(flat), dimension)
    return pa.record_batch([vectors], names=["vector"])


def _batch_generator(spec: DatasetSpec, batch_rows: int) -> Generator[Any, None, None]:
    remaining = spec.scale
    batch_index = 0
    while remaining:
        rows = min(batch_rows, remaining)
        yield _make_batch(rows, spec.dimension, spec.seed + batch_index)
        remaining -= rows
        batch_index += 1


def _has_named_index(dataset, index_name: str) -> bool:
    return any(
        getattr(description, "name", None) == index_name
        for description in dataset.describe_indices()
    )


def _validate_dataset_shape(dataset, spec: DatasetSpec, pa_module) -> None:
    row_count = int(dataset.count_rows())
    if row_count != spec.scale:
        raise RuntimeError(
            f"existing dataset has {row_count} rows, expected {spec.scale}; "
            "use a matching --dataset-uri or --skip-setup intentionally"
        )
    try:
        vector_type = dataset.schema.field("vector").type
    except KeyError as error:
        raise RuntimeError("existing dataset has no 'vector' column") from error
    if not pa_module.types.is_fixed_size_list(vector_type):
        raise RuntimeError(
            f"existing vector column has type {vector_type}, expected a fixed-size list"
        )
    if vector_type.list_size != spec.dimension:
        raise RuntimeError(
            f"existing vector dimension is {vector_type.list_size}, "
            f"expected {spec.dimension}"
        )
    for description in dataset.describe_indices():
        if getattr(description, "name", None) != spec.index_name:
            continue
        index_type = str(getattr(description, "index_type", ""))
        if index_type.upper() != "IVF_RQ":
            raise RuntimeError(
                f"existing index {spec.index_name!r} has type {index_type!r}, "
                "expected 'IVF_RQ'"
            )


def ensure_dataset(
    spec: DatasetSpec,
    uri: str,
    *,
    storage_options: Dict[str, str],
    batch_rows: int = 200_000,
    log=print,
) -> str:
    """Create the synthetic dataset and IVF_RQ index unless already present."""
    import lance  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415

    spec.validate()
    try:
        dataset = lance.dataset(uri, storage_options=storage_options)
    except Exception as error:  # Lance uses several storage-specific error types.
        dataset = None
        log(f"[setup] dataset not reusable at {uri}: {type(error).__name__}")

    if dataset is not None:
        _validate_dataset_shape(dataset, spec, pa)

    if dataset is None:
        log(
            f"[setup] writing {spec.scale:,} vectors × {spec.dimension} dimensions "
            f"to {uri}"
        )
        started = time.perf_counter()
        sample = _make_batch(1, spec.dimension, spec.seed)
        reader = pa.RecordBatchReader.from_batches(
            sample.schema, _batch_generator(spec, batch_rows)
        )
        dataset = lance.write_dataset(
            reader,
            uri,
            storage_options=storage_options,
            max_rows_per_file=1_000_000,
        )
        log(f"[setup] dataset write: {time.perf_counter() - started:.2f}s")

    if not _has_named_index(dataset, spec.index_name):
        log(
            f"[setup] building {spec.index_name!r}: IVF_RQ, "
            f"partitions={spec.num_partitions}, bits={spec.num_bits}"
        )
        started = time.perf_counter()
        dataset.create_index(
            "vector",
            index_type="IVF_RQ",
            name=spec.index_name,
            metric_type="L2",
            num_partitions=spec.num_partitions,
            num_bits=spec.num_bits,
        )
        log(f"[setup] index build: {time.perf_counter() - started:.2f}s")
    else:
        log(f"[setup] reusing dataset and index {spec.index_name!r} at {uri}")
    return uri


def make_query_vectors(count: int, dimension: int, seed: int):
    import numpy as np  # noqa: PLC0415

    if count < 0:
        raise ValueError("query count must not be negative")
    return np.random.default_rng(seed).standard_normal(
        (count, dimension), dtype=np.float32
    )


def parse_int_list(value: str) -> list[int]:
    values = [int(item.strip()) for item in value.split(",") if item.strip()]
    if not values or any(item <= 0 for item in values):
        raise ValueError("expected a comma-separated list of positive integers")
    return values


def parse_partition_selection(value: str) -> list[int]:
    """Parse ``all`` or comma-separated IDs; Lance spells all as ``[]``."""
    if value.strip().lower() == "all":
        return []
    values = list(dict.fromkeys(int(item.strip()) for item in value.split(",")))
    if not values or any(item < 0 for item in values):
        raise ValueError("expected 'all' or non-negative comma-separated IDs")
    return values


def percentiles(latencies_s: Sequence[float]) -> Dict[str, float | int]:
    import numpy as np  # noqa: PLC0415

    if not latencies_s:
        return {"p50_s": 0.0, "p95_s": 0.0, "p99_s": 0.0, "mean_s": 0.0, "n": 0}
    values = np.asarray(latencies_s, dtype=np.float64)
    return {
        "p50_s": float(np.percentile(values, 50)),
        "p95_s": float(np.percentile(values, 95)),
        "p99_s": float(np.percentile(values, 99)),
        "mean_s": float(values.mean()),
        "n": int(values.size),
    }


def resolve_index_addresses(dataset, index_name: str) -> list[str]:
    """Return every physical segment UUID for exact cache invalidation."""
    for description in dataset.describe_indices():
        if getattr(description, "name", None) != index_name:
            continue
        addresses = [
            str(segment.uuid)
            for segment in (getattr(description, "segments", None) or [])
            if getattr(segment, "uuid", None)
        ]
        if not addresses:
            raise RuntimeError(
                f"index {index_name!r} has no segment UUIDs; cannot invalidate safely"
            )
        return addresses
    raise RuntimeError(f"index {index_name!r} was not found")


def aggregate_phase(
    *,
    scenario: str,
    phase: str,
    actor_results: Sequence[Dict[str, Any]],
    k_values: Iterable[int],
    wall_time_s: float,
) -> list[Dict[str, Any]]:
    rows = []
    for k in k_values:
        latencies = [
            latency
            for result in actor_results
            for latency in result["latencies_by_k"].get(str(k), [])
        ]
        query_count = len(latencies)
        rows.append(
            {
                "scenario": scenario,
                "phase": phase,
                "k": int(k),
                **percentiles(latencies),
                "wall_time_s": float(wall_time_s),
                "throughput_qps": query_count / wall_time_s if wall_time_s else 0.0,
                "session_size_bytes": sum(
                    int(result.get("session_size_bytes", 0)) for result in actor_results
                ),
                "l2_cache_entry_count": sum(
                    int((result.get("l2_snapshot") or {}).get("cache_entry_count", 0))
                    for result in actor_results
                ),
                "l2_apparent_bytes": sum(
                    int((result.get("l2_snapshot") or {}).get("apparent_bytes", 0))
                    for result in actor_results
                ),
            }
        )
    return rows
