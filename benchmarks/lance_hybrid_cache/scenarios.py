"""Scenario specs for the Lance distributed-cache benchmark (Lance 6.0).

Byte values are tuned for a 12 CPU / 32 GB host and a ~10 GB IVF_RQ index
(10M * 1024-d, num_bits=8, 3000 partitions). With a 4 GiB DRAM cap, moka
thrashes and the v6 distributed-cache NVMe tier carries the hot working
set.

The v4 `hybrid` / `hybrid_advanced` scenarios are replaced by a single
`distributed` scenario that targets `Session.with_distributed_cache`.
The `no-cache` and `moka` scenarios are preserved; they map to the
plain `Session(index_cache_size_bytes=...)` constructor that v6 still
exposes.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

MIB = 1024 * 1024
GIB = 1024 * MIB

# v6 metadata L1 holds IvfIndexState, IndexMetadata, FragReuseIndex,
# ScalarIndexDetails, etc. Sizing it below this floor defeats the
# per-query routing path; warn rather than reject so a deliberately
# small experiment is still possible.
METADATA_L1_FLOOR_BYTES = 4 * MIB

UNSAFE_L2_DIR_PREFIXES = (
    Path("/tmp"),
    Path("/var/tmp"),
    Path("/dev/shm"),
)

_KNOWN_SCENARIOS = frozenset({"no-cache", "moka", "distributed"})


def distributed_l2_dir_for_repeat(
    base_nvme_dir: str,
    actor_id: int,
    repeat: int,
    reuse_l2: bool,
    now_fn=time.time,
) -> str:
    """Per-(actor, repeat) L2 path for the single-actor driver.

    With `--reuse-l2`, all repeats share `<nvme-dir>/actor-<i>/` so the
    next repeat warms from the prior repeat's L2 contents — useful for
    isolating steady-state behavior. Without `--reuse-l2`, each repeat
    gets a fresh timestamped subdirectory so cold-start latency is
    measured honestly (the default).

    `now_fn` is overridable for deterministic tests.
    """
    if reuse_l2:
        return per_actor_l2_dir(base_nvme_dir, actor_id)
    suffix = f"actor-{actor_id}-r{repeat}-{int(now_fn())}"
    return os.path.join(base_nvme_dir, suffix)


def is_eligible_for_residency_probe(scenario: str, prewarm: str) -> bool:
    """Whether the v6 aggregate-only residency probe should run.

    The probe needs an L2 directory to walk *and* a defined per-actor
    expected partition set:

    * ``no-cache``: nothing to probe (no cache tier).
    * ``forced`` / ``sharded`` prewarm: per-actor expected set is
      well-defined (full range / round-robin slice respectively).
    * ``natural`` / ``none`` prewarm: the expected set is undefined;
      the probe has no reference to report ``missing`` against.

    Notably this returns True for ``distributed`` -- the scenario the
    v6 probe was designed for -- under either forced or sharded
    prewarm.
    """
    return prewarm in ("forced", "sharded") and scenario != "no-cache"


def per_actor_l2_dir(base_nvme_dir: str, actor_id: int) -> str:
    """Per-actor L2 subdirectory under the operator-supplied `--nvme-dir`.

    `Session.with_distributed_cache` takes an exclusive advisory lock on
    `{l2_dir}/lance-distributed.lock` for the lifetime of the session, so
    the directory must be exclusive to one process. The driver builds one
    spec per actor with this layout (`<nvme-dir>/actor-<i>/`); the actor
    itself creates the directory in-process just before constructing the
    session. Driver-side mkdir would only touch the head-node filesystem
    in a multi-node cluster, missing the worker's actual NVMe.
    """
    return os.path.join(base_nvme_dir, f"actor-{actor_id}")


def build_scenario_spec(
    scenario: str,
    actor_id: int,
    *,
    dram_bytes: int = 0,
    nvme_dir: Optional[str] = None,
    metadata_l1_bytes: Optional[int] = None,
    partition_l1_bytes: Optional[int] = None,
    metadata_cache_size_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    """Return one spec dict for `(scenario, actor_id)`.

    The dict shape matches `_hybrid_cache_helpers.build_session`.

    Args:
        scenario: One of `no-cache`, `moka`, `distributed`.
        actor_id: Worker actor index, threaded into the per-actor L2 path
            for the `distributed` scenario. `no-cache` and `moka` accept
            it for signature uniformity.
        dram_bytes: Per-actor DRAM budget for the `moka` scenario; ignored
            by `no-cache` and `distributed` (the distributed scenario
            sizes DRAM via `metadata_l1_bytes` + `partition_l1_bytes`).
        nvme_dir: Base directory for per-actor L2 subdirectories. Required
            for `distributed`; ignored by the other scenarios.
        metadata_l1_bytes: v6 metadata-L1 budget for the `distributed`
            scenario. Mandatory and non-zero for `distributed`; a warning
            is printed below `METADATA_L1_FLOOR_BYTES`.
        partition_l1_bytes: v6 decoded-partition-L1 budget for the
            `distributed` scenario. `None` disables the partition L1
            tier (every partition decode hits L2). This is the only
            "off" spelling; the caller is expected to map an operator's
            `--partition-l1-mb 0` to `None`.
        metadata_cache_size_bytes: Optional session-wide
            `metadata_cache_size_bytes` for the `no-cache` / `moka`
            constructors. Ignored for `distributed` (whose metadata
            budget is `metadata_l1_bytes`).
    """
    if scenario not in _KNOWN_SCENARIOS:
        raise ValueError(
            f"unknown scenario: {scenario!r}; "
            f"expected one of {sorted(_KNOWN_SCENARIOS)}"
        )

    if scenario == "no-cache":
        spec: Dict[str, Any] = {"name": "no-cache", "kind": "no-cache"}
        if metadata_cache_size_bytes is not None:
            spec["metadata_cache_size_bytes"] = int(metadata_cache_size_bytes)
        return spec

    if scenario == "moka":
        if dram_bytes <= 0:
            raise ValueError(
                f"dram_bytes={dram_bytes} must be > 0 for the moka scenario"
            )
        spec = {
            "name": "moka",
            "kind": "moka",
            "index_cache_size_bytes": int(dram_bytes),
        }
        if metadata_cache_size_bytes is not None:
            spec["metadata_cache_size_bytes"] = int(metadata_cache_size_bytes)
        return spec

    # scenario == "distributed"
    if nvme_dir is None:
        raise ValueError("nvme_dir is required for the distributed scenario")
    if metadata_l1_bytes is None:
        raise ValueError(
            "metadata_l1_bytes is required for the distributed scenario "
            "(v6 distributed cache routes via the metadata L1 tier)"
        )
    _validate_distributed(
        nvme_dir=nvme_dir,
        metadata_l1_bytes=metadata_l1_bytes,
        partition_l1_bytes=partition_l1_bytes,
    )
    return {
        "name": "distributed",
        "kind": "distributed",
        "l2_dir": per_actor_l2_dir(nvme_dir, actor_id),
        "metadata_l1_bytes": int(metadata_l1_bytes),
        "partition_l1_bytes": (
            int(partition_l1_bytes) if partition_l1_bytes is not None else None
        ),
    }


def _validate_distributed(
    nvme_dir: str,
    metadata_l1_bytes: int,
    partition_l1_bytes: Optional[int],
) -> None:
    """Validate the distributed scenario's per-actor budgets and L2 path.

    Conservative on the filesystem side: rejects unsafe prefixes and
    non-absolute paths, but does NOT stat the per-actor L2 subdirectory
    and does NOT mkdir. The actor process creates its own L2 directory
    in-place before `Session.with_distributed_cache(...)` is called; in a
    multi-node cluster the worker's NVMe is not visible from the driver
    host and any driver-side mkdir would be a no-op or misleading
    success.
    """
    if metadata_l1_bytes <= 0:
        raise ValueError(
            f"metadata_l1_bytes={metadata_l1_bytes} must be > 0 "
            "(distributed cache routing requires a non-zero metadata L1)"
        )
    if metadata_l1_bytes < METADATA_L1_FLOOR_BYTES:
        print(
            f"warning: metadata_l1_bytes={metadata_l1_bytes} below "
            f"{METADATA_L1_FLOOR_BYTES}-byte floor; per-query routing path may "
            "thrash (IvfIndexState/IndexMetadata/FragReuseIndex live here)",
            file=sys.stderr,
        )
    if partition_l1_bytes is not None and partition_l1_bytes <= 0:
        raise ValueError(
            f"partition_l1_bytes={partition_l1_bytes} must be > 0 or None "
            "(pass None to disable the partition L1 tier)"
        )

    l2_path = Path(nvme_dir).expanduser()
    if not l2_path.is_absolute():
        raise ValueError(
            f"nvme_dir={nvme_dir!r} must be an absolute path on a large local disk"
        )
    l2_path = l2_path.resolve(strict=False)
    if l2_path == Path("/") or any(
        _is_relative_to(l2_path, prefix) for prefix in UNSAFE_L2_DIR_PREFIXES
    ):
        raise ValueError(
            f"nvme_dir={nvme_dir!r} is not a safe L2 directory. Use a large "
            "local disk such as /mnt/nvme/... or /data/fast/...; /tmp, "
            "/var/tmp, /dev/shm, and filesystem root are rejected because "
            "they can be tmpfs/system temp paths and defeat the L2 experiment."
        )


def _is_relative_to(path: Path, prefix: Path) -> bool:
    try:
        path.relative_to(prefix)
    except ValueError:
        return False
    return True


def build_scenario_specs(
    scenarios: List[str],
    *,
    dram_bytes: int = 0,
    nvme_dir: Optional[str] = None,
    metadata_l1_bytes: int = 64 * MIB,
    partition_l1_bytes: Optional[int] = 1024 * MIB,
    metadata_bytes: Optional[int] = None,
    l2_bytes: Optional[int] = None,
    codecless_bytes: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Plural-list builder; one spec per scenario name, all with `actor_id=0`.

    Convenience wrapper around `build_scenario_spec` for the single-actor
    driver and any caller that doesn't want to loop. Multi-actor drivers
    should call `build_scenario_spec` directly with `actor_id=i` so each
    actor gets a distinct `per_actor_l2_dir` path.

    Aliases the v4 `hybrid` scenario name to v6 `distributed` with a
    one-time stderr deprecation notice so old `--scenarios
    no-cache,moka,hybrid` strings keep working through the transition.
    `l2_bytes` and `codecless_bytes` are accepted for v4-caller signature
    compatibility but ignored: the v6 distributed cache does no L2
    capacity bookkeeping and has no codec-less Moka knob.
    """
    if codecless_bytes is not None:
        print(
            "[scenarios] codecless_bytes is a v4 hybrid concept with no v6 "
            "analog; ignored.",
            file=sys.stderr,
        )

    specs: List[Dict[str, Any]] = []
    aliased_hybrid = False
    for raw in scenarios:
        name = raw.strip()
        if not name:
            continue
        if name == "hybrid":
            if not aliased_hybrid:
                print(
                    "[scenarios] 'hybrid' is renamed to 'distributed' in v6; "
                    "auto-aliasing.",
                    file=sys.stderr,
                )
                aliased_hybrid = True
            name = "distributed"
        specs.append(
            build_scenario_spec(
                name,
                actor_id=0,
                dram_bytes=dram_bytes,
                nvme_dir=nvme_dir,
                metadata_l1_bytes=metadata_l1_bytes,
                partition_l1_bytes=partition_l1_bytes,
                metadata_cache_size_bytes=metadata_bytes,
            )
        )
    return specs
