"""Scenario specs for the Lance hybrid-cache benchmark.

Byte values are tuned for a 12 CPU / 32 GB host and a ~10 GB IVF_RQ index
(10M * 1024-d, num_bits=8, 3000 partitions). With a 4 GiB DRAM cap, moka
thrashes and hybrid's NVMe tier carries the hot working set.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

MIB = 1024 * 1024
GIB = 1024 * MIB

MIN_L1_BYTES = 1 * MIB  # foyer rejects smaller L1
MIN_L2_BYTES = 1 * GIB  # 4x default 256 MiB block size
UNSAFE_L2_DIR_PREFIXES = (
    Path("/tmp"),
    Path("/var/tmp"),
    Path("/dev/shm"),
)


def build_scenario_specs(
    scenarios: List[str],
    dram_bytes: int,
    l2_bytes: int,
    nvme_dir: str,
    metadata_bytes: Optional[int] = None,
    codecless_bytes: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return one spec dict per requested scenario name.

    The dict shape matches `_hybrid_cache_helpers.build_session`.

    `codecless_bytes` is the size of the codec-less embedded Moka in the
    hybrid scenario. When set, the hybrid spec switches to
    `with_hybrid_cache_advanced` (lance-open-source 8c7c4d96c) and the
    foyer L1 tier gets `dram_bytes - codecless_bytes`. The total DRAM
    consumed by hybrid stays at `dram_bytes`, matching `moka`'s budget.
    Use this to override Lance's default 90/10 foyer/Moka split when
    the codec-less working set (top-level index objects, scalar index
    pages, legacy IVF v1 entries) needs a different reserve than the
    10% default.
    """
    _validate(dram_bytes, l2_bytes, nvme_dir, scenarios, codecless_bytes)

    wanted = {s.strip() for s in scenarios if s.strip()}
    specs: List[Dict[str, Any]] = []

    if "no-cache" in wanted:
        specs.append({"name": "no-cache", "kind": "no-cache"})

    if "moka" in wanted:
        specs.append(
            {
                "name": "moka",
                "kind": "moka",
                "index_cache_size_bytes": dram_bytes,
            }
        )

    if "hybrid" in wanted:
        hybrid_spec: Dict[str, Any] = {
            "name": "hybrid",
            "kind": "hybrid",
            "l2_dir": nvme_dir,
            "l2_capacity_bytes": l2_bytes,
        }
        if codecless_bytes is None:
            # with_hybrid_cache: combined L1 budget; Lance splits it
            # 90/10 between foyer L1 (the bulk, since codec-bearing
            # IVF partition entries dominate hot vector workloads) and
            # the codec-less embedded Moka internally.
            hybrid_spec["l1_capacity_bytes"] = dram_bytes
        else:
            # with_hybrid_cache_advanced: dram_bytes is the total DRAM
            # ceiling. codecless_bytes carves off the codec-less Moka
            # slice; the remainder is the foyer DRAM tier.
            hybrid_spec["l1_capacity_bytes"] = dram_bytes - codecless_bytes
            hybrid_spec["codecless_capacity_bytes"] = codecless_bytes
        specs.append(hybrid_spec)

    unknown = wanted - {"no-cache", "moka", "hybrid"}
    if unknown:
        raise ValueError(f"unknown scenario(s): {sorted(unknown)}")

    # Fan a single metadata-cache size across every scenario so the
    # comparison stays apples-to-apples. Only injected when the operator
    # explicitly set it; absent key means Lance's default applies.
    if metadata_bytes is not None:
        for spec in specs:
            spec["metadata_cache_size_bytes"] = int(metadata_bytes)

    return specs


def _validate(
    dram_bytes: int,
    l2_bytes: int,
    nvme_dir: str,
    scenarios: List[str],
    codecless_bytes: Optional[int] = None,
) -> None:
    if dram_bytes < MIN_L1_BYTES:
        raise ValueError(
            f"dram_bytes={dram_bytes} < {MIN_L1_BYTES} (foyer requires L1 >= 1 MiB)"
        )
    if codecless_bytes is not None:
        if codecless_bytes <= 0:
            raise ValueError(f"codecless_bytes={codecless_bytes} must be > 0")
        if codecless_bytes >= dram_bytes:
            raise ValueError(
                f"codecless_bytes={codecless_bytes} >= dram_bytes={dram_bytes}; "
                "nothing left for foyer L1"
            )
        foyer_l1_bytes = dram_bytes - codecless_bytes
        if foyer_l1_bytes < MIN_L1_BYTES:
            raise ValueError(
                f"foyer L1 = dram_bytes - codecless_bytes = {foyer_l1_bytes} "
                f"< {MIN_L1_BYTES} (foyer requires L1 >= 1 MiB)"
            )
    if "hybrid" in scenarios:
        if l2_bytes < MIN_L2_BYTES:
            raise ValueError(
                f"l2_bytes={l2_bytes} < {MIN_L2_BYTES} (foyer requires L2 >= 1 GiB "
                "with default 256 MiB block size)"
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
        # Parent must exist and be writable. Create it manually per README
        # (typically requires sudo). Failing here points users at the setup step.
        parent = l2_path.parent
        if not parent.exists():
            raise ValueError(
                f"nvme_dir parent {parent!r} does not exist. Run: "
                f"`sudo mkdir -p {nvme_dir} && sudo chown $USER {nvme_dir}`"
            )


def _is_relative_to(path: Path, prefix: Path) -> bool:
    try:
        path.relative_to(prefix)
    except ValueError:
        return False
    return True
