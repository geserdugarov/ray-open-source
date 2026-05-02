"""Filesystem-level observability for foyer's NVMe L2 directory.

Foyer exposes neither L2 iteration nor live-occupancy via its public API
(`HybridCache::storage()` returns the engine but no aggregate counters).
The Lance hybrid backend's per-partition L2 probes
(`IvfIndexSearcher::partition_is_in_l2`) exist in the Rust side but are
not yet bound to pylance. So the next-best signal is the L2 directory
itself: foyer's block engine creates region files whose `st_blocks *
512` (the "disk" footprint) grows as it writes new blocks, even though
`st_size` (the "apparent" size) reflects pre-allocated capacity.

A pre/post pair around an actor run is therefore a *coarse fallback*
signal that callers use to flag unexpected L2 growth. It is approximate
in a few directions:

- Foyer can recycle blocks on overwrite, so stable disk-byte counts
  only prove there was no *visible growth* and no *file-count churn* —
  not that nothing was overwritten in place.
- Under the no-vector-L1-writeback policy, vector partition entries
  evicted from L1 are dropped from RAM and never re-written to L2 (the
  L2 entry from prewarm already exists). Stable byte totals across the
  measure phase are therefore consistent with the expected behavior,
  but they are not by themselves *proof* of no eviction writeback —
  use them in combination with the prewarm-time
  ``disk_bytes_unknown_spills == 0`` check and (when bound) the
  per-partition L2 probe in ``check_partition_residency``.
- File-count growth between pre and post strongly suggests new L2
  region files were allocated, which under the no-writeback policy
  should never happen for vector partitions and warrants investigation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional


def snapshot_l2_dir(path: str) -> Dict[str, Any]:
    """Return a snapshot of the L2 directory's filesystem footprint.

    Distinguishes apparent size (`st_size`) from on-disk size
    (`st_blocks * 512`) so sparse/preallocated regions are visible.
    Safe to call when the directory does not yet exist (returns an
    `exists=False` shell). Symlinks are followed for the dir traversal
    but the per-file stat is non-following — foyer doesn't create
    symlinks so this only matters defensively.
    """
    p = Path(path)
    snap: Dict[str, Any] = {
        "path": str(p),
        "exists": p.exists(),
        "apparent_bytes": 0,
        "disk_bytes": 0,
        "file_count": 0,
        "files": [],
    }
    if not snap["exists"]:
        return snap

    files: List[Dict[str, Any]] = []
    apparent_total = 0
    disk_total = 0
    for fp in sorted(p.rglob("*")):
        try:
            if not fp.is_file():
                continue
            st = fp.stat()
        except OSError:
            # The actor may still be holding the file open with O_DIRECT;
            # we'd rather skip the entry than fail the snapshot.
            continue
        apparent = int(st.st_size)
        # st_blocks is in 512-byte units regardless of the filesystem's
        # logical block size. Some platforms report 0 for special files;
        # foyer's regions are regular files so this is safe.
        disk = int(getattr(st, "st_blocks", 0)) * 512
        apparent_total += apparent
        disk_total += disk
        files.append(
            {
                "name": str(fp.relative_to(p)),
                "apparent_bytes": apparent,
                "disk_bytes": disk,
            }
        )

    snap["apparent_bytes"] = apparent_total
    snap["disk_bytes"] = disk_total
    snap["file_count"] = len(files)
    snap["files"] = files
    return snap


def diff_snapshots(pre: Dict[str, Any], post: Dict[str, Any]) -> Dict[str, Any]:
    """Compute pre→post deltas. `None` if either side is missing."""
    if not pre or not post or not post.get("exists"):
        return {}
    return {
        "apparent_bytes_delta": int(post["apparent_bytes"]) - int(pre.get("apparent_bytes", 0)),
        "disk_bytes_delta": int(post["disk_bytes"]) - int(pre.get("disk_bytes", 0)),
        "file_count_delta": int(post["file_count"]) - int(pre.get("file_count", 0)),
    }


def format_bytes(n: int) -> str:
    """Compact human-readable byte count: '4.0 GiB', '226 MiB', '512 B'."""
    sign = "-" if n < 0 else ""
    n = abs(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{sign}{n:.1f} {unit}" if unit != "B" else f"{sign}{n} {unit}"
        n /= 1024
    return f"{sign}{n} TiB"


def format_l2_summary_line(name: str, repeat: int, snap: Dict[str, Any], delta: Optional[Dict[str, Any]] = None) -> str:
    if not snap.get("exists"):
        return f"  [{name} r{repeat}] L2 dir absent ({snap.get('path')})"
    parts = [
        f"  [{name} r{repeat}] L2 dir={snap['path']}",
        f"files={snap['file_count']}",
        f"apparent={format_bytes(snap['apparent_bytes'])}",
        f"disk={format_bytes(snap['disk_bytes'])}",
    ]
    if delta:
        parts.append(
            f"Δ(apparent={format_bytes(delta['apparent_bytes_delta'])}, "
            f"disk={format_bytes(delta['disk_bytes_delta'])}, "
            f"files={delta['file_count_delta']:+d})"
        )
    return "  ".join(parts)
