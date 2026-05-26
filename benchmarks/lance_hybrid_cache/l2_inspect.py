"""Filesystem-level observability for the Lance v6 distributed-cache L2 directory.

Lance 6.0's distributed cache writes one ``part-ivf-{id}.bin`` per
prewarmed partition under a deterministic on-disk layout. Unlike v4's
opaque foyer region files, the v6 layout one-to-one maps file presence
to L2 residency, so a directory walk is an *exact* residency probe
rather than the v4 coarse fallback signal.

Layout::

    {l2_dir}/
        lance-distributed.lock
        v1/
            .manifest.json
            .tombstones.json
            {sanitize(prefix)}/
                part-ivf-0.bin
                part-ivf-2.bin
                ...
            .{sanitize(prefix)}.deleting-{nonce}/   # background-removal sentinel
                ...

``snapshot_l2_dir(path)`` returns a structured view of the layout so
callers can:

* Check that the v6 process is healthy (``lock_present``,
  ``manifest_present``).
* Verify no invalidation rename failed silently
  (``tombstones_present`` / ``tombstones_added``).
* Surface the per-prefix decoded-partition file count and on-disk
  footprint without having to walk the directory themselves.

``diff_snapshots(pre, post)`` adds ``tombstones_added``: True when a new
tombstone appeared between the two probes -- a hard-error signal that
an invalidation hit the rename-failure path (see
``Session.invalidate_index_cache``).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional


# v6 distributed-cache constants. These mirror the names the Rust
# backend writes; if any of them shift in Lance, this file is the one
# place that needs to follow.
LOCK_FILENAME = "lance-distributed.lock"
V1_SUBDIR = "v1"
MANIFEST_FILENAME = ".manifest.json"
TOMBSTONES_FILENAME = ".tombstones.json"

# part-ivf-{id}.bin where {id} is a non-negative integer.
_PART_IVF_RE = re.compile(r"^part-ivf-(\d+)\.bin$")

# .{sanitize(prefix)}.deleting-{nonce} marks a prefix subdirectory whose
# contents are being removed in the background after invalidation.
_DELETING_DIR_RE = re.compile(r"^\..+\.deleting-")


def _stat_safely(p: Path) -> Optional[Any]:
    """Stat ``p`` without following symlinks; return ``None`` on OSError.

    The actor may still have a region file open with ``O_DIRECT`` during
    the snapshot; we'd rather skip the entry than fail the snapshot.
    """
    try:
        return p.stat()
    except OSError:
        return None


def _file_bytes(p: Path) -> tuple[int, int]:
    """Return ``(apparent_bytes, disk_bytes)`` for a regular file.

    ``st_blocks`` is in 512-byte units regardless of the filesystem's
    logical block size. Some platforms report 0 for special files;
    Lance v6 partition files are regular files so this is safe.
    """
    st = _stat_safely(p)
    if st is None:
        return 0, 0
    apparent = int(st.st_size)
    disk = int(getattr(st, "st_blocks", 0)) * 512
    return apparent, disk


def _walk_prefix_dir(prefix_path: Path) -> tuple[int, int, int, List[Dict[str, Any]]]:
    """Walk a single ``v1/{prefix}/`` subdirectory.

    Returns ``(file_count, apparent_bytes, disk_bytes, files)`` where
    ``files`` is the per-leaf detail keyed on the relative path under
    the L2 root.
    """
    file_count = 0
    apparent_total = 0
    disk_total = 0
    files: List[Dict[str, Any]] = []
    for fp in sorted(prefix_path.iterdir()):
        if not fp.is_file():
            continue
        apparent, disk = _file_bytes(fp)
        apparent_total += apparent
        disk_total += disk
        file_count += 1
        files.append(
            {
                "name": str(fp.relative_to(prefix_path.parent.parent)),
                "apparent_bytes": apparent,
                "disk_bytes": disk,
            }
        )
    return file_count, apparent_total, disk_total, files


def snapshot_l2_dir(path: str) -> Dict[str, Any]:
    """Return a snapshot of the v6 L2 directory's filesystem footprint.

    Safe to call when the directory does not yet exist (returns an
    ``exists=False`` shell). Distinguishes apparent size (``st_size``)
    from on-disk size (``st_blocks * 512``) so sparse/preallocated
    regions are visible.

    Returned fields:

    * ``path`` -- the input path, as a string.
    * ``exists`` -- whether the L2 directory itself exists.
    * ``lock_present`` -- whether ``lance-distributed.lock`` exists at
      the top level. The v6 backend holds an exclusive advisory lock on
      this file for the session's lifetime; its absence means no live
      writer.
    * ``manifest_present`` -- whether ``v1/.manifest.json`` exists.
    * ``tombstones_present`` -- whether ``v1/.tombstones.json`` exists.
    * ``tombstones_bytes`` -- apparent size of the tombstones file in
      bytes (0 when absent). Lets ``diff_snapshots`` detect new
      tombstones by size growth.
    * ``prefix_dirs`` -- list of per-prefix subdir summaries. Each entry
      has ``name`` (directory name as it appears under ``v1/``),
      ``file_count``, ``apparent_bytes``, ``disk_bytes``, and
      ``deleting`` (True for ``.{prefix}.deleting-{nonce}`` sentinels).
    * ``file_count`` / ``apparent_bytes`` / ``disk_bytes`` -- aggregates
      summed across ``prefix_dirs`` (including deleting sentinels).
    * ``files`` -- flat per-file detail (relative paths under the L2
      root). Callers that only need totals can drop this field.
    """
    p = Path(path)
    snap: Dict[str, Any] = {
        "path": str(p),
        "exists": p.exists(),
        "lock_present": False,
        "manifest_present": False,
        "tombstones_present": False,
        "tombstones_bytes": 0,
        "prefix_dirs": [],
        "file_count": 0,
        "apparent_bytes": 0,
        "disk_bytes": 0,
        "files": [],
    }
    if not snap["exists"]:
        return snap

    snap["lock_present"] = (p / LOCK_FILENAME).is_file()

    v1 = p / V1_SUBDIR
    if not v1.is_dir():
        return snap

    snap["manifest_present"] = (v1 / MANIFEST_FILENAME).is_file()
    tombstones = v1 / TOMBSTONES_FILENAME
    snap["tombstones_present"] = tombstones.is_file()
    if snap["tombstones_present"]:
        st = _stat_safely(tombstones)
        snap["tombstones_bytes"] = int(st.st_size) if st is not None else 0

    prefix_dirs: List[Dict[str, Any]] = []
    files_all: List[Dict[str, Any]] = []
    total_file_count = 0
    total_apparent = 0
    total_disk = 0
    for child in sorted(v1.iterdir()):
        if not child.is_dir():
            continue
        deleting = bool(_DELETING_DIR_RE.match(child.name))
        fc, ap, dk, files = _walk_prefix_dir(child)
        prefix_dirs.append(
            {
                "name": child.name,
                "file_count": fc,
                "apparent_bytes": ap,
                "disk_bytes": dk,
                "deleting": deleting,
            }
        )
        files_all.extend(files)
        total_file_count += fc
        total_apparent += ap
        total_disk += dk

    snap["prefix_dirs"] = prefix_dirs
    snap["file_count"] = total_file_count
    snap["apparent_bytes"] = total_apparent
    snap["disk_bytes"] = total_disk
    snap["files"] = files_all
    return snap


def parse_partition_id(filename: str) -> Optional[int]:
    """Return the partition id encoded in a ``part-ivf-{id}.bin`` name.

    Returns ``None`` for any name that does not match the v6 partition
    file pattern (e.g. ``.manifest.json``, future sidecars).
    """
    m = _PART_IVF_RE.match(Path(filename).name)
    return int(m.group(1)) if m else None


def diff_snapshots(pre: Dict[str, Any], post: Dict[str, Any]) -> Dict[str, Any]:
    """Compute pre→post deltas. ``{}`` if either side is missing.

    Returns ``apparent_bytes_delta``, ``disk_bytes_delta``,
    ``file_count_delta``, and ``tombstones_added`` (bool). A True
    ``tombstones_added`` between two probes is a hard error -- it means
    an invalidation rename failed and the v6 backend wrote a tombstone
    so future opens of that prefix skip it.
    """
    if not pre or not post or not post.get("exists"):
        return {}
    pre_tombstones = bool(pre.get("tombstones_present"))
    post_tombstones = bool(post.get("tombstones_present"))
    pre_tomb_bytes = int(pre.get("tombstones_bytes", 0))
    post_tomb_bytes = int(post.get("tombstones_bytes", 0))
    tombstones_added = (
        (post_tombstones and not pre_tombstones)
        or (post_tombstones and post_tomb_bytes > pre_tomb_bytes)
    )
    return {
        "apparent_bytes_delta": int(post["apparent_bytes"]) - int(pre.get("apparent_bytes", 0)),
        "disk_bytes_delta": int(post["disk_bytes"]) - int(pre.get("disk_bytes", 0)),
        "file_count_delta": int(post["file_count"]) - int(pre.get("file_count", 0)),
        "tombstones_added": tombstones_added,
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


def format_l2_summary_line(
    name: str,
    repeat: int,
    snap: Dict[str, Any],
    delta: Optional[Dict[str, Any]] = None,
) -> str:
    if not snap.get("exists"):
        return f"  [{name} r{repeat}] L2 dir absent ({snap.get('path')})"
    flags = []
    if snap.get("lock_present"):
        flags.append("lock")
    if snap.get("manifest_present"):
        flags.append("manifest")
    if snap.get("tombstones_present"):
        flags.append("tombstones")
    prefix_dirs = snap.get("prefix_dirs") or []
    deleting = sum(1 for pd in prefix_dirs if pd.get("deleting"))
    parts = [
        f"  [{name} r{repeat}] L2 dir={snap['path']}",
        "flags=" + (",".join(flags) if flags else "-"),
        f"prefixes={len(prefix_dirs)}" + (f"(deleting={deleting})" if deleting else ""),
        f"files={snap['file_count']}",
        f"apparent={format_bytes(snap['apparent_bytes'])}",
        f"disk={format_bytes(snap['disk_bytes'])}",
    ]
    if delta:
        delta_parts = [
            f"apparent={format_bytes(delta['apparent_bytes_delta'])}",
            f"disk={format_bytes(delta['disk_bytes_delta'])}",
            f"files={delta['file_count_delta']:+d}",
        ]
        if delta.get("tombstones_added"):
            delta_parts.append("tombstones_added=True")
        parts.append("Δ(" + ", ".join(delta_parts) + ")")
    return "  ".join(parts)
