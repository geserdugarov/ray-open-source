"""Read-only inspection of Lance 12 distributed-cache directories.

Lance deliberately hashes dataset, index, and partition identities before
putting them on disk.  This module therefore reports aggregate occupancy; it
must not claim that a particular filename corresponds to a partition id.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional


CACHE_KEY_FORMAT = "blake3-128-v1"
L2_FORMAT_VERSION = "v1"
LOCK_FILENAME = "lance-distributed.lock"
TOMBSTONES_FILENAME = ".tombstones.json"

_NAMESPACE_RE = re.compile(r"^[0-9a-f]{64}$")
_ENTRY_RE = re.compile(r"^[0-9a-f]{32}\.bin$")
_DELETING_RE = re.compile(r"^\.deleting-[0-9a-f]{64}-[0-9a-f]{16}$")


def _file_sizes(path: Path) -> tuple[int, int]:
    """Return apparent and allocated bytes, or zeros if the stat races."""
    try:
        stat_result = path.stat()
    except OSError:
        return 0, 0
    return int(stat_result.st_size), int(getattr(stat_result, "st_blocks", 0)) * 512


def _scan_namespace(path: Path, root: Path) -> Dict[str, Any]:
    files = []
    apparent_bytes = 0
    disk_bytes = 0
    invalid_files = []
    try:
        children = sorted(path.iterdir())
    except OSError as error:
        return {
            "name": path.name,
            "cache_entry_count": 0,
            "apparent_bytes": 0,
            "disk_bytes": 0,
            "files": [],
            "invalid_files": [],
            "scan_error": repr(error),
        }

    for child in children:
        if child.is_symlink() or not child.is_file():
            invalid_files.append(str(child.relative_to(root)))
            continue
        if not _ENTRY_RE.fullmatch(child.name):
            invalid_files.append(str(child.relative_to(root)))
            continue
        apparent, disk = _file_sizes(child)
        apparent_bytes += apparent
        disk_bytes += disk
        files.append(
            {
                "name": str(child.relative_to(root)),
                "apparent_bytes": apparent,
                "disk_bytes": disk,
            }
        )

    return {
        "name": path.name,
        "cache_entry_count": len(files),
        "apparent_bytes": apparent_bytes,
        "disk_bytes": disk_bytes,
        "files": files,
        "invalid_files": invalid_files,
        "scan_error": None,
    }


def _read_tombstones(path: Path) -> tuple[int, Optional[str]]:
    if not path.is_file():
        return 0, None
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        return 0, repr(error)
    if not isinstance(value, list) or not all(
        isinstance(item, str) and _NAMESPACE_RE.fullmatch(item) for item in value
    ):
        return 0, "expected a JSON list of 64-character lowercase hex namespaces"
    return len(value), None


def snapshot_l2_dir(path: str | os.PathLike[str]) -> Dict[str, Any]:
    """Return a JSON-serializable snapshot of a Lance 12 L2 directory.

    Only live namespace directories and valid opaque entry files contribute to
    ``cache_entry_count`` and byte totals.  Deleting directories are reported
    separately because their contents are no longer eligible for cache hits.
    """
    root = Path(path)
    version_dir = root / CACHE_KEY_FORMAT / L2_FORMAT_VERSION
    snapshot: Dict[str, Any] = {
        "path": str(root),
        "exists": root.is_dir(),
        "lock_present": False,
        "key_format_present": False,
        "version_present": False,
        "tombstones_present": False,
        "tombstone_count": 0,
        "tombstone_parse_error": None,
        "namespace_count": 0,
        "namespace_dirs": [],
        "deleting_dir_count": 0,
        "deleting_dirs": [],
        "cache_entry_count": 0,
        "apparent_bytes": 0,
        "disk_bytes": 0,
        "invalid_entries": [],
        "files": [],
    }
    if not snapshot["exists"]:
        return snapshot

    snapshot["lock_present"] = (root / LOCK_FILENAME).is_file()
    snapshot["key_format_present"] = (root / CACHE_KEY_FORMAT).is_dir()
    snapshot["version_present"] = version_dir.is_dir()
    if not snapshot["version_present"]:
        return snapshot

    tombstones_path = version_dir / TOMBSTONES_FILENAME
    snapshot["tombstones_present"] = tombstones_path.is_file()
    tombstone_count, tombstone_error = _read_tombstones(tombstones_path)
    snapshot["tombstone_count"] = tombstone_count
    snapshot["tombstone_parse_error"] = tombstone_error

    namespace_dirs = []
    deleting_dirs = []
    invalid_entries = []
    try:
        version_children = sorted(version_dir.iterdir())
    except OSError as error:
        version_children = []
        invalid_entries.append(f"scan error for {version_dir}: {error!r}")
    for child in version_children:
        if child.is_symlink():
            invalid_entries.append(str(child.relative_to(root)))
        elif child.is_dir() and _NAMESPACE_RE.fullmatch(child.name):
            namespace_dirs.append(_scan_namespace(child, root))
        elif child.is_dir() and _DELETING_RE.fullmatch(child.name):
            deleting_dirs.append(_scan_namespace(child, root))
        elif child == tombstones_path:
            continue
        else:
            invalid_entries.append(str(child.relative_to(root)))

    namespace_files = [
        file_info for namespace in namespace_dirs for file_info in namespace["files"]
    ]
    invalid_entries.extend(
        invalid
        for namespace in (*namespace_dirs, *deleting_dirs)
        for invalid in namespace["invalid_files"]
    )

    snapshot.update(
        {
            "namespace_count": len(namespace_dirs),
            "namespace_dirs": namespace_dirs,
            "deleting_dir_count": len(deleting_dirs),
            "deleting_dirs": deleting_dirs,
            "cache_entry_count": len(namespace_files),
            "apparent_bytes": sum(item["apparent_bytes"] for item in namespace_files),
            "disk_bytes": sum(item["disk_bytes"] for item in namespace_files),
            "invalid_entries": invalid_entries,
            "files": namespace_files,
        }
    )
    return snapshot


def diff_snapshots(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, int]:
    """Return aggregate live-cache deltas between two snapshots."""
    fields = (
        "cache_entry_count",
        "namespace_count",
        "apparent_bytes",
        "disk_bytes",
        "tombstone_count",
        "deleting_dir_count",
    )
    return {
        f"{field}_delta": int(after.get(field, 0)) - int(before.get(field, 0))
        for field in fields
    }


def invalidation_errors(snapshot: Dict[str, Any]) -> list[str]:
    """Describe state that should not remain after successful invalidation.

    A benchmark directory is dedicated to one dataset/index.  Consequently a
    successful invalidation of every physical segment should leave no live or
    deleting namespaces and no non-empty persistent tombstones.
    """
    errors = []
    if snapshot.get("namespace_count"):
        errors.append(f"{snapshot['namespace_count']} live namespace(s) remain")
    if snapshot.get("deleting_dir_count"):
        errors.append(f"{snapshot['deleting_dir_count']} deleting namespace(s) remain")
    if snapshot.get("tombstone_count"):
        errors.append(f"{snapshot['tombstone_count']} persistent tombstone(s) remain")
    if snapshot.get("tombstone_parse_error"):
        errors.append(f"invalid tombstone file: {snapshot['tombstone_parse_error']}")
    if snapshot.get("invalid_entries"):
        errors.append(f"unexpected cache paths: {snapshot['invalid_entries'][:8]}")
    return errors


def format_bytes(value: int) -> str:
    sign = "-" if value < 0 else ""
    size = float(abs(value))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            if unit == "B":
                return f"{sign}{int(size)} {unit}"
            return f"{sign}{size:.1f} {unit}"
        size /= 1024
    raise AssertionError("unreachable")


def format_snapshot(snapshot: Dict[str, Any]) -> str:
    if not snapshot.get("exists"):
        return f"L2 absent ({snapshot.get('path')})"
    return (
        f"L2={snapshot.get('path')} entries={snapshot.get('cache_entry_count', 0)} "
        f"namespaces={snapshot.get('namespace_count', 0)} "
        f"apparent={format_bytes(int(snapshot.get('apparent_bytes', 0)))} "
        f"disk={format_bytes(int(snapshot.get('disk_bytes', 0)))} "
        f"tombstones={snapshot.get('tombstone_count', 0)}"
    )
