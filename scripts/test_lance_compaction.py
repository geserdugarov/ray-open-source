#!/usr/bin/env python3
"""Exercise Lance table compaction on a small local dataset.

The script creates a Lance dataset with many small fragments, creates an
optional scalar index, soft-deletes rows, runs `dataset.optimize.compact_files`,
and asserts that compaction preserves logical rows while reducing/removing
fragment-level delete state.

Run from a Lance Python environment, for example:

    cd ../lance-open-source/python
    uv run python ../../ray-open-source/scripts/test_lance_compaction.py
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any


TEMP_ROOT = Path("/tmp")


def _import_dependencies() -> tuple[Any, Any]:
    try:
        import lance
        import pyarrow as pa
    except ModuleNotFoundError as exc:
        lance_repo = Path(__file__).resolve().parents[2] / "lance-open-source"
        raise SystemExit(
            f"Missing dependency {exc.name!r}. Run this script from a Lance "
            "Python environment, for example:\n"
            f"  cd {lance_repo / 'python'}\n"
            "  make install\n"
            f"  uv run python {Path(__file__).resolve()}"
        ) from exc
    return lance, pa


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create, delete, compact, and validate a Lance dataset.",
    )
    parser.add_argument(
        "--uri",
        type=Path,
        default=None,
        help=(
            "Dataset path under /tmp. Relative paths are resolved under /tmp. "
            "Defaults to a temporary /tmp directory."
        ),
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1200,
        help="Number of rows to write before deletes.",
    )
    parser.add_argument(
        "--rows-per-fragment",
        type=int,
        default=50,
        help="max_rows_per_file used during initial write.",
    )
    parser.add_argument(
        "--target-rows-per-fragment",
        type=int,
        default=400,
        help="target_rows_per_fragment passed to compact_files.",
    )
    parser.add_argument(
        "--delete-bucket",
        type=int,
        default=0,
        help="Delete rows where bucket equals this value.",
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=5,
        help="Number of buckets used to spread deletes across fragments.",
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip the BTREE index check.",
    )
    parser.add_argument(
        "--defer-index-remap",
        action="store_true",
        help="Pass defer_index_remap=True to compact_files.",
    )
    parser.add_argument(
        "--compaction-mode",
        choices=("reencode", "try_binary_copy", "force_binary_copy"),
        default=None,
        help="Optional compaction mode passed to compact_files.",
    )
    parser.add_argument(
        "--cleanup-old-versions",
        action="store_true",
        help="After validation, call cleanup_old_versions(retain_versions=1).",
    )
    parser.add_argument(
        "--strict-order",
        action="store_true",
        help="Fail if the post-compaction scan order differs from insertion order.",
    )
    parser.add_argument(
        "--ray-check",
        action="store_true",
        help="Also verify the compacted table through ray.data.read_lance.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep the generated dataset instead of deleting temporary data.",
    )
    args = parser.parse_args()

    if args.rows <= 0:
        parser.error("--rows must be positive")
    if args.rows_per_fragment <= 0:
        parser.error("--rows-per-fragment must be positive")
    if args.target_rows_per_fragment <= 0:
        parser.error("--target-rows-per-fragment must be positive")
    if args.bucket_count <= 1:
        parser.error("--bucket-count must be greater than 1")
    if not 0 <= args.delete_bucket < args.bucket_count:
        parser.error("--delete-bucket must satisfy 0 <= value < --bucket-count")
    if args.rows_per_fragment >= args.rows:
        parser.error("--rows-per-fragment must be smaller than --rows")
    if args.target_rows_per_fragment <= args.rows_per_fragment:
        parser.error("--target-rows-per-fragment must be greater than --rows-per-fragment")
    return args


def _is_under_tmp(path: Path) -> bool:
    try:
        path.resolve().relative_to(TEMP_ROOT.resolve())
        return True
    except ValueError:
        return False


def _resolve_tmp_uri(uri: Path | None) -> tuple[Path, str | None]:
    if uri is None:
        temp_dir = tempfile.mkdtemp(prefix="lance-compaction-", dir=TEMP_ROOT)
        return Path(temp_dir) / "table.lance", temp_dir

    resolved = uri if uri.is_absolute() else TEMP_ROOT / uri
    resolved = resolved.resolve()
    if not _is_under_tmp(resolved):
        raise SystemExit(
            f"Dataset path must be under {TEMP_ROOT}; got {uri!s}. "
            "Use a relative path or an absolute /tmp path."
        )
    if resolved.exists():
        raise SystemExit(f"Refusing to overwrite existing dataset path: {resolved}")
    return resolved, None


def _make_table(pa: Any, rows: int, bucket_count: int) -> Any:
    ids = list(range(rows))
    buckets = [row_id % bucket_count for row_id in ids]
    payloads = [f"payload-{row_id:06d}" for row_id in ids]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "bucket": pa.array(buckets, type=pa.int64()),
            "payload": pa.array(payloads, type=pa.string()),
        }
    )


def _fragment_summary(dataset: Any) -> dict[str, Any]:
    fragments = dataset.get_fragments()
    metadata = [
        fragment.metadata() if callable(fragment.metadata) else fragment.metadata
        for fragment in fragments
    ]
    return {
        "fragment_count": len(metadata),
        "fragment_ids": [fragment.id for fragment in metadata],
        "logical_rows": sum(fragment.num_rows for fragment in metadata),
        "physical_rows": sum(fragment.physical_rows for fragment in metadata),
        "deleted_rows": sum(fragment.num_deletions for fragment in metadata),
        "fragments_with_deletions": sum(
            1 for fragment in metadata if fragment.num_deletions
        ),
    }


def _assert_table_ids(
    dataset: Any, expected_ids: list[int], *, strict_order: bool
) -> bool:
    table = dataset.to_table(columns=["id"])
    actual_ids = table["id"].to_pylist()
    preserved_order = actual_ids == expected_ids

    if sorted(actual_ids) != expected_ids:
        raise AssertionError(
            "Compacted table IDs do not match expected row set. "
            f"rows={len(actual_ids)} expected_rows={len(expected_ids)}"
        )

    if not strict_order or preserved_order:
        return preserved_order

    if actual_ids != expected_ids:
        first_diff = next(
            (
                idx
                for idx, (actual, expected) in enumerate(zip(actual_ids, expected_ids))
                if actual != expected
            ),
            None,
        )
        raise AssertionError(
            "Compacted table IDs do not match expected insertion order. "
            f"rows={len(actual_ids)} expected_rows={len(expected_ids)} "
            f"first_diff={first_diff} "
            f"actual={actual_ids[first_diff] if first_diff is not None else None} "
            f"expected={expected_ids[first_diff] if first_diff is not None else None}"
        )
    return preserved_order


def _assert_index_filters(
    dataset: Any, expected_ids: list[int], deleted_ids: list[int]
) -> None:
    present_probe_ids = [
        expected_ids[0],
        expected_ids[len(expected_ids) // 2],
        expected_ids[-1],
    ]
    deleted_probe_ids = [
        deleted_ids[0],
        deleted_ids[len(deleted_ids) // 2],
        deleted_ids[-1],
    ]

    for row_id in present_probe_ids:
        count = dataset.count_rows(f"id = {row_id}")
        if count != 1:
            raise AssertionError(
                f"Expected indexed lookup id={row_id} to return 1 row, got {count}"
            )

    for row_id in deleted_probe_ids:
        count = dataset.count_rows(f"id = {row_id}")
        if count != 0:
            raise AssertionError(
                f"Expected deleted id={row_id} to return 0 rows, got {count}"
            )


def _ray_check(uri: Path, expected_count: int) -> dict[str, Any]:
    try:
        import ray
    except ModuleNotFoundError:
        return {"enabled": True, "skipped": True, "reason": "ray is not installed"}

    dataset = ray.data.read_lance(str(uri))
    count = dataset.count()
    if count != expected_count:
        raise AssertionError(
            f"Ray read_lance count mismatch: expected {expected_count}, got {count}"
        )
    return {"enabled": True, "skipped": False, "count": count}


def _cleanup(uri: Path | None, temp_dir: str | None, keep: bool) -> None:
    if keep:
        return
    if temp_dir is not None:
        shutil.rmtree(temp_dir, ignore_errors=True)
    elif uri is not None and uri.exists():
        shutil.rmtree(uri, ignore_errors=True)


def main() -> int:
    args = _parse_args()
    lance, pa = _import_dependencies()

    uri, temp_dir = _resolve_tmp_uri(args.uri)

    expected_ids = [
        row_id
        for row_id in range(args.rows)
        if row_id % args.bucket_count != args.delete_bucket
    ]
    deleted_ids = [
        row_id
        for row_id in range(args.rows)
        if row_id % args.bucket_count == args.delete_bucket
    ]
    if not expected_ids or not deleted_ids:
        raise SystemExit(
            "The generated dataset must contain both retained and deleted rows. "
            "Adjust --rows, --bucket-count, or --delete-bucket."
        )

    try:
        uri.parent.mkdir(parents=True, exist_ok=True)
        table = _make_table(pa, args.rows, args.bucket_count)
        dataset = lance.write_dataset(
            table,
            str(uri),
            max_rows_per_file=args.rows_per_fragment,
        )
        version_after_write = dataset.version

        initial = _fragment_summary(dataset)
        if initial["fragment_count"] < 2:
            raise AssertionError(
                "Initial write did not create multiple fragments. "
                f"summary={initial}"
            )

        if not args.skip_index:
            dataset.create_scalar_index("id", index_type="BTREE", name="id_btree")

        delete_predicate = f"bucket = {args.delete_bucket}"
        delete_stats = dataset.delete(delete_predicate)
        dataset = lance.dataset(str(uri))
        after_delete = _fragment_summary(dataset)

        if dataset.count_rows() != len(expected_ids):
            actual_rows = dataset.count_rows()
            raise AssertionError(
                f"Delete predicate {delete_predicate!r} left {actual_rows} rows, "
                f"expected {len(expected_ids)}"
            )
        if after_delete["deleted_rows"] != len(deleted_ids):
            raise AssertionError(
                f"Lance reports {after_delete['deleted_rows']} deleted rows, "
                f"expected {len(deleted_ids)}"
            )

        compact_options = {
            "target_rows_per_fragment": args.target_rows_per_fragment,
            "materialize_deletions": True,
            "defer_index_remap": args.defer_index_remap,
        }
        if args.compaction_mode is not None:
            compact_options["compaction_mode"] = args.compaction_mode

        version_before_compaction = dataset.version
        compaction_metrics = dataset.optimize.compact_files(**compact_options)
        dataset = lance.dataset(str(uri))
        after_compaction = _fragment_summary(dataset)

        if dataset.version <= version_before_compaction:
            raise AssertionError(
                "Compaction did not create a new dataset version: "
                f"before={version_before_compaction} after={dataset.version}"
            )
        if dataset.count_rows() != len(expected_ids):
            actual_rows = dataset.count_rows()
            raise AssertionError(
                f"Compaction changed logical row count: expected {len(expected_ids)}, "
                f"got {actual_rows}"
            )
        if after_compaction["deleted_rows"] != 0:
            raise AssertionError(
                f"Compaction should have materialized deletions, "
                f"but deleted_rows={after_compaction['deleted_rows']}"
            )
        if after_compaction["fragment_count"] >= after_delete["fragment_count"]:
            raise AssertionError(
                "Compaction did not reduce fragment count. "
                f"before={after_delete['fragment_count']} "
                f"after={after_compaction['fragment_count']}"
            )
        if after_compaction["fragments_with_deletions"] != 0:
            raise AssertionError(
                "Compaction left fragment deletion files behind. "
                f"summary={after_compaction}"
            )

        preserved_order = _assert_table_ids(
            dataset, expected_ids, strict_order=args.strict_order
        )
        if not args.skip_index:
            _assert_index_filters(dataset, expected_ids, deleted_ids)

        cleanup_stats = None
        if args.cleanup_old_versions:
            cleanup_stats = dataset.cleanup_old_versions(retain_versions=1)
            dataset = lance.dataset(str(uri))

        ray_result = None
        if args.ray_check:
            ray_result = _ray_check(uri, len(expected_ids))

        summary = {
            "uri": str(uri),
            "lance_version": getattr(lance, "__version__", "unknown"),
            "rows_written": args.rows,
            "rows_deleted": len(deleted_ids),
            "rows_after_compaction": dataset.count_rows(),
            "preserved_scan_order": preserved_order,
            "delete_predicate": delete_predicate,
            "delete_stats": delete_stats,
            "versions": {
                "after_write": version_after_write,
                "before_compaction": version_before_compaction,
                "after_compaction": dataset.version,
            },
            "fragments": {
                "initial": initial,
                "after_delete": after_delete,
                "after_compaction": after_compaction,
            },
            "compaction_options": compact_options,
            "compaction_metrics": {
                "fragments_removed": compaction_metrics.fragments_removed,
                "fragments_added": compaction_metrics.fragments_added,
                "files_removed": compaction_metrics.files_removed,
                "files_added": compaction_metrics.files_added,
            },
            "cleanup_old_versions": (
                None
                if cleanup_stats is None
                else {
                    "bytes_removed": cleanup_stats.bytes_removed,
                    "old_versions": cleanup_stats.old_versions,
                }
            ),
            "ray_check": ray_result,
            "kept": args.keep,
        }

        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        print("Lance compaction procedure check passed.")
        return 0
    finally:
        _cleanup(uri, temp_dir, args.keep)


if __name__ == "__main__":
    os.environ.setdefault("RUST_BACKTRACE", "1")
    raise SystemExit(main())
