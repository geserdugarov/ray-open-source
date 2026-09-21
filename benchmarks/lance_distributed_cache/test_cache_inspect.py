from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from cache_inspect import (
    CACHE_KEY_FORMAT,
    L2_FORMAT_VERSION,
    diff_snapshots,
    invalidation_errors,
    snapshot_l2_dir,
)


NAMESPACE = "ab" * 32
ENTRY_1 = "01" * 16 + ".bin"
ENTRY_2 = "02" * 16 + ".bin"


class SnapshotL2DirTest(unittest.TestCase):
    def test_missing_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot = snapshot_l2_dir(Path(temp_dir) / "missing")
        self.assertFalse(snapshot["exists"])
        self.assertEqual(snapshot["cache_entry_count"], 0)

    def test_reads_lance_12_opaque_layout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "lance-distributed.lock").touch()
            version = root / CACHE_KEY_FORMAT / L2_FORMAT_VERSION
            namespace = version / NAMESPACE
            namespace.mkdir(parents=True)
            (namespace / ENTRY_1).write_bytes(b"a" * 10)
            (namespace / ENTRY_2).write_bytes(b"b" * 20)
            (version / ".tombstones.json").write_text("[]")

            snapshot = snapshot_l2_dir(root)

        self.assertTrue(snapshot["lock_present"])
        self.assertTrue(snapshot["key_format_present"])
        self.assertTrue(snapshot["version_present"])
        self.assertEqual(snapshot["namespace_count"], 1)
        self.assertEqual(snapshot["cache_entry_count"], 2)
        self.assertEqual(snapshot["apparent_bytes"], 30)
        self.assertEqual(snapshot["tombstone_count"], 0)
        self.assertFalse(snapshot["tombstone_parse_error"])

    def test_does_not_infer_partition_ids_or_count_invalid_names(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            namespace = root / CACHE_KEY_FORMAT / L2_FORMAT_VERSION / NAMESPACE
            namespace.mkdir(parents=True)
            (namespace / "part-ivf-7.bin").write_bytes(b"legacy")
            (namespace / ENTRY_1).write_bytes(b"current")

            snapshot = snapshot_l2_dir(root)

        self.assertEqual(snapshot["cache_entry_count"], 1)
        self.assertEqual(len(snapshot["invalid_entries"]), 1)
        self.assertIn("part-ivf-7.bin", snapshot["invalid_entries"][0])

    def test_reports_unexpected_version_level_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            version = root / CACHE_KEY_FORMAT / L2_FORMAT_VERSION
            version.mkdir(parents=True)
            (version / "orphan.tmp").write_bytes(b"partial")

            snapshot = snapshot_l2_dir(root)

        self.assertEqual(snapshot["cache_entry_count"], 0)
        self.assertEqual(snapshot["invalid_entries"], ["blake3-128-v1/v1/orphan.tmp"])

    def test_deleting_namespaces_are_not_live_entries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            version = root / CACHE_KEY_FORMAT / L2_FORMAT_VERSION
            deleting = version / f".deleting-{NAMESPACE}-0000000000000001"
            deleting.mkdir(parents=True)
            (deleting / ENTRY_1).write_bytes(b"stale")

            snapshot = snapshot_l2_dir(root)

        self.assertEqual(snapshot["namespace_count"], 0)
        self.assertEqual(snapshot["deleting_dir_count"], 1)
        self.assertEqual(snapshot["cache_entry_count"], 0)

    def test_counts_persistent_tombstones_not_file_presence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            version = root / CACHE_KEY_FORMAT / L2_FORMAT_VERSION
            version.mkdir(parents=True)
            tombstones = version / ".tombstones.json"
            tombstones.write_text("[]")
            empty = snapshot_l2_dir(root)
            tombstones.write_text(json.dumps([NAMESPACE]))
            nonempty = snapshot_l2_dir(root)

        self.assertTrue(empty["tombstones_present"])
        self.assertEqual(empty["tombstone_count"], 0)
        self.assertEqual(nonempty["tombstone_count"], 1)


class SnapshotHelpersTest(unittest.TestCase):
    def test_diff(self):
        before = {
            "cache_entry_count": 1,
            "namespace_count": 1,
            "apparent_bytes": 10,
            "disk_bytes": 4096,
            "tombstone_count": 0,
            "deleting_dir_count": 0,
        }
        after = {
            "cache_entry_count": 3,
            "namespace_count": 2,
            "apparent_bytes": 50,
            "disk_bytes": 12288,
            "tombstone_count": 1,
            "deleting_dir_count": 1,
        }
        self.assertEqual(
            diff_snapshots(before, after),
            {
                "cache_entry_count_delta": 2,
                "namespace_count_delta": 1,
                "apparent_bytes_delta": 40,
                "disk_bytes_delta": 8192,
                "tombstone_count_delta": 1,
                "deleting_dir_count_delta": 1,
            },
        )

    def test_successful_invalidation_allows_empty_tombstone_file(self):
        snapshot = {
            "namespace_count": 0,
            "deleting_dir_count": 0,
            "tombstone_count": 0,
            "tombstone_parse_error": None,
            "invalid_entries": [],
        }
        self.assertEqual(invalidation_errors(snapshot), [])

    def test_invalidation_reports_unsafe_state(self):
        snapshot = {
            "namespace_count": 1,
            "deleting_dir_count": 1,
            "tombstone_count": 1,
            "tombstone_parse_error": None,
            "invalid_entries": ["unexpected"],
        }
        errors = invalidation_errors(snapshot)
        self.assertEqual(len(errors), 4)


if __name__ == "__main__":
    unittest.main()
