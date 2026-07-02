"""Filesystem-driven tests for the v6 L2 inspection + residency tooling.

Builds a fake Lance v6 L2 layout under a tempdir and exercises
``l2_inspect`` and ``check_l2_residency`` against it. Also pins the
driver-side eligibility rule so a future tightening of the gate must
update an assertion here.

These tests do not need Ray; the ``ray`` import in
``check_l2_residency`` is stubbed before module load so the import is
side-effect-free.
"""

from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


# Stub Ray before importing check_l2_residency: the library entry points
# do not need a live cluster (the actor-RPC path is exercised separately
# in real-cluster runs), and the production module imports ``ray`` at
# top-level. The stub mirrors the surface other benchmark tests expect
# (``ray.remote``, ``ray._private.worker._global_node``) so the same
# stub serves any test order.
if "ray" not in sys.modules:
    _fake_ray = types.ModuleType("ray")

    def _fake_remote(*a, **kw):
        if len(a) == 1 and callable(a[0]):
            return a[0]
        return lambda c: c

    _fake_ray.remote = _fake_remote
    _fake_ray.init = lambda *a, **k: None
    _fake_ray.get = lambda _x: []
    _fake_ray.get_actor = lambda *a, **k: None
    _fake_ray._private = types.SimpleNamespace(
        worker=types.SimpleNamespace(_global_node=None)
    )
    sys.modules["ray"] = _fake_ray


import l2_inspect  # noqa: E402
from check_l2_residency import (  # noqa: E402
    L1_SIZE_UNKNOWN,
    compute_l2_residency,
    walk_l2_partition_ids,
)
from scenarios import is_eligible_for_residency_probe  # noqa: E402


def _build_v6_layout(
    root: Path,
    *,
    prefix: str = "dataset_index",
    partition_ids=(0, 2, 4, 6),
    bytes_per_partition: int = 1024,
    lock: bool = True,
    manifest: bool = True,
    tombstones_bytes: int = 0,
    deleting_partition_ids=(),
) -> None:
    """Create a minimal Lance v6 L2 layout under ``root``.

    Pass ``deleting_partition_ids`` to drop a
    ``.{prefix}.deleting-{nonce}/`` background-removal sentinel with
    those ids inside; the residency walk must skip them and the
    snapshot must classify them as ``deleting=True``.
    """
    root.mkdir(parents=True, exist_ok=True)
    if lock:
        (root / l2_inspect.LOCK_FILENAME).write_bytes(b"\x00" * 4)
    v1 = root / l2_inspect.V1_SUBDIR
    v1.mkdir(exist_ok=True)
    if manifest:
        (v1 / l2_inspect.MANIFEST_FILENAME).write_text("{}")
    if tombstones_bytes > 0:
        (v1 / l2_inspect.TOMBSTONES_FILENAME).write_bytes(b"T" * tombstones_bytes)
    pfx = v1 / prefix
    pfx.mkdir(exist_ok=True)
    for pid in partition_ids:
        (pfx / f"part-ivf-{pid}.bin").write_bytes(b"A" * bytes_per_partition)
    if deleting_partition_ids:
        deleting = v1 / f".{prefix}.deleting-abc123"
        deleting.mkdir(exist_ok=True)
        for pid in deleting_partition_ids:
            (deleting / f"part-ivf-{pid}.bin").write_bytes(b"B" * 512)


class L2InspectSnapshotTest(unittest.TestCase):
    def test_missing_dir_returns_exists_false_shell(self):
        with tempfile.TemporaryDirectory() as td:
            snap = l2_inspect.snapshot_l2_dir(str(Path(td) / "does-not-exist"))
            self.assertFalse(snap["exists"])
            self.assertFalse(snap["lock_present"])
            self.assertFalse(snap["manifest_present"])
            self.assertFalse(snap["tombstones_present"])
            self.assertEqual(snap["prefix_dirs"], [])
            self.assertEqual(snap["file_count"], 0)
            self.assertEqual(snap["files"], [])

    def test_walks_v6_layout(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, partition_ids=(0, 2, 4, 6))
            snap = l2_inspect.snapshot_l2_dir(str(root))

            self.assertTrue(snap["exists"])
            self.assertTrue(snap["lock_present"])
            self.assertTrue(snap["manifest_present"])
            self.assertFalse(snap["tombstones_present"])
            self.assertEqual(snap["tombstones_bytes"], 0)

            self.assertEqual(len(snap["prefix_dirs"]), 1)
            pd = snap["prefix_dirs"][0]
            self.assertEqual(pd["name"], "dataset_index")
            self.assertEqual(pd["file_count"], 4)
            self.assertEqual(pd["apparent_bytes"], 4 * 1024)
            self.assertFalse(pd["deleting"])

            self.assertEqual(snap["file_count"], 4)
            self.assertEqual(snap["apparent_bytes"], 4 * 1024)
            self.assertEqual(len(snap["files"]), 4)

    def test_classifies_deleting_sentinel(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(
                root,
                partition_ids=(0, 2, 4),
                deleting_partition_ids=(99,),
            )
            snap = l2_inspect.snapshot_l2_dir(str(root))
            names = {pd["name"]: pd for pd in snap["prefix_dirs"]}
            self.assertIn("dataset_index", names)
            self.assertFalse(names["dataset_index"]["deleting"])
            deleting_name = next(n for n in names if n.startswith("."))
            self.assertTrue(names[deleting_name]["deleting"])
            # File count aggregates across all prefix subdirs, including
            # the in-flight deleting sentinel: 3 live + 1 deleting = 4.
            self.assertEqual(snap["file_count"], 4)


class ParsePartitionIdTest(unittest.TestCase):
    def test_part_ivf_filename_parses(self):
        self.assertEqual(l2_inspect.parse_partition_id("part-ivf-0.bin"), 0)
        self.assertEqual(l2_inspect.parse_partition_id("part-ivf-12345.bin"), 12345)
        # Path prefix is stripped before matching, so a full relative
        # path also works.
        self.assertEqual(
            l2_inspect.parse_partition_id("v1/dataset_index/part-ivf-7.bin"),
            7,
        )

    def test_non_partition_filenames_yield_none(self):
        self.assertIsNone(l2_inspect.parse_partition_id(".manifest.json"))
        self.assertIsNone(l2_inspect.parse_partition_id(".tombstones.json"))
        self.assertIsNone(l2_inspect.parse_partition_id("part-ivf-.bin"))
        self.assertIsNone(l2_inspect.parse_partition_id("part-ivf-1.dat"))


class DiffSnapshotsTombstoneTest(unittest.TestCase):
    def test_tombstones_added_on_first_creation(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, partition_ids=(0, 2, 4))
            pre = l2_inspect.snapshot_l2_dir(str(root))
            # Now drop a tombstones file mid-experiment.
            (root / "v1" / l2_inspect.TOMBSTONES_FILENAME).write_text("{}")
            post = l2_inspect.snapshot_l2_dir(str(root))
            delta = l2_inspect.diff_snapshots(pre, post)
            self.assertTrue(delta["tombstones_added"])

    def test_tombstones_added_false_when_stable(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(
                root,
                partition_ids=(0, 2, 4),
                tombstones_bytes=100,
            )
            pre = l2_inspect.snapshot_l2_dir(str(root))
            post = l2_inspect.snapshot_l2_dir(str(root))
            delta = l2_inspect.diff_snapshots(pre, post)
            self.assertFalse(delta["tombstones_added"])
            self.assertEqual(delta["file_count_delta"], 0)
            self.assertEqual(delta["apparent_bytes_delta"], 0)


class WalkL2PartitionIdsTest(unittest.TestCase):
    def test_empty_when_dir_missing(self):
        with tempfile.TemporaryDirectory() as td:
            ids, count, abytes, prefixes = walk_l2_partition_ids(
                str(Path(td) / "nope")
            )
            self.assertEqual(ids, [])
            self.assertEqual(count, 0)
            self.assertEqual(abytes, 0)
            self.assertEqual(prefixes, [])

    def test_single_live_prefix_is_walked(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, prefix="dataset_idx", partition_ids=(0, 2, 4))
            ids, count, abytes, prefixes = walk_l2_partition_ids(str(root))
            self.assertEqual(ids, [0, 2, 4])
            self.assertEqual(count, 3)
            self.assertEqual(abytes, 3 * 1024)
            self.assertEqual(prefixes, ["dataset_idx"])

    def test_skips_deleting_sentinel(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(
                root,
                partition_ids=(0, 2, 4, 6),
                deleting_partition_ids=(99, 100),
            )
            ids, count, abytes, prefixes = walk_l2_partition_ids(str(root))
            # Only the four live partitions; the deleting sentinel's
            # files are ignored (they are being torn down) and it is
            # not listed in ``prefixes``.
            self.assertEqual(ids, [0, 2, 4, 6])
            self.assertEqual(count, 4)
            self.assertEqual(abytes, 4 * 1024)
            self.assertEqual(prefixes, ["dataset_index"])

    def test_multiple_live_prefixes_refuse_residency_claim(self):
        # Stale-prefix masking regression: when two unrelated prefix dirs
        # both contain part-ivf-*.bin files, the walk MUST NOT union
        # them. Otherwise a stale prefix with the right partition ids
        # could make the current (possibly empty) prefix look healthy.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, prefix="dataset_a", partition_ids=(0, 2))
            v1 = root / l2_inspect.V1_SUBDIR
            pfx_b = v1 / "dataset_b"
            pfx_b.mkdir()
            for pid in (2, 4):
                (pfx_b / f"part-ivf-{pid}.bin").write_bytes(b"X" * 256)
            ids, count, abytes, prefixes = walk_l2_partition_ids(str(root))
            # No residency claim; both live prefixes are surfaced for
            # the operator to investigate.
            self.assertEqual(ids, [])
            self.assertEqual(count, 0)
            self.assertEqual(abytes, 0)
            self.assertEqual(prefixes, ["dataset_a", "dataset_b"])

    def test_active_prefix_scopes_walk(self):
        # Caller can disambiguate by passing ``active_prefix``: the walk
        # ignores the other live prefix dir entirely.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, prefix="dataset_a", partition_ids=(0, 2))
            v1 = root / l2_inspect.V1_SUBDIR
            pfx_b = v1 / "dataset_b"
            pfx_b.mkdir()
            for pid in (10, 20):
                (pfx_b / f"part-ivf-{pid}.bin").write_bytes(b"X" * 512)
            ids, count, _abytes, prefixes = walk_l2_partition_ids(
                str(root), active_prefix="dataset_b"
            )
            self.assertEqual(ids, [10, 20])
            self.assertEqual(count, 2)
            # ``prefixes`` still lists every live prefix dir so callers
            # can detect a stale neighbor even when scoping the walk.
            self.assertEqual(prefixes, ["dataset_a", "dataset_b"])


class ComputeL2ResidencyTest(unittest.TestCase):
    def test_owned_present_and_missing(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, partition_ids=(0, 2, 4, 6))
            r = compute_l2_residency(
                actor_id=0,
                label="post-prewarm",
                owned_partitions=[0, 2, 4, 6, 8],  # 8 is missing on disk
                l2_dir=str(root),
                l1_size_bytes=99,
            )
            self.assertEqual(r["actor_id"], 0)
            self.assertEqual(r["label"], "post-prewarm")
            self.assertEqual(r["owned_count"], 5)
            self.assertEqual(r["in_l2"], [0, 2, 4, 6])
            self.assertEqual(r["missing"], [8])
            self.assertEqual(r["l2_file_count"], 4)
            self.assertEqual(r["l2_size_bytes_total"], 4 * 1024)
            self.assertEqual(r["l2_prefix_dirs"], ["dataset_index"])
            self.assertEqual(r["l1_size_bytes_at_probe"], 99)
            self.assertGreaterEqual(r["probe_duration_s"], 0.0)

    def test_no_l2_dir_reports_all_missing(self):
        # moka / no-cache scenarios pass l2_dir=None; the row keeps shape
        # but every owned partition is "missing" by definition.
        r = compute_l2_residency(
            actor_id=2,
            label="post-measure",
            owned_partitions=[0, 1, 2],
            l2_dir=None,
            l1_size_bytes=0,
        )
        self.assertEqual(r["in_l2"], [])
        self.assertEqual(r["missing"], [0, 1, 2])
        self.assertEqual(r["l2_file_count"], 0)
        self.assertEqual(r["l2_size_bytes_total"], 0)
        self.assertEqual(r["l2_prefix_dirs"], [])
        self.assertEqual(r["l1_size_bytes_at_probe"], 0)

    def test_stale_prefix_does_not_mask_empty_current_prefix(self):
        # Regression: an old prefix dir holding the owned partition ids
        # must not make the report look healthy when the current prefix
        # (unknown to the caller without ``active_prefix``) is empty or
        # different. Two live prefixes -> walk refuses the claim and
        # everything lands in ``missing``.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, prefix="stale_idx", partition_ids=(0, 2, 4))
            # Drop a second, currently-empty live prefix (the "real"
            # session dir) -- e.g. the new session opened the dataset
            # but hasn't yet prewarmed.
            (root / l2_inspect.V1_SUBDIR / "current_idx").mkdir()
            r = compute_l2_residency(
                actor_id=0,
                label="post-prewarm",
                owned_partitions=[0, 2, 4],
                l2_dir=str(root),
                l1_size_bytes=0,
            )
            self.assertEqual(r["in_l2"], [])
            self.assertEqual(r["missing"], [0, 2, 4])
            self.assertEqual(r["l2_file_count"], 0)
            self.assertEqual(
                sorted(r["l2_prefix_dirs"]),
                ["current_idx", "stale_idx"],
            )

    def test_active_prefix_resolves_ambiguity(self):
        # When the caller knows which prefix is "the right one", passing
        # ``active_prefix`` scopes the walk and bypasses the refuse-on-
        # ambiguity guard.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "actor-0"
            _build_v6_layout(root, prefix="stale_idx", partition_ids=(0, 2, 4))
            current = root / l2_inspect.V1_SUBDIR / "current_idx"
            current.mkdir()
            for pid in (10, 20):
                (current / f"part-ivf-{pid}.bin").write_bytes(b"X" * 256)
            r = compute_l2_residency(
                actor_id=0,
                label="post-prewarm",
                owned_partitions=[10, 20, 30],
                l2_dir=str(root),
                l1_size_bytes=0,
                active_prefix="current_idx",
            )
            self.assertEqual(r["in_l2"], [10, 20])
            self.assertEqual(r["missing"], [30])
            self.assertEqual(r["l2_file_count"], 2)

    def test_l1_size_unknown_is_sentinel(self):
        r = compute_l2_residency(
            actor_id=0,
            label="adhoc",
            owned_partitions=[0],
            l2_dir=None,
            l1_size_bytes=L1_SIZE_UNKNOWN,
        )
        self.assertEqual(r["l1_size_bytes_at_probe"], -1)


class ResidencyProbeEligibilityTest(unittest.TestCase):
    """Pin the driver's residency-probe gating.

    The v6 aggregate-only probe was added so the distributed scenario
    has a residency check. If a future refactor accidentally re-excludes
    ``distributed`` (as the original v6 port did), these assertions
    fail.
    """

    def test_distributed_forced_is_eligible(self):
        self.assertTrue(is_eligible_for_residency_probe("distributed", "forced"))

    def test_distributed_sharded_is_eligible(self):
        self.assertTrue(is_eligible_for_residency_probe("distributed", "sharded"))

    def test_moka_sharded_is_eligible(self):
        self.assertTrue(is_eligible_for_residency_probe("moka", "sharded"))

    def test_no_cache_is_never_eligible(self):
        for prewarm in ("forced", "sharded", "natural", "none"):
            self.assertFalse(
                is_eligible_for_residency_probe("no-cache", prewarm),
                f"no-cache should not be eligible under prewarm={prewarm!r}",
            )

    def test_undefined_prewarm_modes_are_not_eligible(self):
        for prewarm in ("natural", "none"):
            self.assertFalse(
                is_eligible_for_residency_probe("distributed", prewarm),
                f"distributed should not be eligible under prewarm={prewarm!r}",
            )


if __name__ == "__main__":
    unittest.main()
