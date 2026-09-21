from __future__ import annotations

import argparse
import tempfile
import types
import unittest
from pathlib import Path

from benchmark_lib import (
    MIB,
    DatasetSpec,
    SessionSpec,
    aggregate_phase,
    build_session,
    parse_int_list,
    parse_partition_selection,
    resolve_index_addresses,
)
from run_benchmark import (
    _session_spec,
    _validate_args,
    _validate_lance_api,
    parse_args,
)


class _FakeSession:
    def __init__(self, **kwargs):
        self.factory = "plain"
        self.kwargs = kwargs

    @staticmethod
    def with_distributed_cache(**kwargs):
        session = _FakeSession.__new__(_FakeSession)
        session.factory = "distributed"
        session.kwargs = kwargs
        return session


FAKE_LANCE = types.SimpleNamespace(Session=_FakeSession)


class BuildSessionTest(unittest.TestCase):
    def test_no_index_cache(self):
        session = build_session(
            SessionSpec("no-index-cache", 64 * MIB, 128 * MIB, 192 * MIB),
            FAKE_LANCE,
        )
        self.assertEqual(session.factory, "plain")
        self.assertEqual(session.kwargs["index_cache_size_bytes"], 0)
        self.assertEqual(session.kwargs["metadata_cache_size_bytes"], 64 * MIB)

    def test_moka(self):
        session = build_session(
            SessionSpec("moka", 64 * MIB, 128 * MIB, 192 * MIB), FAKE_LANCE
        )
        self.assertEqual(session.factory, "plain")
        self.assertEqual(session.kwargs["index_cache_size_bytes"], 192 * MIB)

    def test_distributed_uses_lance_12_keyword(self):
        session = build_session(
            SessionSpec(
                "distributed",
                64 * MIB,
                128 * MIB,
                192 * MIB,
                "/mnt/nvme/cache/actor-0",
            ),
            FAKE_LANCE,
        )
        self.assertEqual(session.factory, "distributed")
        self.assertEqual(
            session.kwargs,
            {
                "l2_dir": "/mnt/nvme/cache/actor-0",
                "index_metadata_l1_capacity_bytes": 64 * MIB,
                "l1_partition_capacity_bytes": 128 * MIB,
            },
        )
        self.assertNotIn("moka_l1_partition_bytes", session.kwargs)

    def test_distributed_can_disable_partition_l1(self):
        session = build_session(
            SessionSpec("distributed", 64 * MIB, None, 64 * MIB, "/mnt/nvme/cache"),
            FAKE_LANCE,
        )
        self.assertIsNone(session.kwargs["l1_partition_capacity_bytes"])


class ParsingTest(unittest.TestCase):
    def test_positive_integer_list(self):
        self.assertEqual(parse_int_list("10, 100,1000"), [10, 100, 1000])
        with self.assertRaises(ValueError):
            parse_int_list("10,0")

    def test_partition_selection(self):
        self.assertEqual(parse_partition_selection("all"), [])
        self.assertEqual(parse_partition_selection("3,1,3"), [3, 1])
        with self.assertRaises(ValueError):
            parse_partition_selection("-1")

    def test_cli_defaults_exercise_strict_prewarm_and_restart(self):
        args = parse_args([])
        self.assertEqual(args.prewarm, "strict")
        self.assertTrue(args.measure_restart)
        self.assertEqual(args.prewarm_partitions, "all")

    def test_cli_rejects_partition_list_without_strict_mode(self):
        args = parse_args(["--prewarm", "natural", "--prewarm-partitions", "1"])
        with self.assertRaises(ValueError):
            _validate_args(args)

    def test_invalidation_drill_requires_restart_phase(self):
        args = parse_args(["--invalidation-drill", "--no-measure-restart"])
        with self.assertRaisesRegex(ValueError, "requires --measure-restart"):
            _validate_args(args)


class LanceApiValidationTest(unittest.TestCase):
    def test_accepts_requested_surface(self):
        class Session:
            @staticmethod
            def with_distributed_cache(
                l2_dir,
                index_metadata_l1_capacity_bytes,
                *,
                l1_partition_capacity_bytes=None,
            ):
                pass

            def invalidate_index_cache(self, dataset_uri, index_addr):
                pass

        class Dataset:
            def prewarm_index(self, name, *, partition_ids=None):
                pass

        _validate_lance_api(
            types.SimpleNamespace(Session=Session, LanceDataset=Dataset)
        )

    def test_rejects_old_partition_l1_keyword(self):
        class Session:
            @staticmethod
            def with_distributed_cache(
                l2_dir,
                index_metadata_l1_capacity_bytes,
                *,
                moka_l1_partition_bytes=None,
            ):
                pass

            def invalidate_index_cache(self, dataset_uri, index_addr):
                pass

        class Dataset:
            def prewarm_index(self, name, *, partition_ids=None):
                pass

        with self.assertRaisesRegex(RuntimeError, "l1_partition_capacity_bytes"):
            _validate_lance_api(
                types.SimpleNamespace(Session=Session, LanceDataset=Dataset)
            )


class DatasetSpecTest(unittest.TestCase):
    def test_hash_changes_with_index_shape(self):
        first = DatasetSpec(100, 16, 4, 2)
        second = DatasetSpec(100, 16, 8, 2)
        self.assertNotEqual(first.content_hash(), second.content_hash())

    def test_ivf_rq_dimension_validation(self):
        with self.assertRaisesRegex(ValueError, "divisible by 8"):
            DatasetSpec(100, 15, 4, 2).validate()


class ResolveIndexAddressesTest(unittest.TestCase):
    def test_returns_every_physical_segment(self):
        dataset = types.SimpleNamespace(
            describe_indices=lambda: [
                types.SimpleNamespace(
                    name="vector_idx",
                    segments=[
                        types.SimpleNamespace(uuid="first"),
                        types.SimpleNamespace(uuid="second"),
                    ],
                )
            ]
        )
        self.assertEqual(
            resolve_index_addresses(dataset, "vector_idx"), ["first", "second"]
        )

    def test_missing_index_fails_closed(self):
        dataset = types.SimpleNamespace(describe_indices=lambda: [])
        with self.assertRaisesRegex(RuntimeError, "not found"):
            resolve_index_addresses(dataset, "vector_idx")


class AggregatePhaseTest(unittest.TestCase):
    def test_aggregates_actor_latencies_and_cache_sizes(self):
        results = [
            {
                "latencies_by_k": {"10": [0.1, 0.2]},
                "session_size_bytes": 100,
                "l2_snapshot": {"cache_entry_count": 2, "apparent_bytes": 20},
            },
            {
                "latencies_by_k": {"10": [0.3]},
                "session_size_bytes": 200,
                "l2_snapshot": {"cache_entry_count": 3, "apparent_bytes": 30},
            },
        ]
        row = aggregate_phase(
            scenario="distributed",
            phase="initial",
            actor_results=results,
            k_values=[10],
            wall_time_s=0.5,
        )[0]
        self.assertEqual(row["n"], 3)
        self.assertEqual(row["throughput_qps"], 6.0)
        self.assertEqual(row["session_size_bytes"], 300)
        self.assertEqual(row["l2_cache_entry_count"], 5)


class SessionSpecFromArgsTest(unittest.TestCase):
    def test_builds_unique_per_actor_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = argparse.Namespace(
                metadata_l1_mb=64,
                partition_l1_mb=128,
                moka_mb=None,
                nvme_dir=temp_dir,
            )
            first = _session_spec(args, "distributed", 0, "run-1")
            second = _session_spec(args, "distributed", 1, "run-1")
        self.assertTrue(Path(first.l2_dir).is_absolute())
        self.assertNotEqual(first.l2_dir, second.l2_dir)
        self.assertEqual(first.moka_capacity_bytes, 192 * MIB)


if __name__ == "__main__":
    unittest.main()
