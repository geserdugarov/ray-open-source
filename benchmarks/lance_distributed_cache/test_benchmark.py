from __future__ import annotations

import argparse
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmark_lib import (
    MIB,
    DatasetSpec,
    SessionSpec,
    aggregate_phase,
    build_session,
    parse_int_list,
    parse_partition_selection,
    resolve_index_addresses,
    validate_result_digests,
)
from run_benchmark import (
    CacheWorker,
    _measure_phase,
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

    def test_memory(self):
        session = build_session(
            SessionSpec("memory", 64 * MIB, 128 * MIB, 192 * MIB), FAKE_LANCE
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

    def test_page_eviction_requires_restart_phase(self):
        args = parse_args(["--drop-l2-page-cache-on-restart", "--no-measure-restart"])
        with self.assertRaisesRegex(ValueError, "requires --measure-restart"):
            _validate_args(args)

    def test_cli_rejects_empty_actor_work_and_excessive_k(self):
        for arguments, message in (
            (["--measure-queries", "1", "--num-actors", "2"], "--num-actors"),
            (["--scale", "5", "--k-list", "10"], "--scale"),
        ):
            with self.subTest(arguments=arguments):
                with self.assertRaisesRegex(ValueError, message):
                    _validate_args(parse_args(arguments))


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
                "index_cache_weight_bytes": 10,
                "source_read_bytes": 1024,
                "source_read_iops": 2,
                "process_disk_read_bytes": 128,
                "l2_snapshot": {"cache_entry_count": 2, "apparent_bytes": 20},
            },
            {
                "latencies_by_k": {"10": [0.3]},
                "session_size_bytes": 200,
                "index_cache_weight_bytes": 20,
                "source_read_bytes": 2048,
                "source_read_iops": 3,
                "process_disk_read_bytes": 256,
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
        self.assertEqual(row["index_cache_weight_bytes"], 30)
        self.assertEqual(row["l2_cache_entry_count"], 5)
        self.assertEqual(row["source_read_bytes"], 3072)
        self.assertEqual(row["source_read_iops"], 5)
        self.assertEqual(row["process_disk_read_bytes"], 384)

    def test_each_k_has_an_independent_wall_time(self):
        calls = []
        clock = [0.0]

        def measure(phase, queries, k_values):
            calls.append(k_values)
            clock[0] += 1 if k_values[0] == 10 else 2
            return {
                "latencies_by_k": {str(k_values[0]): [0.1, 0.2]},
            }

        def stats():
            # Deliberately slower than the query pass; this must not affect QPS.
            clock[0] += 100
            return {"session_size_bytes": 0, "index_cache_weight_bytes": 10}

        actor = types.SimpleNamespace(
            measure=types.SimpleNamespace(remote=measure),
            measurement_stats=types.SimpleNamespace(remote=stats),
        )
        with patch("run_benchmark.time.perf_counter", side_effect=lambda: clock[0]):
            _, rows = _measure_phase(
                ray_module=types.SimpleNamespace(get=lambda values: values),
                actors=[actor, actor],
                scenario="memory",
                phase="initial",
                query_chunks=[[], []],
                k_values=[10, 100],
            )
        self.assertEqual(calls, [[10], [10], [100], [100]])
        self.assertEqual([row["wall_time_s"] for row in rows], [2, 4])
        self.assertEqual([row["throughput_qps"] for row in rows], [2, 1])
        self.assertEqual([row["index_cache_weight_bytes"] for row in rows], [20, 20])
        self.assertIsNone(rows[0]["process_disk_read_bytes"])


class WorkerDiskIoTest(unittest.TestCase):
    def test_disk_counter_and_unavailable_platform(self):
        with patch(
            "run_benchmark.Path.read_text", return_value="rchar: 99\nread_bytes: 8192\n"
        ):
            self.assertEqual(CacheWorker._disk_read_bytes(), 8192)
        with patch("run_benchmark.Path.read_text", side_effect=FileNotFoundError):
            self.assertIsNone(CacheWorker._disk_read_bytes())

    def test_evicts_only_persisted_entries_after_releasing_session(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            namespace = Path(temp_dir) / "blake3-128-v1" / "v1" / ("ab" * 32)
            namespace.mkdir(parents=True)
            entry = namespace / ("01" * 16 + ".bin")
            entry.write_bytes(b"payload")
            worker = CacheWorker.__new__(CacheWorker)
            worker.actor_id = 0
            worker.l2_dir = temp_dir
            worker.dataset = object()
            worker.session = object()

            def advise(fd, offset, length, advice):
                self.assertIsNone(worker.dataset)
                self.assertIsNone(worker.session)
                self.assertGreaterEqual(fd, 0)
                self.assertEqual((offset, length), (0, 0))

            with patch("run_benchmark.os.posix_fadvise", side_effect=advise) as mocked:
                result = worker.close(drop_l2_file_pages=True)
            self.assertEqual(mocked.call_count, 1)
            self.assertEqual(result["l2_page_cache_advised_files"], 1)
            self.assertEqual(result["l2_page_cache_advised_bytes"], 7)
            self.assertEqual(entry.read_bytes(), b"payload")


class ResultValidationTest(unittest.TestCase):
    def test_checks_every_actor_k_and_phase(self):
        first = {
            "event": "measure",
            "actor_id": 0,
            "scenario": "no-index-cache",
            "phase": "initial",
            "row_ids_sha256_by_k": {"10": "first", "100": "second"},
        }
        second = {**first, "scenario": "distributed", "phase": "process_restart"}
        validate_result_digests([first, second])
        for k in ("10", "100"):
            with self.subTest(k=k):
                changed = {**second, "row_ids_sha256_by_k": {k: "different"}}
                with self.assertRaisesRegex(RuntimeError, "query results differ"):
                    validate_result_digests([first, changed])


class SessionSpecFromArgsTest(unittest.TestCase):
    def test_builds_unique_per_actor_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = argparse.Namespace(
                metadata_l1_mb=64,
                partition_l1_mb=128,
                memory_mb=None,
                nvme_dir=temp_dir,
            )
            first = _session_spec(args, "distributed", 0, "run-1")
            second = _session_spec(args, "distributed", 1, "run-1")
        self.assertTrue(Path(first.l2_dir).is_absolute())
        self.assertNotEqual(first.l2_dir, second.l2_dir)
        self.assertEqual(first.memory_capacity_bytes, 192 * MIB)


if __name__ == "__main__":
    unittest.main()
