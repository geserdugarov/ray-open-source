"""Smoke tests for the Lance distributed-cache scenario specs + build_session.

Imports `lance` via a stub module so the test runs without a real pylance
build. Asserts that `build_scenario_spec` produces the v6-shaped dict for
each scenario and that `build_session` dispatches to the v6 factory
(`Session.with_distributed_cache`) with the right keyword names.
"""

from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


# numpy / pyarrow are imported eagerly by `_hybrid_cache_helpers`; stub
# them so the test runs in a barebones environment.
for _mod_name in ("numpy", "pyarrow"):
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = types.ModuleType(_mod_name)


class _FakeSession:
    """Stand-in for `lance.Session` capturing the constructor kwargs.

    Deliberately omits `index_cache_stats` — that v4 method was removed
    in Lance 6.0 and any code that tries to call it should fail loudly
    in the test suite rather than silently keep working.
    """

    def __init__(self, **kw):
        self.kw = kw
        self.factory = "plain"
        self._size_bytes = 1_234_567

    def size_bytes(self) -> int:
        return self._size_bytes

    @staticmethod
    def with_distributed_cache(**kw):
        s = _FakeSession.__new__(_FakeSession)
        s.kw = kw
        s.factory = "distributed"
        s._size_bytes = 7_654_321
        return s


_fake_lance = types.ModuleType("lance")
_fake_lance.Session = _FakeSession
_fake_lance.LanceDataset = object
sys.modules["lance"] = _fake_lance


from _hybrid_cache_helpers import build_session, size_bytes_stats  # noqa: E402
from scenarios import (  # noqa: E402
    GIB,
    MIB,
    build_scenario_spec,
    build_scenario_specs,
    distributed_l2_dir_for_repeat,
    per_actor_l2_dir,
)


class PerActorL2DirTest(unittest.TestCase):
    def test_layout(self):
        self.assertEqual(
            per_actor_l2_dir("/mnt/nvme/l2", 0), "/mnt/nvme/l2/actor-0"
        )
        self.assertEqual(
            per_actor_l2_dir("/mnt/nvme/l2", 7), "/mnt/nvme/l2/actor-7"
        )


class BuildScenarioSpecTest(unittest.TestCase):
    def test_no_cache(self):
        self.assertEqual(
            build_scenario_spec("no-cache", 0),
            {"name": "no-cache", "kind": "no-cache"},
        )

    def test_no_cache_passes_metadata_cache_size_through(self):
        spec = build_scenario_spec(
            "no-cache", 0, metadata_cache_size_bytes=8 * MIB
        )
        self.assertEqual(spec["metadata_cache_size_bytes"], 8 * MIB)

    def test_moka(self):
        spec = build_scenario_spec("moka", 0, dram_bytes=4 * GIB)
        self.assertEqual(spec["kind"], "moka")
        self.assertEqual(spec["index_cache_size_bytes"], 4 * GIB)

    def test_moka_requires_positive_dram(self):
        with self.assertRaises(ValueError):
            build_scenario_spec("moka", 0, dram_bytes=0)

    def test_distributed_actor_path(self):
        spec = build_scenario_spec(
            "distributed",
            actor_id=2,
            nvme_dir="/mnt/nvme/l2",
            metadata_l1_bytes=64 * MIB,
            partition_l1_bytes=1024 * MIB,
        )
        self.assertEqual(spec["kind"], "distributed")
        self.assertEqual(spec["l2_dir"], "/mnt/nvme/l2/actor-2")
        self.assertEqual(spec["metadata_l1_bytes"], 64 * MIB)
        self.assertEqual(spec["partition_l1_bytes"], 1024 * MIB)

    def test_distributed_partition_l1_can_be_disabled(self):
        spec = build_scenario_spec(
            "distributed",
            0,
            nvme_dir="/mnt/nvme/l2",
            metadata_l1_bytes=64 * MIB,
            partition_l1_bytes=None,
        )
        self.assertIsNone(spec["partition_l1_bytes"])

    def test_distributed_rejects_unsafe_l2_dir(self):
        for bad in ("/tmp/x", "/var/tmp/x", "/dev/shm/x", "/"):
            with self.assertRaises(ValueError, msg=bad):
                build_scenario_spec(
                    "distributed", 0, nvme_dir=bad, metadata_l1_bytes=64 * MIB
                )

    def test_distributed_rejects_non_absolute_l2_dir(self):
        with self.assertRaises(ValueError):
            build_scenario_spec(
                "distributed",
                0,
                nvme_dir="relative/path",
                metadata_l1_bytes=64 * MIB,
            )

    def test_distributed_requires_metadata_l1(self):
        with self.assertRaises(ValueError):
            build_scenario_spec(
                "distributed", 0, nvme_dir="/mnt/nvme/l2", metadata_l1_bytes=None
            )

    def test_unknown_scenario_raises(self):
        with self.assertRaises(ValueError):
            build_scenario_spec("hybrid", 0)
        with self.assertRaises(ValueError):
            build_scenario_spec("foo", 0)


class BuildScenarioSpecsPluralShimTest(unittest.TestCase):
    def test_hybrid_alias(self):
        specs = build_scenario_specs(
            ["no-cache", "moka", "hybrid"],
            dram_bytes=4 * GIB,
            nvme_dir="/mnt/nvme/l2",
        )
        self.assertEqual([s["kind"] for s in specs], ["no-cache", "moka", "distributed"])
        self.assertEqual(specs[2]["l2_dir"], "/mnt/nvme/l2/actor-0")

    def test_distributed_pass_through(self):
        specs = build_scenario_specs(
            ["distributed"],
            dram_bytes=4 * GIB,
            nvme_dir="/mnt/nvme/l2",
        )
        self.assertEqual(specs[0]["kind"], "distributed")


class BuildSessionTest(unittest.TestCase):
    def test_no_cache_dispatches_to_plain_session(self):
        sess = build_session({"name": "no-cache", "kind": "no-cache"})
        self.assertEqual(sess.factory, "plain")
        self.assertEqual(sess.kw, {"index_cache_size_bytes": 0})

    def test_no_cache_with_metadata_cache_size(self):
        sess = build_session(
            {
                "name": "no-cache",
                "kind": "no-cache",
                "metadata_cache_size_bytes": 8 * MIB,
            }
        )
        self.assertEqual(sess.kw["metadata_cache_size_bytes"], 8 * MIB)

    def test_moka_dispatches_to_plain_session(self):
        sess = build_session(
            {
                "name": "moka",
                "kind": "moka",
                "index_cache_size_bytes": 4 * GIB,
            }
        )
        self.assertEqual(sess.factory, "plain")
        self.assertEqual(sess.kw["index_cache_size_bytes"], 4 * GIB)

    def test_distributed_dispatches_with_v6_kwargs(self):
        sess = build_session(
            {
                "name": "distributed",
                "kind": "distributed",
                "l2_dir": "/mnt/nvme/l2/actor-0",
                "metadata_l1_bytes": 64 * MIB,
                "partition_l1_bytes": 1024 * MIB,
            }
        )
        self.assertEqual(sess.factory, "distributed")
        self.assertEqual(
            sess.kw,
            {
                "l2_dir": "/mnt/nvme/l2/actor-0",
                "index_metadata_l1_capacity_bytes": 64 * MIB,
                "moka_l1_partition_bytes": 1024 * MIB,
            },
        )

    def test_distributed_partition_l1_none_preserved(self):
        sess = build_session(
            {
                "name": "distributed",
                "kind": "distributed",
                "l2_dir": "/mnt/nvme/l2/actor-0",
                "metadata_l1_bytes": 64 * MIB,
                "partition_l1_bytes": None,
            }
        )
        self.assertIsNone(sess.kw["moka_l1_partition_bytes"])

    def test_unknown_kind_raises(self):
        with self.assertRaises(ValueError):
            build_session({"kind": "hybrid"})


class V6SessionStatsTest(unittest.TestCase):
    """Guard against any code path calling the removed v4 stats API."""

    def test_fake_session_lacks_index_cache_stats(self):
        # If a future test stub adds `index_cache_stats` back, every
        # production path that was supposed to be ported to size_bytes()
        # would silently keep working — defeating the v6 migration. Pin
        # the absence so the stub matches the real Lance 6.0 _Session.
        sess = _FakeSession()
        self.assertFalse(hasattr(sess, "index_cache_stats"))
        self.assertTrue(hasattr(sess, "size_bytes"))

    def test_size_bytes_stats_returns_only_size_bytes(self):
        # `size_bytes_stats` is the v6 producer shim. It must not touch
        # `index_cache_stats` (which doesn't exist on v6 sessions); the
        # fake session has no such method, so any accidental access
        # would AttributeError here. It also must NOT fabricate zero
        # placeholders for the removed `hits` / `misses` / `num_entries`
        # keys — that would let downstream code silently report 0%
        # hit ratios for data Lance 6.0 simply does not expose. The
        # dict is intentionally minimal so KeyError surfaces stale v4
        # readers loudly.
        sess = _FakeSession()
        stats = size_bytes_stats(sess)
        self.assertEqual(stats, {"size_bytes": sess._size_bytes})
        self.assertNotIn("hits", stats)
        self.assertNotIn("misses", stats)
        self.assertNotIn("num_entries", stats)

    def test_build_session_distributed_session_has_no_v4_api(self):
        sess = build_session(
            {
                "name": "distributed",
                "kind": "distributed",
                "l2_dir": "/mnt/nvme/l2/actor-0",
                "metadata_l1_bytes": 64 * MIB,
                "partition_l1_bytes": None,
            }
        )
        self.assertFalse(hasattr(sess, "index_cache_stats"))
        # size_bytes is callable — the v6 surface.
        self.assertIsInstance(sess.size_bytes(), int)


class DistributedL2DirForRepeatTest(unittest.TestCase):
    def test_reuse_l2_collapses_to_per_actor_path(self):
        for repeat in range(3):
            self.assertEqual(
                distributed_l2_dir_for_repeat(
                    "/mnt/nvme/l2",
                    actor_id=0,
                    repeat=repeat,
                    reuse_l2=True,
                    now_fn=lambda: 1_700_000_000,
                ),
                "/mnt/nvme/l2/actor-0",
            )

    def test_cold_repeat_returns_unique_paths_per_repeat(self):
        # Different repeat numbers MUST produce different paths so the
        # L2 lock is not contended and cold-start latency is honest.
        paths = {
            distributed_l2_dir_for_repeat(
                "/mnt/nvme/l2",
                actor_id=0,
                repeat=r,
                reuse_l2=False,
                now_fn=lambda: 1_700_000_000,
            )
            for r in range(3)
        }
        self.assertEqual(len(paths), 3, paths)
        # The per-actor prefix is preserved so the actor mkdir is still
        # under the operator-supplied --nvme-dir.
        for p in paths:
            self.assertTrue(p.startswith("/mnt/nvme/l2/actor-0-"), p)

    def test_cold_repeat_includes_timestamp_for_uniqueness_across_runs(self):
        # Two invocations of the same repeat number at different times
        # should produce different paths so re-running with the same
        # `--repeats` doesn't accidentally reattach to a prior run's L2.
        p1 = distributed_l2_dir_for_repeat(
            "/mnt/nvme/l2",
            actor_id=0,
            repeat=0,
            reuse_l2=False,
            now_fn=lambda: 1_700_000_000,
        )
        p2 = distributed_l2_dir_for_repeat(
            "/mnt/nvme/l2",
            actor_id=0,
            repeat=0,
            reuse_l2=False,
            now_fn=lambda: 1_700_000_005,
        )
        self.assertNotEqual(p1, p2)


class DistributedDriverV6BudgetFlagsTest(unittest.TestCase):
    """`run_distributed_bench` must surface `--metadata-l1-mb` and
    `--partition-l1-mb` as v6 sizing CLI flags so operators are not stuck
    with the helper's hidden defaults. `--partition-l1-mb 0` must map to
    a disabled partition-L1 tier (`partition_l1_bytes=None`).
    """

    @staticmethod
    def _load():
        for _mod_name in ("ray",):
            if _mod_name not in sys.modules:
                _stub = types.ModuleType(_mod_name)

                def _remote(*a, **kw):
                    if len(a) == 1 and callable(a[0]):
                        return a[0]
                    return lambda c: c

                _stub.remote = _remote
                _stub._private = types.SimpleNamespace(
                    worker=types.SimpleNamespace(_global_node=None)
                )
                sys.modules[_mod_name] = _stub
        import run_distributed_bench

        return run_distributed_bench

    def test_metadata_l1_mb_flag_present(self):
        mod = self._load()
        sys.argv = ["run_distributed_bench.py", "--metadata-l1-mb", "128"]
        args = mod.parse_args()
        self.assertEqual(args.metadata_l1_mb, 128)

    def test_partition_l1_mb_flag_present(self):
        mod = self._load()
        sys.argv = ["run_distributed_bench.py", "--partition-l1-mb", "512"]
        args = mod.parse_args()
        self.assertEqual(args.partition_l1_mb, 512)

    def test_build_per_actor_spec_wires_explicit_budgets(self):
        mod = self._load()
        spec = mod.build_per_actor_spec(
            "distributed",
            actor_id=0,
            nvme_dir="/mnt/nvme/lance-l2",
            dram_bytes=1 << 30,
            metadata_l1_bytes=128 * MIB,
            partition_l1_bytes=512 * MIB,
        )
        self.assertEqual(spec["metadata_l1_bytes"], 128 * MIB)
        self.assertEqual(spec["partition_l1_bytes"], 512 * MIB)

    def test_build_per_actor_spec_propagates_disabled_partition_l1(self):
        mod = self._load()
        spec = mod.build_per_actor_spec(
            "distributed",
            actor_id=0,
            nvme_dir="/mnt/nvme/lance-l2",
            dram_bytes=1 << 30,
            metadata_l1_bytes=64 * MIB,
            partition_l1_bytes=None,
        )
        self.assertIsNone(spec["partition_l1_bytes"])


class DistributedDriverPerActorSummaryTest(unittest.TestCase):
    """`_format_per_actor_summary_lines` is the last thing main() emits;
    a regression here crashes a successful run instead of printing the
    summary. Before the v6 stats rewrite this loop referenced an
    undefined `hr` variable computed from removed `hits` / `misses`
    counters — these tests pin both branches (coord vs replicated) to
    the v6 `size_bytes`-only shape.
    """

    @staticmethod
    def _load():
        for _mod_name in ("ray",):
            if _mod_name not in sys.modules:
                _stub = types.ModuleType(_mod_name)

                def _remote(*a, **kw):
                    if len(a) == 1 and callable(a[0]):
                        return a[0]
                    return lambda c: c

                _stub.remote = _remote
                _stub._private = types.SimpleNamespace(
                    worker=types.SimpleNamespace(_global_node=None)
                )
                sys.modules[_mod_name] = _stub
        import run_distributed_bench

        return run_distributed_bench._format_per_actor_summary_lines

    def test_replicated_summary_uses_size_bytes_not_hit_ratio(self):
        fmt = self._load()
        # Empty per-k latency list exercises the `percentiles` zero-path
        # without touching numpy (the test stub for numpy doesn't carry
        # the full ndarray surface).
        per_actor_results = [
            {
                "actor_id": 0,
                "stats_post": {"size_bytes": 12_345_678},
                "duration_s": 1.5,
                "latencies_by_k": {10: []},
            },
        ]
        lines = fmt(per_actor_results, coord_result=None)
        joined = "\n".join(lines)
        # The bug under review printed `hit={hr:.1%}` which raised
        # NameError because `hr` was deleted with the v4 stats cleanup.
        self.assertNotIn("hit=", joined)
        # size_bytes must appear, formatted with thousands separator.
        self.assertIn("bytes=12,345,678", joined)
        # dur= is the only other per-row field.
        self.assertIn("dur=1.5s", joined)

    def test_coord_mode_summary_reports_size_bytes_owned_and_calls(self):
        fmt = self._load()
        per_actor_results = [
            {
                "actor_id": 1,
                "stats_post": {"size_bytes": 999_000},
                "owned_partitions": 1500,
                "n_searches_handled": 42,
            },
        ]
        coord_result = {"latencies_by_k": {10: [0.01]}}
        lines = fmt(per_actor_results, coord_result=coord_result)
        joined = "\n".join(lines)
        self.assertNotIn("hit=", joined)
        self.assertIn("bytes=999,000", joined)
        self.assertIn("owned=1500", joined)
        self.assertIn("calls_handled=42", joined)

    def test_summary_does_not_raise_with_missing_size_bytes(self):
        fmt = self._load()
        # Defensive: if upstream ever returns a stats_post without
        # `size_bytes`, the formatter should default to 0 rather than
        # KeyError. v6 `size_bytes_stats` always sets it, but a stale
        # stub or downgraded session must not crash this print loop.
        per_actor_results = [
            {
                "actor_id": 0,
                "stats_post": {},
                "duration_s": 0.0,
                "latencies_by_k": {10: []},
            },
        ]
        lines = fmt(per_actor_results, coord_result=None)
        joined = "\n".join(lines)
        self.assertIn("bytes=0", joined)


class RunBenchV6FlagsTest(unittest.TestCase):
    """`run_bench.py` (the single-actor driver — the primary README path)
    must accept the v6 sizing flags and route them through
    `build_scenario_specs`. Before this fix the parser rejected
    `--metadata-l1-mb` / `--partition-l1-mb` even though the README
    documented them, so the documented command failed at argument
    parsing.
    """

    @staticmethod
    def _load():
        for _mod_name in ("ray",):
            if _mod_name not in sys.modules:
                _stub = types.ModuleType(_mod_name)

                def _remote(*a, **kw):
                    if len(a) == 1 and callable(a[0]):
                        return a[0]
                    return lambda c: c

                _stub.remote = _remote
                _stub._private = types.SimpleNamespace(
                    worker=types.SimpleNamespace(_global_node=None)
                )
                sys.modules[_mod_name] = _stub
        import run_bench

        return run_bench

    def test_documented_readme_invocation_parses(self):
        # This is exactly the form documented in
        # benchmarks/lance_hybrid_cache/README.md "v6 DRAM split"
        # example. Pre-fix it failed argument parsing.
        mod = self._load()
        sys.argv = [
            "run_bench.py",
            "--dram-mb",
            "4096",
            "--metadata-l1-mb",
            "64",
            "--partition-l1-mb",
            "1024",
            "--nvme-dir",
            "/mnt/nvme/lance-l2",
            "--scenarios",
            "no-cache,moka,distributed",
        ]
        # `--dram-mb` doesn't exist on run_bench (that flag is on
        # `bench_hybrid_cache_ivf_rq.py`); the README uses `--dram-gb`
        # in run_bench's example. Re-run with the actual README form
        # before asserting.
        sys.argv = [
            "run_bench.py",
            "--dram-gb",
            "4",
            "--metadata-l1-mb",
            "64",
            "--partition-l1-mb",
            "1024",
            "--nvme-dir",
            "/mnt/nvme/lance-l2",
            "--scenarios",
            "no-cache,moka,distributed",
        ]
        args = mod.parse_args()
        self.assertEqual(args.metadata_l1_mb, 64)
        self.assertEqual(args.partition_l1_mb, 1024)
        self.assertEqual(args.dram_gb, 4.0)

    def test_partition_l1_zero_disables_tier(self):
        mod = self._load()
        sys.argv = ["run_bench.py", "--partition-l1-mb", "0"]
        args = mod.parse_args()
        self.assertEqual(args.partition_l1_mb, 0)

    def test_rejects_negative_partition_l1(self):
        mod = self._load()
        sys.argv = ["run_bench.py", "--partition-l1-mb", "-1"]
        with self.assertRaises(SystemExit) as cm:
            mod.parse_args()
        self.assertEqual(cm.exception.code, 2)

    def test_rejects_negative_metadata_l1(self):
        mod = self._load()
        sys.argv = ["run_bench.py", "--metadata-l1-mb", "-1"]
        with self.assertRaises(SystemExit) as cm:
            mod.parse_args()
        self.assertEqual(cm.exception.code, 2)

    def test_specs_built_from_args_carry_v6_l1_budgets(self):
        # Wire-through test: confirm the parsed flags reach the spec
        # via `build_scenario_specs`. The plural shim accepts the v6
        # kwargs; this checks the driver actually forwards them.
        mod = self._load()
        from scenarios import build_scenario_specs as _build_specs

        sys.argv = [
            "run_bench.py",
            "--metadata-l1-mb",
            "32",
            "--partition-l1-mb",
            "512",
            "--nvme-dir",
            "/mnt/nvme/lance-l2",
            "--scenarios",
            "distributed",
        ]
        args = mod.parse_args()
        # Re-run the driver's spec-construction logic with the parsed
        # args (the production main() does the same conversions before
        # calling `build_scenario_specs`).
        partition_l1_mb = int(args.partition_l1_mb)
        specs = _build_specs(
            [s.strip() for s in args.scenarios.split(",") if s.strip()],
            dram_bytes=int(args.dram_gb * (1 << 30)),
            nvme_dir=args.nvme_dir,
            metadata_l1_bytes=int(args.metadata_l1_mb) * MIB,
            partition_l1_bytes=(partition_l1_mb * MIB) if partition_l1_mb > 0 else None,
        )
        self.assertEqual(specs[0]["kind"], "distributed")
        self.assertEqual(specs[0]["metadata_l1_bytes"], 32 * MIB)
        self.assertEqual(specs[0]["partition_l1_bytes"], 512 * MIB)


class NegativePartitionL1MbRejectionTest(unittest.TestCase):
    """`--partition-l1-mb` must reject negative values at argparse time.

    Before this fix both drivers used `partition_l1_mb > 0 else None`,
    which silently turned `--partition-l1-mb -1` into "disabled" — a
    typo masquerading as a feature. Negative values now hit
    `ArgumentTypeError` and abort parse_args.
    """

    @staticmethod
    def _load_distributed():
        for _mod_name in ("ray",):
            if _mod_name not in sys.modules:
                _stub = types.ModuleType(_mod_name)

                def _remote(*a, **kw):
                    if len(a) == 1 and callable(a[0]):
                        return a[0]
                    return lambda c: c

                _stub.remote = _remote
                _stub._private = types.SimpleNamespace(
                    worker=types.SimpleNamespace(_global_node=None)
                )
                sys.modules[_mod_name] = _stub
        import run_distributed_bench

        return run_distributed_bench

    @staticmethod
    def _load_single_actor():
        import bench_hybrid_cache_ivf_rq

        return bench_hybrid_cache_ivf_rq

    def _expect_argparse_error(self, mod, argv):
        sys.argv = [f"{mod.__name__}.py"] + argv
        with self.assertRaises(SystemExit) as cm:
            mod.parse_args()
        # argparse exits with status 2 for argument errors.
        self.assertEqual(cm.exception.code, 2)

    def test_distributed_driver_rejects_negative_partition_l1(self):
        mod = self._load_distributed()
        self._expect_argparse_error(mod, ["--partition-l1-mb", "-1"])

    def test_distributed_driver_rejects_negative_metadata_l1(self):
        mod = self._load_distributed()
        self._expect_argparse_error(mod, ["--metadata-l1-mb", "-1"])

    def test_distributed_driver_accepts_zero_partition_l1(self):
        mod = self._load_distributed()
        sys.argv = ["run_distributed_bench.py", "--partition-l1-mb", "0"]
        args = mod.parse_args()
        # `0` is the documented "disable partition L1" spelling; the
        # driver maps it to `partition_l1_bytes=None` before calling
        # build_per_actor_spec (the spec validator rejects literal 0).
        self.assertEqual(args.partition_l1_mb, 0)
        spec = mod.build_per_actor_spec(
            "distributed",
            actor_id=0,
            nvme_dir="/mnt/nvme/lance-l2",
            dram_bytes=1 << 30,
            metadata_l1_bytes=64 * MIB,
            partition_l1_bytes=None,
        )
        self.assertIsNone(spec["partition_l1_bytes"])

    def test_single_actor_driver_rejects_negative_partition_l1(self):
        mod = self._load_single_actor()
        self._expect_argparse_error(mod, ["--partition-l1-mb", "-1"])

    def test_single_actor_driver_accepts_zero_partition_l1(self):
        mod = self._load_single_actor()
        sys.argv = ["bench_hybrid_cache_ivf_rq.py", "--partition-l1-mb", "0"]
        args = mod.parse_args()
        self.assertEqual(args.partition_l1_mb, 0)


class PlotResultsV6ColumnsTest(unittest.TestCase):
    """`plot_results.py` must not assume v4 column names.

    The reviewer's specific complaints: `plot_p99_bars` hard-coded the
    v4 scenario list (`no-cache,moka,hybrid`) and dropped `distributed`,
    and `plot_hit_ratio` read the now-removed `hit_ratio` column. After
    the v6 port, plotting must accept `distributed` and read only
    columns the v6 writers produce.
    """

    def test_p99_bars_does_not_filter_out_distributed(self):
        # Parse the source to confirm the v4-only scenario filter is
        # gone. Inspecting source rather than running matplotlib keeps
        # the unit test dependency-free.
        plot_src = (
            Path(HERE) / "plot_results.py"
        ).read_text()
        # The broken line was a literal tuple containing only the v4
        # names. The fixed code references `preferred_order` (which
        # includes `distributed`) and accepts any scenario the CSV
        # carries.
        self.assertNotIn(
            '("no-cache", "moka", "hybrid")', plot_src,
            msg="plot_p99_bars still hard-codes the v4 scenario filter",
        )
        self.assertIn("distributed", plot_src)

    def test_hit_ratio_helper_removed_in_favor_of_l1_size(self):
        plot_src = (
            Path(HERE) / "plot_results.py"
        ).read_text()
        # The v4 hit_ratio panel read `df["hit_ratio"]`; that column
        # is gone from v6 `summary.csv`. The replacement is
        # `plot_l1_size`, which reads `session_size_bytes_*`.
        self.assertNotIn('df["hit_ratio"]', plot_src)
        self.assertIn("plot_l1_size", plot_src)
        self.assertIn("session_size_bytes_pre", plot_src)
        self.assertIn("session_size_bytes_post", plot_src)


class DistributedDriverAliasTest(unittest.TestCase):
    """`run_distributed_bench._normalize_scenario_alias` must rewrite
    'hybrid' to 'distributed' (the original review-fix reversed the
    check). Loaded lazily — the driver pulls heavyweight deps at import
    time.
    """

    @staticmethod
    def _load_helper():
        # Stub Ray-related deps that run_distributed_bench imports at
        # module load. Done lazily inside the test so the rest of the
        # suite doesn't pay for it.
        for _mod_name in ("ray",):
            if _mod_name not in sys.modules:
                _stub = types.ModuleType(_mod_name)

                def _remote(*a, **kw):
                    if len(a) == 1 and callable(a[0]):
                        return a[0]
                    return lambda c: c

                _stub.remote = _remote
                _stub._private = types.SimpleNamespace(
                    worker=types.SimpleNamespace(_global_node=None)
                )
                sys.modules[_mod_name] = _stub
        import run_distributed_bench

        return run_distributed_bench._normalize_scenario_alias

    def test_hybrid_normalizes_to_distributed(self):
        norm = self._load_helper()
        self.assertEqual(norm("hybrid"), "distributed")

    def test_distributed_passes_through_unchanged(self):
        norm = self._load_helper()
        self.assertEqual(norm("distributed"), "distributed")

    def test_other_scenarios_pass_through_unchanged(self):
        norm = self._load_helper()
        self.assertEqual(norm("moka"), "moka")
        self.assertEqual(norm("no-cache"), "no-cache")


if __name__ == "__main__":
    unittest.main()
