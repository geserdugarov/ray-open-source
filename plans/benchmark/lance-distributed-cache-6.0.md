# Lance 6.0 Distributed Cache Benchmark

## Status

**Implementation moved out of this repository.** The Ray-owned
`benchmarks/lance_hybrid_cache/` subtree this plan tracked has been
deleted. The distributed IVF cache actor / coordinator / example now
lives in the `lance-ray` project under
`lance_ray/distributed_cache/` and
`examples/distributed_ivf_cache.py` (commits `17140fe..d0d09ab`).
Refer to that repository for the live design, runbooks, and
implementation; the sections below are preserved as historical
context for the v6 port and no longer describe code in this tree.

This document is the successor of
[`lance-hybrid-cache-ivf-rq.md`](./lance-hybrid-cache-ivf-rq.md). That plan
covers the implemented v4-era hybrid-cache benchmark (`benchmarks/lance_hybrid_cache/`,
commits `36474d837c` and `3be01135c3`). v6 of the Lance cache replaces the
foyer-backed hybrid backend with a new `DistributedCacheBackend`; this
plan describes how to port the existing benchmark to the new API surface.

## Goal

Re-measure the same Lance vector-search workload against the v6
distributed cache, with three cache scenarios that are directly
comparable to the v4 baseline:

- `no-cache`: per-actor `Session(index_cache_size_bytes=0)` (unchanged).
- `moka`: per-actor `Session(index_cache_size_bytes=N)` (unchanged; the
  pure-DRAM moka path is what the default `Session` constructor still
  produces in v6).
- `distributed`: per-actor `Session.with_distributed_cache(l2_dir,
  index_metadata_l1_capacity_bytes, moka_l1_partition_bytes=...)`.
  Replaces both v4 `hybrid` and `hybrid_advanced` scenarios.

The workload target is unchanged so absolute numbers stay comparable:

- Dataset: 10M rows x 1024-d `f32` vectors.
- Index: IVF_RQ, 3000 partitions, `num_bits=8`.
- Storage: MinIO, with optional netem delay for single-host runs.
- Query shape: fixed `nprobes`, same seeded queries across scenarios.

## Approach: incremental rewrite, not greenfield

Recommendation: rewrite the four files that touch the Lance API surface
(`scenarios.py`, `distributed_actor.py`, `check_partition_residency.py`,
`l2_inspect.py`) plus the driver flag plumbing in
`run_distributed_bench.py` / `run_bench.py`, and keep everything else.

What stays as-is:

- Dataset / index creation logic (`bench_hybrid_cache_ivf_rq.py`'s
  `ensure_dataset` path, `_hybrid_cache_helpers.py::ensure_dataset` +
  `minio_storage_options`). v6 does not change `lance.write_dataset`
  or `create_index(index_type="IVF_RQ", ...)`. The `ScenarioActor`
  driver loop in the same file does need the
  `index_cache_stats` -> `size_bytes` swap; see *Output schema and
  helper changes* below.
- Ray topology: `HybridSearchActor` lifecycle, `CoordinatorActor`
  centroid-routing + scatter-gather + top-K merge, `partition_id %
  num_actors` ownership.
- Driver phase shape: prewarm -> optional pre-measure residency -> measure
  -> post-measure residency -> close. The v4 plan's verification
  checklist transfers almost line for line.
- Infra: `infra/docker-compose.yml`, `make_bucket.sh`, `netem_*.sh`,
  `ship_real_cluster.sh`.
- Output file *layout*: `out/results.jsonl`, `out/summary.csv`,
  `out/l2_inventory.csv`, `out/plots/*`,
  `out/partition_residency.jsonl` (the file set stays the same; the
  per-record schemas inside `results.jsonl` and `summary.csv` shrink
  and the `hit_ratio` plot panel is replaced -- see *Output schema
  and helper changes* below).

What changes:

- Every `Session.with_hybrid_cache(...)` / `with_hybrid_cache_advanced(...)`
  call. These factories are gone in v6.
- Every `dataset.prewarm_vector_cache(name, partition_ids, policy=...,
  ram_bytes=..., wait_for_disk=...)` call. The v4 `policy` knob is gone;
  the v6 strict prewarm is `dataset.prewarm_index(name,
  partition_ids=[...])`.
- The per-partition L1 residency probe (`prewarm_vector_cache(...,
  policy="moka_ram_cap", ram_bytes=0)`) no longer works -- there is no
  `ram_bytes=0` no-load shortcut on `prewarm_index`. See *Residency
  probe* below for the replacement.
- The L2 directory inspection helper. v4 walked a foyer region layout;
  v6 walks the new file-per-partition layout under
  `{l2_dir}/v1/{sanitize(prefix)}/part-ivf-{id}.bin`.
- CLI flags. `--codecless-mb` and `--prewarm-ram-fraction` lose their
  meaning. Two new flags appear (`--partition-l1-mb`,
  `--metadata-l1-mb`); `--dram-gb` is retained only for the `moka`
  scenario (where it still sizes `index_cache_size_bytes`) and is
  ignored for `distributed`, whose budget comes from
  `--partition-l1-mb + --metadata-l1-mb`. See the *CLI flag changes*
  table for the full mapping.
- `sess.index_cache_stats()` is **gone from the v6 Python `_Session`**
  (verified: grep `index_cache_stats` over the entire
  `../lance-open-source/python/` tree at commit `9ebfe4de0` returns
  zero hits; only `session.size_bytes()` survives -- see
  `python/python/tests/test_session.py` for the canonical example).
  Every `sess.index_cache_stats()` call in the existing benchmark
  (9 sites in `distributed_actor.py`, 2 in `bench_hybrid_cache_ivf_rq.py`)
  must be replaced; the `results.jsonl` / `summary.csv` per-record
  schemas shrink and the `plot_results.py` hit-ratio panel loses its
  data source. See *Output schema and helper changes* below.

Why not start over: the orchestration code (Ray actor lifecycle,
coordinator scatter-gather, owner-pid math, residency phase shape, MinIO
infra, plotting) is independent of the Lance cache implementation and
was already validated by the v4 run. Greenfield would re-derive it for
no gain. The two prior commits (`36474d837c [Plans]`, `3be01135c3
[Data][Lance][Benchmark]`) stay in history; new commits modify the same
files in place.

## v6 API surface used by this benchmark

Confirmed from the diff `b3d546a64..9ebfe4de0` in `../lance-open-source`:

`python/python/lance/lance/__init__.pyi`:

```python
class _Session:
    def __init__(
        self,
        index_cache_size_bytes: Optional[int] = None,
        metadata_cache_size_bytes: Optional[int] = None,
    ) -> None: ...
    @staticmethod
    def with_distributed_cache(
        l2_dir: Union[str, Path],
        index_metadata_l1_capacity_bytes: int,
        *,
        moka_l1_partition_bytes: Optional[int] = None,
    ) -> _Session: ...
    def invalidate_index_cache(self, dataset_uri: str, index_addr: str) -> None: ...
    def size_bytes(self) -> int: ...
    def is_same_as(self, other: _Session) -> bool: ...

class _Dataset:
    def prewarm_index(
        self,
        name: str,
        *,
        with_position: bool = False,
        partition_ids: Optional[List[int]] = None,
    ): ...
```

PyO3 bridge:

- `Session.with_distributed_cache(...)` returns `Err(PyValueError)` on
  config rejection (e.g. `l2_dir` does not exist, is a symlink, or is
  already locked by another process).
- `Session.invalidate_index_cache(...)` returns `Err(PyIOError)` on rename
  / drain failure. The Ray driver MUST catch this and treat it as a
  retryable condition -- the v6 backend writes a tombstone so a
  subsequent call from the same session can clear it.
- `Dataset.prewarm_index(name, partition_ids=...)` is the strict
  variant: it raises `LanceError` on any L2 write failure or on
  mid-prewarm generation change. Empty list means "prewarm every
  partition under the strict path"; `None` means "use the legacy
  default prewarm path (best-effort)".

What v6 does NOT expose to Python (gaps the plan reckons with):

- `DistributedCacheBackend::stats()` -- the Rust counters defined on
  `DistributedCacheStats`
  (`rust/lance-core/src/cache/distributed.rs:390`) are not bound to
  Python in this branch. The actual exposed fields, verified against
  Lance commit `9ebfe4de0`, are:
    - `l2_writes_total`
    - `l2_write_bytes_total`
    - `l2_read_bytes_total`
    - `l2_write_errors_total`
    - `l2_decode_errors_total`
    - `l2_stale_loader_discards_total`
    - `l2_invalidate_errors_total`
    - `l2_prewarm_skip_corrupt_total`
    - `l2_skipped_non_partition_total`
    - `l2_tombstone_strict_skips_total`
  Note that **no hit counter exists** (there is no `l2_hits_total`,
  no L1-hit counter, no per-tier `entry_count`); cache effectiveness
  has to be inferred from `l2_read_bytes_total` deltas plus
  Python-visible per-query latency plus L2 directory snapshots.
  Treat the L2 directory snapshot delta plus Python-visible
  per-query latency as the only observability surface available
  without a new pylance binding. See *Open questions* below.
- A per-partition residency probe. The v4 `ram_bytes=0` no-load
  shortcut on `prewarm_vector_cache` is gone. See *Residency probe*.
- **`dataset.compute_partition_ids(...)` and
  `dataset.search_partitions(...)`** -- these were Python-exposed on
  the v4 fork the existing benchmark targets, but in the Lance 6.0
  branch at `9ebfe4de0` they exist **only as Rust APIs**
  (`rust/lance/src/index/vector/ivf/v2.rs`, `lance-index/src/vector.rs`,
  `rust/lance/src/io/exec/knn.rs`) plus a passing reference in
  `plans/distributed-cache.md`. Greping the entire `python/` tree at
  that commit returns zero hits for either symbol. The Ray sharded
  coordinator topology depends on both APIs at the Python level, so
  the rewrite is **blocked** on Lance shipping PyO3 wrappers for
  them. Two ways forward, decided before the rewrite starts (see
  *Sharded topology prerequisite* below):
    1. **(recommended)** add a small Lance-side PyO3 patch that
       re-exposes both methods on `_Dataset` with signatures matching
       the v4 fork (`compute_partition_ids(name, query, nprobes) ->
       list[int]`, `search_partitions(name, query, partition_ids, k)
       -> RecordBatch`). Track that patch as a Lance-repo
       prerequisite issue; it is mechanically small (each method
       wraps an existing Rust call).
    2. drop `--mode sharded` from the v6 benchmark and only support
       `--mode replicated`. This loses the partition-sharded
       full-recall coordinator topology, which is the most
       interesting distributed-cache workload, so this option is a
       reduced-scope fallback only.

## Scenarios

Three scenarios; one factory per scenario. The spec is **per-actor**
because the `distributed` session's `l2_dir` is exclusive to one
process -- `Session.with_distributed_cache(...)` returns
`PyValueError` if the directory is missing, a symlink, or already
locked by another process (the constructor takes an exclusive
advisory lock on `{l2_dir}/lance-distributed.lock` for the lifetime
of the session). The driver builds one spec per actor; the actor
itself creates the directory in-process just before constructing the
session.

```python
# scenarios.py (post-rewrite shape)

def per_actor_l2_dir(base_nvme_dir: str, actor_id: int) -> str:
    """Per-actor L2 subdirectory under --nvme-dir.

    Matches the existing v4 layout `<nvme-dir>/actor-<i>/`. The
    directory is created **inside the actor process** (see
    HybridSearchActor.__init__ below), not on the driver host.
    """
    return os.path.join(base_nvme_dir, f"actor-{actor_id}")


def build_scenario_spec(args, scenario: str, actor_id: int) -> dict:
    """One spec per (scenario, actor_id). The driver calls this once
    per worker actor before spawning it; the moka/no-cache branches
    ignore `actor_id` but accept it for signature uniformity. The
    `distributed` branch does NOT touch the filesystem -- it only
    computes the path; mkdir runs inside the actor process."""
    if scenario == "no-cache":
        return {"name": "no-cache", "kind": "no-cache"}
    if scenario == "moka":
        return {
            "name": "moka",
            "kind": "moka",
            "index_cache_size_bytes": int(args.dram_gb * (1 << 30)),
        }
    if scenario == "distributed":
        partition_l1_mb = args.partition_l1_mb  # default 1024; 0 means disable
        partition_l1_bytes = (
            int(partition_l1_mb * (1 << 20)) if partition_l1_mb else None
        )
        return {
            "name": "distributed",
            "kind": "distributed",
            "l2_dir": per_actor_l2_dir(args.nvme_dir, actor_id),
            "metadata_l1_bytes": int(args.metadata_l1_mb * (1 << 20)),
            "partition_l1_bytes": partition_l1_bytes,
        }
    raise ValueError(f"unknown scenario: {scenario!r}")
```

**Per-actor L2 dir creation** lives in `HybridSearchActor.__init__`
(and `ScenarioActor.run` for the single-actor driver), before
`build_session(spec)` is called:

```python
# distributed_actor.py (post-rewrite shape)

class HybridSearchActor:
    def __init__(self, actor_id, spec, uri, endpoint_url, nprobes):
        if spec["kind"] == "distributed":
            # Runs on the worker node, against actor-local NVMe.
            # Driver-side mkdir would only touch the head-node
            # filesystem in a multi-node cluster, leaving the
            # worker's --nvme-dir absent and Session.with_distributed_cache
            # would PyValueError on construction.
            os.makedirs(spec["l2_dir"], exist_ok=True)
        self._sess = build_session(spec)
        ...
```

**Driver-side validation is optional and scoped to single-host runs.**
The drivers MAY do a fast-fail `os.access(args.nvme_dir, os.W_OK)`
check before spawning any actor, but ONLY when actors run on the
local node (no `--actor-resource`); in real-cluster mode (`actors
pinned to remote nodes via Ray custom resources`) the driver host
does not see the worker's NVMe and any driver-side `mkdir` would be
either a no-op or a misleading success. The driver therefore prints
the planned `<nvme-dir>/actor-<i>/` paths and the
`--actor-resource` value to stderr so an operator can verify the
worker nodes have the directory prepared, but does not attempt to
create it remotely.

The driver (`run_distributed_bench.py`) calls
`build_scenario_spec(args, args.scenario, actor_id=i)` in a loop over
`range(args.num_actors)` and hands each spec to `HybridSearchActor.options(...).remote(...)`.
The single-actor driver (`run_bench.py`) uses `actor_id=0`. Replicated
mode (`--mode replicated`) still gets one spec per actor and one L2
directory per actor; sharing an L2 dir between actors is not supported
by the v6 backend.

`_hybrid_cache_helpers.build_session(spec)` routes:

```python
def build_session(spec: dict) -> lance.Session:
    kind = spec["kind"]
    if kind == "no-cache":
        return lance.Session(index_cache_size_bytes=0)
    if kind == "moka":
        return lance.Session(
            index_cache_size_bytes=spec["index_cache_size_bytes"],
        )
    if kind == "distributed":
        return lance.Session.with_distributed_cache(
            l2_dir=spec["l2_dir"],
            index_metadata_l1_capacity_bytes=spec["metadata_l1_bytes"],
            moka_l1_partition_bytes=spec.get("partition_l1_bytes"),
        )
    raise ValueError(f"unknown scenario kind: {kind!r}")
```

Notes:

- The v4 `hybrid` / `hybrid_advanced` distinction collapses. The v6
  backend always exposes the two-tier moka split (partition L1 +
  metadata L1) as separate budgets; there is no codec-less Moka knob to
  carve out. Drop `--codecless-mb`.
- The metadata L1 budget is mandatory and non-zero. Default to 64 MiB
  (`--metadata-l1-mb 64`), with a floor warning if the user sets it
  below 4 MiB. The metadata tier holds `IvfIndexState`, `IndexMetadata`,
  `FragReuseIndex`, `ScalarIndexDetails`, etc.; sizing too small
  defeats the per-query routing path.
- The partition L1 budget defaults to **`--partition-l1-mb 1024`**
  (~1 GiB per actor) for the `distributed` scenario. Sizing rationale:
  the 10M / 3000-partition / 8-bit RQ run produces ~3.4 MiB per
  partition; with two actors each owning 1500 partitions, ~1 GiB
  holds a meaningful working slice without dominating actor RAM.
  The **disable path is `--partition-l1-mb 0`**, which the driver
  translates to `moka_l1_partition_bytes=None` when calling
  `Session.with_distributed_cache(...)`. Disabling the partition L1
  tier means every partition decode runs against the L2 file; useful
  as a stress baseline but not a default. There is no other "off"
  spelling -- the CLI parser rejects `none` / `off` strings to keep
  the flag schema uniform with `--metadata-l1-mb`.

## CLI flag changes

| Flag | v4 | v6 |
|---|---|---|
| `--scenario {no-cache,moka,hybrid}` | yes | replace `hybrid` -> `distributed` |
| `--dram-gb` | per-actor DRAM budget (foyer L1 + codec-less Moka) | per-actor DRAM budget for the moka scenario only; ignored for `distributed` (see two flags below) |
| `--codecless-mb` | carves codec-less Moka out of `--dram-gb` | **drop** (no analog) |
| `--prewarm-ram-fraction` | legacy no-op already | **drop** (no analog) |
| `--metadata-l1-mb` | n/a | **new**: metadata L1 budget; default 64; required for `distributed` |
| `--partition-l1-mb` | n/a | **new**: partition L1 budget (decoded entries); default 1024 (MiB); pass `0` to disable (driver maps to `moka_l1_partition_bytes=None`) |
| `--l2-gb` | L2 NVMe budget (informational; foyer enforces) | informational only; v6 has no L2 capacity bookkeeping (operator sizes `l2_dir` against the partition slice) |
| `--prewarm {natural,forced,sharded,none}` | yes | keep `natural`, `sharded`, `none`; rename `forced` -> `forced-all` and switch its impl to `dataset.prewarm_index(name, partition_ids=[])` -- the **strict** all-partitions path. Do **NOT** call the no-`partition_ids` form `dataset.prewarm_index(name)`: against the distributed backend that is the best-effort path that swallows L2 write errors and tombstoned-prefix skips, which would let a "forced" benchmark silently run with an incomplete L2. The strict path raises `LanceError` on any L2 write failure / mid-prewarm generation change; the driver re-raises and the run fails fast. |
| `--pre-measure-residency-probe` | yes | keep flag, but its impl changes (see *Residency probe*) |
| `--actor-resource`, `--coordinator-resource`, `--nvme-dir`, `--k-list`, `--nprobes`, `--warmup-queries`, `--measure-queries`, `--skip-setup`, `--num-actors`, `--mode {replicated,sharded}` | yes | unchanged |
| `--simulate-invalidation` | n/a | **new (optional)**: after Phase 2 measure, call `session.invalidate_index_cache(uri, index_addr)` per actor and rerun a second measure phase to validate freshness and L2-rehydration cost. See *Invalidation drill* below. |

A small back-compat shim is reasonable: if a user passes `--scenario
hybrid` against the v6 driver, alias it to `distributed` and print a
deprecation warning. If a user passes `--codecless-mb` or
`--prewarm-ram-fraction`, log a warning and ignore.

## Output schema and helper changes

The existing benchmark records per-actor `index_cache_stats()` snapshots
(`{hits, misses, num_entries, size_bytes}`) in the run output and
derives a hit ratio for `plots/hit_ratio.png`. Lance 6.0 drops
`index_cache_stats()` from the Python `_Session` (`size_bytes()` is the
only surviving accessor), so these schemas and the plot panel must
change. Decisions, captured here so the rewrite PR does not relitigate
them:

- Replace every `dict(sess.index_cache_stats())` in
  `distributed_actor.py` (8 sites, one per method that snapshots a
  session-stats dict: `prewarm_index`, `prewarm_partitions`,
  `prewarm_partitions_deterministic`, `set_owned_partitions`,
  `measure`, `measure_sharded`, `cache_stats`,
  `check_partition_residency`) and `bench_hybrid_cache_ivf_rq.py`
  (`ScenarioActor.run` `stats_pre` + `stats_post`, 2 sites) with
  `{"size_bytes": int(sess.size_bytes())}`. 8 + 2 = **10 producer
  sites total**.
- The result JSON key stays `stats_pre` / `stats_post` so the
  `out/results.jsonl` field layout is unchanged at the top level;
  every consumer that reads `stats_post["hits"]` /
  `stats_post["misses"]` / `stats_post["num_entries"]` must be
  migrated in lock-step with the swap, otherwise the next run
  either raises `KeyError` or silently logs `hit_ratio=0.00%`
  (`hits=0 / max(1, 0+0)`) once Lance 6.0 stops emitting the
  fields. The full inventory of live dereference sites in the
  current branch, with the v6 replacement for each:

    | File | Line(s) | Current behavior | v6 replacement |
    |---|---|---|---|
    | `_hybrid_cache_helpers.py::ScenarioResult.summary_rows` | 227-246 | builds the per-row dict for `summary.csv` (reads `hits`, `misses`, `num_entries`, `size_bytes`) | rewrite to emit the new `summary.csv` schema below (`session_size_bytes_pre/post/delta`) |
    | `bench_hybrid_cache_ivf_rq.py::write_results_to_csv` | 244-273 | second `summary.csv` writer (duplicates `summary_rows`'s columns inline) -- the single-actor driver path; reads `hits`/`misses`/`num_entries`/`size_bytes`/`hit_ratio` | rewrite the inline `rows.append({...})` to match the same new schema; drop the `hit_ratio` and `cache_size_bytes` fields |
    | `bench_hybrid_cache_ivf_rq.py::print_summary` | 276-287 | prints per-scenario stdout line "hit_ratio=...%  cache_entries=...  cache_bytes=..." | replace with "session_size: pre=... -> post=...  delta=..." formatted from `size_bytes` |
    | `run_bench.py::print_summary` (warmup-phase counters) | 148-166 | computes `pre_hits`, `pre_misses`, `pre_ratio`, `delta_hits`, `delta_misses`, `delta_ratio` from `stats_pre` / `stats_post` and prints the "Warmup-phase counters" block | drop the whole block. Replace with a single line per (scenario, repeat): `f"  [{name} r{repeat}] session_size: pre={pre_bytes:,} -> post={post_bytes:,}  delta={delta:,}"`. The "warmup vs measure" decomposition the v4 block produced cannot be recovered without a hit counter; the L2-directory diff is the only Python-visible signal for cache effectiveness. |
    | `run_distributed_bench.py` coord-mode pre-measure subtraction | 861-868 | snapshots `pre_measure_stats`, then **destructively mutates** `post["hits"] -= base["hits"]` and `post["misses"] -= base["misses"]` so the per-actor row reports measure-only counters | drop the subtraction block entirely. `stats_post["size_bytes"]` is a cumulative size (not a delta), so there is no "subtract baseline" step; instead pass `stats_pre["size_bytes"]` alongside `stats_post["size_bytes"]` and let downstream rows / prints compute the delta themselves. The `pre_measure_stats` ray.get call stays so the pre-snapshot is captured. |
    | `run_distributed_bench.py` replicated-mode pre-measure subtraction | 905-911 | same destructive mutation gated on `if do_pre_residency_probe` | same fix as the coord branch |
    | `run_distributed_bench.py` aggregate reporting | 1034-1069 | accumulates `total_hits` / `total_misses` and emits the "aggregate cache stats: hit_ratio=..." log line | replace per the *Aggregate reporting* paragraph below (`total_l1_size_bytes_pre/post`, `total_l2_files`, `total_l2_disk_bytes`) |
    | `run_distributed_bench.py` per-actor printer | 1088-1104 | prints `f"  actor-{id}  hit={hr:.1%}  entries={num_entries}  bytes={size_bytes:,}  owned=..."` | print `f"  actor-{id}  L1 size: {pre:,} -> {post:,}  owned={...}  calls_handled={...}"` (drops the `hit=` and `entries=` columns; both require the dead `hits`/`num_entries` keys) |

  In total: 8 live dereference sites across 4 files. The plan's
  earlier "two downstream consumers" count was wrong; the rewrite
  PR must migrate all 8, plus the 8 + 2 = 10 producer sites
  enumerated in the next bullet.
- **`summary.csv` schema (concrete column list).** Drop `hits`,
  `misses`, `hit_ratio`, `num_entries`, `cache_size_bytes`. Add:
    - `session_size_bytes_pre` -- `stats_pre["size_bytes"]` for
      that (scenario, repeat). Captured **before warmup** for the
      single-actor driver; **after the prewarm phase but before
      the measure phase** for the distributed driver.
    - `session_size_bytes_post` -- `stats_post["size_bytes"]` for
      that (scenario, repeat). Captured **after the measure
      phase**.
    - `session_size_bytes_delta` -- precomputed
      `post - pre` for the plot panel below (saves the
      plotting / spreadsheet consumer from doing the subtraction).
      **This delta is not a "query-driven L1 growth" measurement.**
      Lance 6.0's strict prewarm
      (`try_persist_with_codec_under_global_read` at
      `rust/lance-core/src/cache/distributed.rs:1073-1075`) writes
      L2 **and** admits the partition entry into the L1 partition
      tier when L1 is configured; under `--prewarm sharded` or
      `--prewarm forced-all` the L1 may already be at or near its
      cap by the time the measure phase starts. Measure traffic
      can hit L1 (delta ~= 0), churn entries within the cap
      (delta ~= 0), evict to make room for entries from outside
      the prewarmed slice (delta < 0), or fill any remaining
      headroom (delta > 0). The signed value is informative for
      tuning `--partition-l1-mb` but is **not** a proxy for "the
      cache was used"; rely on per-query latency vs. the
      `no-cache` scenario for that signal.
  The order of unchanged columns (`scenario`, `repeat`, `k`,
  `p50_s`, `p95_s`, `p99_s`, `mean_s`, `n`, `duration_s`) is
  preserved.
- **`plot_results.py`.** Drop the hit-ratio bar panel and replace
  it with `plots/l1_size.png`, a per-scenario grouped bar chart
  with two bars per scenario (`session_size_bytes_pre` and
  `session_size_bytes_post`) averaged across repeats. The panel
  visualizes the absolute L1 occupancy entering and leaving the
  measure phase; a measure-phase delta panel is not added because
  the signed delta does not have a single "good direction" under
  the L1-admit-on-prewarm behavior described above. The latency
  CDF and p99 bars panels are unchanged and remain the primary
  evidence that the cache is doing its job.
- **Aggregate reporting in `run_distributed_bench.py` (lines
  ~1034-1069).** The post-coordinator-merge loop no longer
  accumulates `total_hits` / `total_misses`. Replace with:
    1. `total_l1_size_bytes_pre = sum(r["stats_pre"]["size_bytes"]
       for r in per_actor_results)` and the symmetric
       `total_l1_size_bytes_post`, collected over the same
       `for r in per_actor_results` loop that already runs.
    2. `total_l2_files`, `total_l2_disk_bytes` from each actor's
       post-measure L2 snapshot (already collected via
       `snapshot_l2_dir`; reuse that data, no extra remote call).
    3. The "aggregate cache stats" log line becomes:
       `aggregate L1: {humanize(total_l1_size_bytes_pre)} ->
       {humanize(total_l1_size_bytes_post)} across {N} actors;
       aggregate L2: {total_l2_files} files,
       {humanize(total_l2_disk_bytes)} on disk`.
       The signed pre/post values intentionally replace the v4
       `hit_ratio=%`; the delta is reported in the per-scenario
       `summary.csv` (`session_size_bytes_delta`) but is not the
       primary KPI -- see the caveat in *summary.csv schema*.
  The per-actor section below (lines ~1088+) drops the per-actor
  `hit_ratio` print and substitutes
  `f"L1 size: {humanize(r['stats_pre']['size_bytes'])} -> "
  f"{humanize(r['stats_post']['size_bytes'])}"`.
- The per-actor residency JSONL entry's `session_stats` field
  similarly shrinks from `{hits, misses, num_entries, size_bytes}`
  to `{"size_bytes": int}`.
- The two README references to `index_cache_stats` /
  `hit_ratio.png` / `hit ratio` (in `README.md` and `REAL_CLUSTER.md`)
  are rewritten to describe the `session_size_bytes` view and the
  new `l1_size.png` panel. The README should also call out the
  L1-admit-on-prewarm caveat so a reader does not interpret a
  near-zero or negative delta as a cache miss.

If Open Question (1) is resolved by adding a pylance binding for
`DistributedCacheStats`, this section is amended in the rewrite PR to
re-introduce a derived hit-ratio approximation from
`l2_read_bytes_total` / `l2_writes_total` deltas. The default path
documented here does NOT depend on that binding landing.

## Sharded prewarm

Replace this v4 call:

```python
dataset.prewarm_vector_cache(
    index_name,
    partition_ids,
    policy="hybrid_tiered",
    ram_bytes=...,
    wait_for_disk=True,
)
```

with the v6 strict path:

```python
dataset.prewarm_index(index_name, partition_ids=partition_ids)
```

Properties:

- For `distributed`, the strict path writes one `part-ivf-{id}.bin`
  file per partition under `{l2_dir}/v1/{sanitize(prefix)}/`. Atomic
  publish (`tmp-{nonce}` -> `fsync` -> `rename` -> `fsync(parent)`)
  means a crash leaves no half-written files.
- For `moka`, the strict path loads decoded entries into the moka
  cache until the cap is hit. With v4 the `policy="moka_ram_cap"`
  variant returned `stopped_before` once a budget was exhausted; v6 has
  no such early-exit. Two coping options for `moka`:
    - Document that the `moka` scenario's sharded prewarm will always
      complete and may double-cache (the actor owns the whole prefix's
      partition entries until moka's own LRU evicts them). This is
      what we want: the scenario is intentionally over-budget.
    - Skip the strict prewarm for `moka`; rely on warmup queries to
      populate the moka cache the v4 way. Cleaner; preferred default.
- For `no-cache`, sharded prewarm still registers ownership only
  (`HybridSearchActor.set_owned_partitions(...)`); no Lance call.

Driver-side validation in v4 read counters from the prewarm return dict
(`loaded_to_ram == 0`, `loaded_to_disk + skipped_existing ==
owned_n`, `disk_bytes_unknown_spills == 0`). v6's `prewarm_index`
returns `None`. The new validation is:

- The strict call either completed cleanly (assumed: every owned
  partition has a `part-ivf-{id}.bin` file on disk for distributed
  scenarios) or raised `LanceError`. Treat any raised error as
  hard-fail with traceback.
- Take an L2 directory snapshot post-prewarm and verify
  `file_count == len(owned_partition_ids)` for the
  `{l2_dir}/v1/{sanitize(prefix)}/` subdir. This is the v6 analog of
  the v4 counter check.
- Sum the per-file `disk_bytes` and log it as the "prewarm L2
  footprint" so the operator can compare against `--l2-gb`.

Sharded prewarm for `distributed` runs in parallel across actors. The
coordinator (if any) does not prewarm; it only routes.

## Residency probe

The v4 per-partition L1 probe relied on
`prewarm_vector_cache(..., policy="moka_ram_cap", ram_bytes=0,
wait_for_disk=False)` reporting `skipped_existing == 1` for partitions
already in L1. v6 has no equivalent: `prewarm_index(name,
partition_ids=[pid])` is the strict path; calling it with `partition_ids=[pid]`
when the partition is L2-resident would still re-read and re-decode if
not in L1, which defeats the "no-load probe" property.

Two viable replacements:

1. **Aggregate-only probe (recommended).** Drop the per-partition
   `in_l1` / `not_in_l1` lists. Replace with:
    - L2 file inventory under `{l2_dir}/v1/{sanitize(prefix)}/` --
      gives `in_l2 = set(parse_partition_id(f) for f in
      part-ivf-*.bin)` exactly (no inference). The v6 `partition_is_in_l2`
      ambiguity from v4 goes away because the on-disk layout
      one-to-one maps file presence to L2 residency, and `tokio::fs`
      writes are atomic.
    - L1 size from `session.size_bytes()` (already exposed in v6). This
      gives a coarse "how full is the L1 partition tier" indicator but
      not per-partition identity. Useful as an absolute occupancy
      readout entering and leaving the measure phase; not a "L1
      promoted by query traffic" signal -- strict prewarm
      (`try_persist_with_codec_under_global_read`) admits partition
      entries into L1 when `--partition-l1-mb > 0`, so L1 is often
      already at or near its cap when the measure phase starts.
    - L2 directory diff (file count + on-disk bytes) between
      post-prewarm and post-measure to detect unexpected L2 growth.
      Under the v6 design's "no L1 -> L2 writeback" property, the diff
      should be zero or near-zero. A non-zero diff is a regression.
   This loses the v4 per-partition residency shift detail
   (`stayed_in_l1`, `evicted_from_l1`, `promoted_into_l1`) but those
   were already inferred values in v4 since the L2 probe was unbound.

2. **Per-partition probe via strict prewarm (optional, expensive).**
   Call `dataset.prewarm_index(name, partition_ids=[pid])` per partition
   and time it. A sub-millisecond call indicates the L2 fast-skip path
   fired (file exists, parsed OK, same generation), which means the
   partition is L2-resident. A multi-millisecond call indicates an OBS
   re-fetch happened. This is heavy enough to perturb the cache it is
   trying to measure -- only enable under `--pre-measure-residency-probe`
   with a strong warning. NOT recommended as the default.

Plan default: implement option (1). Keep `--pre-measure-residency-probe`
as a flag but make it gate option (2) only.

`check_partition_residency.py` is renamed to `check_l2_residency.py`
and reduced in scope: it walks `{l2_dir}/v1/{sanitize(prefix)}/`, lists
the partition ids found, and reports against the actor's owned slice
(missing partitions = expected_owned - found_on_disk). The Rust-side
`IvfIndexSearcher::partition_is_in_l2` binding gap is moot because the
on-disk layout already answers the question.

Output schema for `partition_residency.jsonl` (per actor entry):

```json
{
  "actor_id": 0,
  "label": "post-prewarm" | "post-measure",
  "owned_count": 1500,
  "in_l2": [0, 2, 4, ...],
  "missing": [],
  "l2_size_bytes_total": 5234567890,
  "l2_file_count": 1500,
  "l1_size_bytes_at_probe": 0,
  "probe_duration_s": 0.05
}
```

(Backward-compat aliases `in_ram` / `not_in_ram` were always
inference-only on the v4 path. Drop them; the v4-era plotting code
already preferred `in_l1` / `not_in_l1`.)

## Invalidation drill (new in v6)

The v4 benchmark could not exercise invalidation cleanly -- the
hybrid backend's only path to "drop everything" was actor restart.
v6 exposes `Session.invalidate_index_cache(dataset_uri, index_addr)`
synchronously; the IOError-on-failure contract makes it directly
testable from the driver.

Proposed `--simulate-invalidation` flow, opt-in:

1. Run Phase 2 measure exactly as today; record per-query latency.
2. For each worker actor, call
   `session.invalidate_index_cache(dataset_uri, index_addr)`. Catch
   `IOError` (rename failure); on failure, sleep 1s and retry once.
   Hard-fail the run if the retry still raises -- the freshness
   contract is broken.
3. Snapshot L2 directory; verify the per-prefix subdir is gone (or
   renamed to `.{sanitize(prefix)}.deleting-{nonce}`).
4. Re-run sharded prewarm. Time it. This is the "cold L2 rehydration
   cost".
5. Re-run Phase 2 measure. Compare latencies vs the first measure pass
   (should be within noise; both runs see warm L2).

Output a small `out/invalidation.json`:

```json
{
  "measure1_p50_s": 0.012,
  "measure1_p95_s": 0.034,
  "invalidate_per_actor_s": [0.07, 0.08],
  "rehydrate_prewarm_s": 19.3,
  "measure2_p50_s": 0.012,
  "measure2_p95_s": 0.035,
  "delta_p50_pct": 0.0,
  "delta_p95_pct": 2.9
}
```

This is the smallest end-to-end verification of the v6 freshness
guarantee that the benchmark can produce; including it from the start
catches regressions in the Rust generation / tombstone protocol that
unit tests alone cannot.

## L2 directory layout helper

`l2_inspect.py` changes from "walk an opaque foyer region directory" to
"walk the v6 layout":

```text
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
```

API stays the same (`snapshot_l2_dir(path) -> dict`, `diff_snapshots(pre,
post)`), but the dict gains structured fields:

```python
{
  "path": "/mnt/nvme/lance-l2/distributed/actor-0",
  "exists": True,
  "lock_present": True,
  "manifest_present": True,
  "tombstones_present": False,
  "prefix_dirs": [
    {
      "name": "<sanitize(prefix)>",
      "file_count": 1500,
      "apparent_bytes": 5234567890,
      "disk_bytes": 5234680000,
      "deleting": False,
    },
    ...
  ],
  "file_count": 1500,
  "apparent_bytes": 5234567890,
  "disk_bytes": 5234680000,
  "files": [...],  # leaf detail, unchanged
}
```

`diff_snapshots` returns `apparent_bytes_delta`, `disk_bytes_delta`,
`file_count_delta` (unchanged) and a new `tombstones_added` bool. A
non-zero `tombstones_added` between two probes is a hard error and
should fail the run -- it means an invalidation hit the rename-failure
path silently.

## Coordinator

`CoordinatorActor` stays as-is *shape-wise*: it owns the IVF model
centroids in DRAM, calls `dataset.compute_partition_ids(...)` to
route a query, and scatter-gathers `search_partitions(...)` calls to
the workers. Its `Session` is constructed with the plain
`lance.Session(index_cache_size_bytes=64*1024*1024)` factory -- the
distributed backend is per-worker, not per-coordinator.

The coordinator code can only run once the Python wrappers for both
APIs land in pylance (see *Sharded topology prerequisite* under
"What v6 does NOT expose to Python"). Until then the coordinator is
unbuildable against the v6 wheel; the workaround during early
bring-up is to run the benchmark in `--mode replicated`.

One small change: when `--mode sharded` and `--simulate-invalidation`
is enabled, the coordinator also needs to call
`session.invalidate_index_cache(uri, index_addr)` so its cached
`IvfIndexState` (held in the coordinator's metadata L1) is dropped too.
Without this the coordinator routes against a stale state after the
workers have been invalidated.

## Real-cluster shape

`REAL_CLUSTER.md` is updated to:

- Pinned pylance build: `../lance-open-source` branch
  `private-cache-6.0-ver-1` at commit `9ebfe4de0` or newer.
- `--scenario distributed` instead of `--scenario hybrid`.
- `--metadata-l1-mb 64 --partition-l1-mb 1024 --l2-gb 8` as the
  recommended starting point for the 10M / 1024-d / 3000-partition /
  8-bit RQ run with 2 actors (~5 GiB per actor on L2; ~1 GiB partition
  L1 + 64 MiB metadata L1 per actor; ~1.1 GiB DRAM total per actor).
- `--simulate-invalidation` listed as recommended for any first run
  against a new Lance build (catches generation / tombstone
  regressions early).

`ship_real_cluster.sh` does not change.

## Migration: keep or revert the v4 commits

Keep them. Reasons:

- The v4 plan (`plans/benchmark/lance-hybrid-cache-ivf-rq.md`,
  `36474d837c`) documents the old API surface and is still the
  reference for anyone reading the history of the v4 production run.
- The v4 implementation (`3be01135c3`) contains the dataset / index
  builder, MinIO infra, plotting, and Ray topology code, all of which
  the v6 rewrite reuses in place.
- Reverting both commits would force a re-derivation of the unchanged
  parts and lose the v4 measurements in git history. There is no
  upside.

The v4 plan stays as-is; this plan supersedes it for any new run. When
the v6 rewrite is implemented, mark the v4 plan's *Status* section as
"superseded by lance-distributed-cache-6.0.md".

## File-by-file change list

For the follow-up implementation PR:

| File | Change |
|---|---|
| `benchmarks/lance_hybrid_cache/scenarios.py` | replace `hybrid` / `hybrid_advanced` constructors with a `distributed` spec; drop `codecless_capacity_bytes`; add `metadata_l1_bytes` + `partition_l1_bytes`. |
| `benchmarks/lance_hybrid_cache/_hybrid_cache_helpers.py` | rewrite `build_session` per the snippet above; `ensure_dataset`, `minio_storage_options`, `warmup`, `measure`, `percentiles`, `format_latency_row` unchanged. |
| `benchmarks/lance_hybrid_cache/bench_hybrid_cache_ivf_rq.py` | full v6 port: (a) `build_scenario_specs` (line 139) -- replace the `hybrid` branch entirely (it builds specs for the removed `Session.with_hybrid_cache(...)` / `with_hybrid_cache_advanced(...)` factories) with a `distributed` branch using `Session.with_distributed_cache(...)`; drop the `--codecless-mb` arm of the if/else and the `args.dram_mb * MIB` / `args.codecless_mb * MIB` budget math; (b) replace the CLI flag definitions -- drop `--codecless-mb` and the dual-meaning `--dram-mb`, add `--metadata-l1-mb` (default 64) and `--partition-l1-mb` (default 1024; pass `0` to disable); keep `--dram-mb` only as the moka-scenario budget alias for `--dram-gb` (see *CLI flag changes*); (c) replace the default `--scenarios no-cache,moka,hybrid` with `no-cache,moka,distributed`; (d) thread `actor_id=0` through to the new per-actor `build_scenario_spec` (see *Scenarios*); (e) replace the two `sess.index_cache_stats()` calls in `ScenarioActor.run` with `{"size_bytes": int(sess.size_bytes())}`; (f) **rewrite `write_results_to_csv` at lines 244-273 and `print_summary` at lines 276-287** -- both currently dereference `hits` / `misses` / `num_entries` / `hit_ratio` / `cache_size_bytes` and will `KeyError` once `stats_post` shrinks to `{size_bytes}`; emit the new schema columns / log line per the consumer-inventory table above. Dataset / index creation unchanged. |
| `benchmarks/lance_hybrid_cache/distributed_actor.py` | rewrite `prewarm_partitions_deterministic` to call `dataset.prewarm_index(name, partition_ids=...)`; drop the `policy` / `ram_bytes` / `wait_for_disk` arguments; rewrite `check_partition_residency` to use the L2-walk path; switch hard-fail guards from prewarm counters to the L2 file-count check; replace every `sess.index_cache_stats()` call (9 sites) with `{"size_bytes": int(sess.size_bytes())}`. |
| `benchmarks/lance_hybrid_cache/plot_results.py` | drop the `hit_ratio` panel and the `index_cache_stats` axis title; add an `l1_size.png` grouped-bar panel showing `session_size_bytes_pre` and `session_size_bytes_post` per scenario (no growth-delta panel -- see *Output schema and helper changes* for why the delta is not a single-direction KPI under L1-admit-on-prewarm). |
| `benchmarks/lance_hybrid_cache/run_distributed_bench.py` | drop `--codecless-mb` and `--prewarm-ram-fraction`; add `--metadata-l1-mb`, `--partition-l1-mb`, `--simulate-invalidation`; alias `--scenario hybrid` -> `distributed` with deprecation warning; add the invalidation drill as Phase 2.7. **Also**: delete the two destructive `post["hits"] -= base["hits"]` / `post["misses"] -= base["misses"]` mutation blocks at lines 861-868 (coord-mode) and 905-911 (replicated-mode + `--pre-measure-residency-probe`); keep the `pre_measure_stats` snapshot but propagate it as a separate field. Update the aggregate reporting (lines 1034-1069) and per-actor printer (lines 1088-1104) per *Output schema and helper changes*. Driver no longer mkdirs L2 directories on the head node (see *Scenarios*); replace any v4 driver-side mkdir with a stderr advisory listing of expected per-actor paths. |
| `benchmarks/lance_hybrid_cache/run_bench.py` | same scenario / flag changes; **delete the "Warmup-phase counters (stats_pre)" block at lines 148-166** (pre/measure hit/miss decomposition has no v6 analog -- replace with a single per-(scenario, repeat) "session_size: pre=... -> post=... delta=..." line); single-actor flow otherwise unchanged. Pass `actor_id=0` to `build_scenario_spec`. |
| `benchmarks/lance_hybrid_cache/check_partition_residency.py` | rename to `check_l2_residency.py`; reduce to L2-walk; keep the JSONL output schema with the per-actor fields in *Residency probe* above. |
| `benchmarks/lance_hybrid_cache/l2_inspect.py` | rewrite `snapshot_l2_dir` to walk the v6 `v1/...` layout; add `tombstones_added` to the diff. |
| `benchmarks/lance_hybrid_cache/infra/*` | no change. |
| `benchmarks/lance_hybrid_cache/requirements.txt` | bump pylance pin to the v6 branch wheel (or document the local-build path). |
| `benchmarks/lance_hybrid_cache/README.md` | replace scenario names, flag list, pylance pin, observability caveat. |
| `benchmarks/lance_hybrid_cache/REAL_CLUSTER.md` | replace recommended flags and L2-residency interpretation. |
| `plans/benchmark/lance-hybrid-cache-ivf-rq.md` | flip *Status* to "superseded by lance-distributed-cache-6.0.md" (single-line edit). |

The rewrite is contained to one Ray subpackage; no Ray Core, Data, or
Train code changes.

## Verification checklist (post-rewrite)

For a sharded `distributed` real-cluster run, verify:

1. The driver logs `sharded prewarm (strict, policy=v6 distributed
   cache) -- writing one part-ivf-{id}.bin per owned partition`.
2. Each actor owns the expected `num_partitions / num_actors` count.
3. `dataset.prewarm_index(name, partition_ids=[...])` returned without
   raising for every actor.
4. The L2 directory snapshot shows
   `file_count == owned_count` and `apparent_bytes` within ~5% of
   `owned_count * expected_partition_size`.
5. The coordinator routing warmup runs once before measurement.
6. Measurement output includes coordinator aggregate latency and
   per-actor `Session.size_bytes()` pre/post snapshots. Treat
   measure-phase latency (vs. `--scenario no-cache` baseline) as
   the primary cache-effectiveness signal; the `size_bytes` delta
   may be ~= 0, positive, or negative depending on how full L1
   was after prewarm -- see *Output schema and helper changes*.
7. Post-measure L2 directory snapshot shows zero or near-zero byte /
   file-count delta vs the post-prewarm snapshot.
8. `tombstones_added == False` on every snapshot diff.
9. If `--simulate-invalidation` is enabled, the post-invalidate
   snapshot shows the per-prefix subdir gone (or renamed to
   `.deleting-{nonce}`), the rehydrate prewarm completes, and the
   second-measure p50 / p95 are within 5% of the first.
10. The run completes with no `LanceError` / `IOError` raised from
    `prewarm_index` or `invalidate_index_cache`.

## Open questions

These need a quick answer from the Lance build before the rewrite
starts. Item 0 is a **hard prerequisite** -- the sharded topology
cannot be implemented at all without it -- and is recorded here so
the implementation PR is not started until the Lance side moves.
Items 1-4 are softer (the migration is buildable without them but
the cost estimate slips if any answer comes back unexpected):

0. **(Blocking) PyO3 bindings for `compute_partition_ids` and
   `search_partitions`.** Both are Rust-only at Lance commit
   `9ebfe4de0` (`rust/lance/src/index/vector/ivf/v2.rs`,
   `lance-index/src/vector.rs`, `rust/lance/src/io/exec/knn.rs`); the
   Python tree at that commit has zero matches for either symbol.
   The sharded coordinator topology depends on both at the Python
   layer. Resolution: file a Lance-side issue to add the wrappers
   on `_Dataset` (signatures match the v4 fork). Without that patch
   the v6 rewrite either drops `--mode sharded` or stalls on the
   Lance-side change; this plan recommends waiting for the patch
   rather than carrying a permanent reduced-scope benchmark.

1. **Python observability binding.** The v6 backend exposes
   `DistributedCacheStats` in Rust (10 counters, listed in *What v6
   does NOT expose to Python* above) but the Python diff does not
   bind it. Two options: (a) ship a small Lance-side PyO3 wrapper
   (`Session.distributed_cache_stats(self) -> dict`) and use it in
   the driver; (b) live with L2-directory-only observability. The
   plan assumes (b); switching to (a) would let the driver report
   `l2_stale_loader_discards_total`, `l2_read_bytes_total`,
   `l2_write_errors_total`, etc. directly. Note that even with (a)
   there is **no cache-hit counter** -- inference from
   `l2_read_bytes_total` deltas and per-query latency is the best
   that's available without a Lance-side counter addition.
2. **Strict prewarm vs `moka` scenario.** Strict prewarm on the moka
   scenario will load every owned partition into the moka cache,
   blowing past the cap and relying on moka's LRU to evict. Confirm
   this is the intended way to measure moka under sharded prewarm,
   or switch the moka scenario to skip the strict prewarm and rely
   on warmup queries the way the v4 plan did.
3. **Coordinator metadata L1 budget.** For the v4 coordinator the
   v4 driver used `index_cache_size_bytes=64*1024*1024`. v6's
   coordinator does not need partition L1 but does need metadata L1
   (to cache `IvfIndexState`). Confirm that the plain
   `Session(index_cache_size_bytes=N)` constructor in v6 sizes the
   metadata tier correctly, or whether the coordinator should also
   use `with_distributed_cache` with `moka_l1_partition_bytes=None`
   and a small `index_metadata_l1_capacity_bytes`. The plan
   currently keeps the plain constructor.

Summary of what blocks vs. what is decided:

- Item (0) is a **Lance-side runtime requirement** -- the Ray-side
  `--mode sharded` rewrite has landed with actor-side ``hasattr``
  gates around `compute_partition_ids` / `search_partitions`. A
  pylance build that ships both wrappers runs the sharded path
  end-to-end; a build that does not raises a clear `RuntimeError`
  on first use. The driver-level pre-block was lifted (see Status
  above). The Lance patch is still required for any actual sharded
  benchmarking; verifying it against the pylance build under test is
  recorded in
  [`lance-v6-api-verification.md`](./lance-v6-api-verification.md).
- Item (1) is an **optional Lance-side improvement** -- the rewrite
  is buildable without it (the plan defaults to L2-directory-only
  observability); if the PyO3 wrapper for `DistributedCacheStats`
  is added, the driver gets richer counters but the schemas above
  do not change.
- Items (2) and (3) are **internal decisions** for the rewrite PR;
  neither requires a Lance change.
- The previous "verify `index_cache_stats()`" open question is
  now decided: it does not exist in Lance 6.0 Python; the plan
  adopts `session.size_bytes()` everywhere (see *Output schema and
  helper changes*).
