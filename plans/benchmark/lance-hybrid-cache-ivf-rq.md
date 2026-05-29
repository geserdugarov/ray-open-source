# Lance Hybrid Cache IVF_RQ Benchmark

## Status

**Superseded by
[`lance-distributed-cache-6.0.md`](./lance-distributed-cache-6.0.md)
and implementation moved out of this repository.** The Ray-owned
`benchmarks/lance_hybrid_cache/` subtree described below has been
deleted; the live distributed IVF cache actor / coordinator /
example now lives in the `lance-ray` project under
`lance_ray/distributed_cache/` and
`examples/distributed_ivf_cache.py` (commits `17140fe..d0d09ab`).
This document is preserved as the v4-era reference for the
foyer-backed hybrid-cache design (covered by commits `36474d837c`
and `3be01135c3`); its API references
(`Session.with_hybrid_cache`, `Session.with_hybrid_cache_advanced`,
`dataset.prewarm_vector_cache`, `index_cache_stats`, the
`policy='hybrid_tiered'` / `'moka_ram_cap'` knobs, and the
per-partition L1 no-load probe) do not match the v6 Lance Python
surface — read the v6 plan and the `lance-ray` repository for the
live design.

Originally implemented under `benchmarks/lance_hybrid_cache/` (now removed).

This document replaces the older generic benchmark plan. The implemented
benchmark does not use a Rust query subprocess or a separate Rust workspace in
the Ray repository. It uses the Python Lance bindings directly and drives the
benchmark through Ray actors.

## Goal

Measure Lance vector-search latency for an IVF_RQ index with three cache
scenarios:

- `no-cache`: no index cache for partition entries.
- `moka`: DRAM-only Moka cache.
- `hybrid`: foyer-backed hybrid cache with DRAM L1 and NVMe L2.

The benchmark target remains:

- Dataset: 10M rows x 1024-d `f32` vectors.
- Index: IVF_RQ, 3000 partitions, `num_bits=8`.
- Storage: MinIO object store, with optional netem delay for single-host runs.
- Query shape: fixed `nprobes`, same seeded queries across scenarios.

## Implemented Layout

```text
benchmarks/lance_hybrid_cache/
|-- README.md                         # single-host and distributed usage
|-- REAL_CLUSTER.md                   # 3-node real-cluster runbook
|-- run_bench.py                      # single-actor driver
|-- run_distributed_bench.py          # multi-actor driver
|-- distributed_actor.py              # Ray worker + coordinator actors
|-- check_partition_residency.py      # residency probe library/CLI
|-- l2_inspect.py                     # actor-local L2 directory snapshots
|-- scenarios.py                      # cache/session scenario construction
|-- _hybrid_cache_helpers.py          # dataset setup and shared helpers
|-- bench_hybrid_cache_ivf_rq.py      # dataset/index creation helpers
|-- plot_results.py                   # result plotting
`-- infra/
    |-- docker-compose.yml
    |-- make_bucket.sh
    |-- netem_up.sh
    |-- netem_down.sh
    `-- ship_real_cluster.sh
```

## Single-Actor Driver

`run_bench.py` is the local baseline path. It opens one Ray actor per scenario,
uses Lance's Python session APIs, runs seeded warmup/measurement queries, and
writes:

- `out/results.jsonl`
- `out/summary.csv`
- `out/l2_inventory.csv`
- `out/plots/*`

The single-actor driver is useful for proving the local cache comparison and
warm-L2 behavior. It is not the real-cluster partition-sharded topology.

## Distributed Driver

`run_distributed_bench.py` supports two topologies:

- `--mode replicated`: every actor can see every partition. The driver splits
  queries across actors. This is useful for actor-level cache divergence tests,
  but sharded prewarm in this mode only searches each actor's local slice and
  does not merge full-recall top-K.
- `--mode sharded`: creates a `CoordinatorActor`. Workers own disjoint IVF
  partition slices by `partition_id % num_actors`; the coordinator computes
  routed IVF partition ids, groups them by owner actor, scatters
  `search_partitions(...)`, and merges partial results into full-recall top-K.

For real cluster runs, `--mode sharded` is the intended topology:

- The coordinator/head node owns routing and merge.
- Worker actors own partition cache state.
- Each hybrid worker uses its own actor-local `<nvme-dir>/actor-<i>` L2
  directory.
- Optional Ray custom resources pin workers and coordinator to specific nodes.

## Deterministic Sharded Prewarm

This corresponds to `plans/task-lance-forced-prewarm.md`.

`--mode sharded` forces `--prewarm sharded`. Actor `i` owns:

```text
partition_id % num_actors == i
```

For example, with 2 actors and 3000 partitions:

```text
actor 0: 0, 2, 4, ..., 2998
actor 1: 1, 3, 5, ..., 2999
```

Each actor calls Lance's deterministic vector-cache prewarm API:

```python
dataset.prewarm_vector_cache(
    index_name,
    partition_ids,
    policy=...,
    ram_bytes=...,
    wait_for_disk=True,
)
```

The driver chooses policy from scenario:

- `hybrid`: `policy="hybrid_tiered"`. The actor places every owned vector
  partition into foyer L2 (NVMe) and leaves L1 cold. The `ram_bytes` argument
  is accepted for source compatibility but ignored — the orchestrator does
  not admit anything to L1 during prewarm. Vector partitions are later
  promoted into volatile L1 by ordinary query traffic; L1 evictions drop the
  entry from RAM only (the L2 entry from prewarm already exists, so there is
  no L1→L2 writeback).
- `moka`: `policy="moka_ram_cap"`. The actor loads partitions in deterministic
  order until the DRAM budget is full, then stops. It does not churn through the
  rest of the slice.
- `no-cache`: sharded prewarm registers ownership only.

For hybrid, foyer L1 is sized from the per-actor DRAM split:

- With `--codecless-mb N`, foyer L1 is `--dram-gb - N`.
- Without `--codecless-mb`, Lance's default 90/10 hybrid split applies — foyer
  L1 gets ~90% of `--dram-gb`, the codec-less embedded Moka gets the
  remaining ~10%.

`--prewarm-ram-fraction` is a legacy backward-compatibility flag. It
previously scaled the foyer L1 target when hybrid prewarm filled L1 first
and the rest spilled to L2; the current hybrid_tiered policy admits nothing
to L1 during prewarm, so there is no L1 budget to scale. Values other than
`1.0` print a warning and are otherwise ignored.

The driver logs per-actor prewarm stats:

- owned partition count
- loaded-to-RAM count (always 0 for hybrid)
- loaded-to-disk count
- skipped-existing count (L2-resident-already for hybrid)
- Moka `stopped_before` (n/a for hybrid)
- decoded RAM bytes (always 0 for hybrid)
- serialized disk bytes
- post-prewarm cache entries/bytes
- per-actor L2 footprint for hybrid (snapshot taken post-prewarm and
  diffed against a post-measure snapshot to surface unexpected L2
  growth)

The driver hard-fails if a hybrid prewarm reports `loaded_to_ram > 0`,
`disk_bytes_unknown_spills > 0`, or if `loaded_to_disk + skipped_existing`
does not match the owned partition count — those indicate a pylance
build that still admits vector partitions to L1 during hybrid prewarm,
which the no-vector-L1-writeback policy explicitly forbids.

In sharded mode, random query warmup is not used to populate partition entries.
The deterministic prewarm covers worker partition entries (in L2) and worker
top-level vector index objects (in the codec-less Moka tier). The coordinator
still runs one `compute_partition_ids` call on a throwaway query so its own
top-level vector index is opened outside the measurement timer.

## Partition Residency Verification

This corresponds to `plans/task-check-partition-prewarm.md`.

The implemented check lives in:

- `benchmarks/lance_hybrid_cache/check_partition_residency.py`
- `HybridSearchActor.check_partition_residency(...)` in
  `benchmarks/lance_hybrid_cache/distributed_actor.py`
- phase 1.5 and phase 2.5 of `run_distributed_bench.py`

The probe runs per actor over the partition ids that actor is expected to own.
For each partition it uses Lance's deterministic prewarm API as a no-load L1
probe:

```python
dataset.prewarm_vector_cache(
    index_name,
    [partition_id],
    policy="moka_ram_cap",
    ram_bytes=0,
)
```

With `ram_bytes=0`, the API reports L1-resident partitions via
`skipped_existing` and short-circuits before loading uncached partitions from
storage. This gives a per-partition `in_l1` / `not_in_l1` list without pulling
absent partitions into cache.

Per-partition L2 residency — `IvfIndexSearcher::partition_is_in_l2` exists
in the Rust side but is not yet bound to pylance. Until the binding lands the
report fills the `in_l2` field from each actor's owned slice for hybrid
sessions: deterministic `hybrid_tiered` prewarm with `wait_for_disk=True`
places every owned partition in L2 by construction, and the
no-vector-L1-writeback policy means subsequent query traffic cannot remove
a partition from L2. The actor flags this with `l2_probe_supported=False`
so downstream tooling can mark `in_l2` and `missing` as inferred values.
The report also includes each actor's L2 directory snapshot (file count +
on-disk bytes); the driver diffs pre/post-measure snapshots to surface
unexpected L2 growth that would invalidate the inference.

Default behavior:

- Post-measure residency runs automatically for `--prewarm sharded` and
  scenarios other than `no-cache`, before actors are closed.
- Post-prewarm/pre-measure residency is opt-in with
  `--pre-measure-residency-probe`, because it walks the cache access path once
  per owned partition and can affect replacement-policy state immediately
  before measurement.

When both probes run, the driver prints a residency-shift summary:

- `stayed_in_l1`
- `evicted_from_l1` (drops from RAM only — no L1→L2 writeback)
- `promoted_into_l1` (L2 → L1 promotion driven by query decode)
- `still_in_l2` (inferred from owned slice until L2 probe is bound)
- `missing_from_l2` (always 0 under inference; flag cluster bugs via the
  L2 directory snapshot delta)

The full results are written as JSON Lines to:

```text
<out-dir>/partition_residency.jsonl
```

The same probe can be run manually while the named actors are still alive:

```bash
python -u check_partition_residency.py \
    --num-actors 2 --num-partitions 3000 \
    --index-name vector_idx \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --label adhoc \
    --out out/hybrid-real-2actors/partition_residency.jsonl
```

## Real-Cluster Shape

The real-cluster runbook is `benchmarks/lance_hybrid_cache/REAL_CLUSTER.md`.

The documented target topology is:

- 1 coordinator/head node.
- 2 worker nodes.
- MinIO reachable from all Ray nodes.
- `--mode sharded`.
- `--num-actors 2`.
- `--actor-resource` to place workers on worker nodes.
- `--coordinator-resource` to place the coordinator on the head node.

For the 10M x 1024-d, 3000-partition, 8-bit RQ run, the index footprint is
roughly 10 GiB. With two actors, each actor owns roughly 5 GiB of partition
data. A typical hybrid command uses:

```text
--dram-gb 1 --codecless-mb 64 --l2-gb 8
```

That leaves roughly 960 MiB foyer L1 per actor and places the remainder of the
actor's slice on actor-local NVMe L2.

## Important Flags

| Flag | Meaning |
|---|---|
| `--mode sharded` | Full-recall coordinator topology with partition-sharded worker caches. |
| `--prewarm sharded` | Deterministic partition-slice prewarm, forced by `--mode sharded`. |
| `--prewarm-ram-fraction` | Legacy no-op (hybrid prewarm no longer admits to L1). Kept for backward compatibility; values ≠ 1.0 are flagged as ignored. |
| `--pre-measure-residency-probe` | Also run the residency probe after prewarm and before measurement. |
| `--actor-resource` | Pin worker actors to Ray nodes with a custom resource. |
| `--coordinator-resource` | Pin the coordinator actor to a Ray node with a custom resource. |
| `--codecless-mb` | Override Lance's default 90/10 foyer/Moka split with an explicit codec-less Moka size; foyer L1 gets the rest of `--dram-gb`. |

## Verification Checklist

For a sharded hybrid real-cluster run, verify:

1. The driver logs `sharded prewarm (deterministic, policy='hybrid_tiered') —
   placing every owned vector partition into L2; foyer L1 remains cold`.
2. Each actor owns the expected `num_partitions / num_actors` partition count.
3. Hybrid prewarm reports `loaded_to_ram == 0`, `loaded_to_disk +
   skipped_existing == owned_count`, and `disk_bytes_unknown_spills == 0`.
   The driver hard-fails if any of these are violated.
4. Each hybrid actor has a non-empty `<nvme-dir>/actor-<i>` L2 directory and a
   `[driver] L2 snapshot (post-prewarm)` log line.
5. The coordinator routing warmup runs before measurement.
6. Measurement output includes coordinator aggregate latency and per-worker
   cache stats.
7. Post-measure partition residency prints per-actor
   `in_l1 / not_in_l1 / in_l2 / missing` lists, with `(l2: inferred)` until
   the per-partition L2 probe is bound to pylance.
8. Post-measure L2 directory snapshot shows zero or minimal byte/file-count
   delta versus the post-prewarm snapshot — vector partition L1 evictions
   under the no-vector-L1-writeback policy must not produce extra L2 writes.
9. Measurement queries promote some vector partitions into L1 (post-measure
   `in_l1 > 0`) without causing any owned partition to disappear from L2.
10. If `--pre-measure-residency-probe` is enabled, the run also prints
    post-prewarm residency and a residency-shift summary
    (`stayed_in_l1 / evicted_from_l1 / promoted_into_l1 / still_in_l2 /
    missing_from_l2`).

## Superseded Design Notes

The original plan described:

- a Rust workspace inside `benchmarks/lance_hybrid_cache/`
- Rust `gen`, `build_index`, and `query` binaries
- a long-lived Rust query subprocess owned by each Ray actor
- JSON-over-stdio query calls

That architecture is no longer the implemented path. Dataset creation, index
creation, session construction, deterministic prewarm, query execution, L2
inspection, and residency checks are all driven from Python through the Lance
bindings and Ray actors.
