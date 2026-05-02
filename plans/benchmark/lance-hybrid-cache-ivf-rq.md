# Lance Hybrid Cache IVF_RQ Benchmark

## Status

Implemented under `benchmarks/lance_hybrid_cache/`.

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

- `hybrid`: `policy="hybrid_tiered"`. The actor fills foyer L1 up to the
  configured prewarm RAM budget and forces the remainder of its partition slice
  to L2.
- `moka`: `policy="moka_ram_cap"`. The actor loads partitions in deterministic
  order until the DRAM budget is full, then stops. It does not churn through the
  rest of the slice.
- `no-cache`: sharded prewarm registers ownership only.

For hybrid, the RAM budget is the actor's foyer L1 budget:

- With `--codecless-mb N`, foyer L1 is `--dram-gb - N`.
- Without `--codecless-mb`, Lance's default 90/10 hybrid split applies — foyer
  L1 gets ~90% of `--dram-gb`, the codec-less embedded Moka gets the
  remaining ~10%.
- `--prewarm-ram-fraction` can scale the requested foyer L1 target down when
  foyer shard skew would otherwise spill a subset of nominal L1 admissions.

The driver logs per-actor prewarm stats:

- owned partition count
- loaded-to-RAM count
- loaded-to-disk count
- skipped-existing count
- Moka `stopped_before`
- decoded RAM bytes
- serialized disk bytes
- foyer spill count
- post-prewarm cache entries/bytes
- per-actor L2 footprint for hybrid

In sharded mode, random query warmup is not used to populate partition entries.
The deterministic prewarm covers worker partition entries and worker top-level
vector index objects. The coordinator still runs one `compute_partition_ids`
call on a throwaway query so its own top-level vector index is opened outside
the measurement timer.

## Partition Residency Verification

This corresponds to `plans/task-check-partition-prewarm.md`.

The implemented check lives in:

- `benchmarks/lance_hybrid_cache/check_partition_residency.py`
- `HybridSearchActor.check_partition_residency(...)` in
  `benchmarks/lance_hybrid_cache/distributed_actor.py`
- phase 1.5 and phase 2.5 of `run_distributed_bench.py`

The probe runs per actor over the partition ids that actor is expected to own.
For each partition it uses Lance's deterministic prewarm API as a no-load DRAM
probe:

```python
dataset.prewarm_vector_cache(
    index_name,
    [partition_id],
    policy="moka_ram_cap",
    ram_bytes=0,
)
```

With `ram_bytes=0`, the API reports DRAM-resident partitions via
`skipped_existing` and short-circuits before loading uncached partitions from
storage. This gives a per-partition `in_ram` / `not_in_ram` list without
pulling absent partitions into cache.

Current Python bindings do not expose per-partition L2 residency directly. For
hybrid runs the report includes each actor's L2 directory snapshot and aggregate
session cache stats. Operators cross-reference:

- `not_in_ram`
- prewarm-time `loaded_to_disk`
- L2 directory size/file count

to confirm that non-DRAM partitions are accounted for by L2 placement rather
than missing cache state.

Default behavior:

- Post-measure residency runs automatically for `--prewarm sharded` and
  scenarios other than `no-cache`, before actors are closed.
- Post-prewarm/pre-measure residency is opt-in with
  `--pre-measure-residency-probe`, because it walks the cache access path once
  per owned partition and can affect replacement-policy state immediately
  before measurement.

When both probes run, the driver prints a residency-shift summary:

- `stayed_in_ram`
- `evicted`
- `promoted_into_ram`

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
| `--prewarm-ram-fraction` | Scales hybrid foyer L1 prewarm target to avoid shard-skew spills. |
| `--pre-measure-residency-probe` | Also run the residency probe after prewarm and before measurement. |
| `--actor-resource` | Pin worker actors to Ray nodes with a custom resource. |
| `--coordinator-resource` | Pin the coordinator actor to a Ray node with a custom resource. |
| `--codecless-mb` | Override Lance's default 90/10 foyer/Moka split with an explicit codec-less Moka size; foyer L1 gets the rest of `--dram-gb`. |

## Verification Checklist

For a sharded hybrid real-cluster run, verify:

1. The driver logs `sharded prewarm (deterministic, policy='hybrid_tiered', ...)`.
2. Each actor owns the expected `num_partitions / num_actors` partition count.
3. Hybrid prewarm reports non-zero `loaded_to_ram` and `loaded_to_disk`.
4. Each hybrid actor has a non-empty `<nvme-dir>/actor-<i>` L2 directory.
5. The coordinator routing warmup runs before measurement.
6. Measurement output includes coordinator aggregate latency and per-worker
   cache stats.
7. Post-measure partition residency prints per-actor `in_ram` and `not_in_ram`
   lists.
8. If `--pre-measure-residency-probe` is enabled, the run also prints
   post-prewarm residency and a residency-shift summary.

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
