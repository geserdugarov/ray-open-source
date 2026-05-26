# Lance hybrid-cache (NVMe + DRAM) IVF_RQ benchmark

Measures vector-search latency for Lance's two-tier hybrid cache (foyer-backed
L1 DRAM + L2 NVMe) against two baselines: *no cache* and *Moka DRAM-only*.

> **Status — Lance 6.0 distributed-cache port.** Scenario specs and
> `build_session` target `Session.with_distributed_cache` (the v6
> replacement for `with_hybrid_cache` / `with_hybrid_cache_advanced`); the
> CLIs default to `--scenario distributed` and accept `hybrid` as a
> deprecated alias. `--codecless-mb`, `--prewarm-ram-fraction`, and
> `--l2-gb` are accepted for back-compat but ignored under v6. Session
> stats use `session.size_bytes()` only — `index_cache_stats()` is gone
> in v6. **`--mode sharded` AND `--prewarm sharded` are hard-failed at
> startup** because the sharded measure path (`measure_sharded`) needs
> PyO3 wrappers for `compute_partition_ids` / `search_partitions`,
> which are Rust-only at the pinned Lance 6.0 commit (see
> [`plans/benchmark/lance-v6-api-verification.md`](../../plans/benchmark/lance-v6-api-verification.md)).
> Use `--mode replicated --prewarm forced` (every actor calls
> `dataset.prewarm_index(name)` in parallel — the v6 strict prewarm path)
> or `--prewarm natural` / `--prewarm none`. The per-partition residency
> probe is also disabled for `--scenario distributed` — there is no v6
> no-load primitive yet; the L2 directory snapshot remains as the
> placement cross-check. The narrative below still uses v4 hybrid terminology
> (foyer L1 / L2 capacity / 90-10 split) that the v6 distributed cache
> replaces with the per-actor metadata-L1 + partition-L1 + NVMe-L2 model;
> the full README rewrite is tracked in
> [`plans/benchmark/lance-distributed-cache-6.0.md`](../../plans/benchmark/lance-distributed-cache-6.0.md).

- **Dataset**: 10M × 1024-d f32 embeddings
- **Index**: IVF_RQ, 3000 partitions, `num_bits=8` (~10 GB total)
- **Scenarios**: `no-cache` | `moka` (4 GiB DRAM) | `distributed` (per-actor
  metadata L1 + partition L1 + NVMe L2; `hybrid` is a deprecated alias for
  `distributed`). Both `moka` and `distributed` get a comparable per-actor
  DRAM budget — the distributed scenario's extra resource is the NVMe L2
  tier. Under Lance 6.0 the per-actor L2 subdirectory is created
  in-process by the actor (`HybridSearchActor.__init__` and
  `ScenarioActor.run`) just before constructing the session, so driver
  hosts do not need write access to worker-local NVMe paths.
- **Top-K**: 10, 100, 1000 (`nprobes=32` fixed)
- **Storage**: MinIO on localhost, with `tc netem` adding 15 ms on MinIO's port only
  (Lance does not support HDFS; this simulates a remote object store)
- **Orchestration**: Ray standalone cluster on the local node, one actor per scenario.
  The single-actor driver, distributed driver, actors, helpers, and host-specific
  defaults all live in this directory.

## One-time setup

```bash
# Activate the same venv used to build Ray (see ../../python/venv)
source "$HOME/git/ray-open-source/python/venv/bin/activate"

cd "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache"

# 1. Lance + bench deps
# Ensure your lance-open-source checkout is on a branch where `python/Cargo.toml`
# enables `hybrid-cache` on the `lance` dependency (e.g. `private-cache-4.0`
# at commit 196f5dae or later). On those branches the feature is compiled
# in unconditionally and pip needs no extra build flags.
pip install -e "$HOME/git/lance-open-source/python"
# After this command:
# - `import lance` resolves to lance-open-source/python/python/lance/ (live —
#   edits to .py files take effect with no reinstall).
# - The native `_lance.so` exposes `lance.Session.with_distributed_cache(...)`
#   and `lance.Session.close()` under the Lance 6.0 distributed-cache crate.
#   (The v4 `with_hybrid_cache` / `with_hybrid_cache_advanced` factories
#   are gone in v6; see `plans/benchmark/lance-distributed-cache-6.0.md`.)
# - Editing Rust files under lance-open-source/python/src/ requires re-running
#   this command (or `maturin develop`) to recompile. Python edits do not.
#
# Build profile: `pip install -e ...` goes through maturin's PEP 517/660 path,
# which builds in **release** mode by default. The resulting `_lance.so` is
# optimized and suitable for benchmarking as-is — no `--release` flag needed.
# If you later iterate on Rust code with `maturin develop` or `maturin build`,
# those default to **debug** (unoptimized, debug_assertions on); you MUST pass
# `--release` before re-running benchmarks or the latency numbers are garbage.
pip install -r requirements.txt

# 2. NVMe L2 directory (must be on the real NVMe mount, NOT /tmp)
sudo mkdir -p /mnt/nvme/lance-l2 && sudo chown "$USER" /mnt/nvme/lance-l2

# 3. MinIO + bucket
# Default binds MinIO to 127.0.0.1 for single-host runs. For a real cluster,
# expose it on the coordinator's LAN interface so worker nodes can reach
# `http://<MINIO_HOST>:9000`:
#   MINIO_BIND_ADDR=0.0.0.0 docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml up -d
# `make_bucket.sh` launches a one-shot `mc` container. If this host cannot
# pull from Quay, preload `quay.io/minio/mc:latest` or set `MC_IMAGE` to an
# already-loaded MinIO client image.
bash infra/make_bucket.sh

# 4. Loopback delay on port 9000 only
sudo bash infra/netem_up.sh
# Revert (removes the tc qdisc/filter on lo, restoring sub-ms loopback):
#   sudo bash infra/netem_down.sh
# Run this before rebooting, before any non-benchmark workload that touches
# port 9000 on lo, and as part of teardown (see "Teardown" section below).
```

Verify:

```bash
# MinIO's /minio/health/live returns 200 with an EMPTY body, so plain
# `curl -fsS` prints nothing on success — print the status code explicitly:
curl -fsS -o /dev/null -w "%{http_code}\n" http://127.0.0.1:9000/minio/health/live
# expect: 200
# Expect ~60-90ms wall time: 15ms netem delay applies PER PACKET, and one
# curl request needs 2 RTTs (TCP handshake + GET/response) plus process
# startup. The benchmark reuses connections, so its per-request latency
# floor is ~30ms (one RTT), not 60ms+.
# Unrelated loopback traffic (no port 9000) should remain sub-ms.
time curl -sS -o /dev/null http://127.0.0.1:9000/minio/health/live
```

## Running

```bash
# Activate the Ray venv if it isn't already active
source "$HOME/git/ray-open-source/python/venv/bin/activate"

# First run: creates the 10M dataset + IVF_RQ index on MinIO (~1-2 hours)
# Subsequent runs reuse the URI automatically.
#
# `--reuse-l2` is required to observe the warm-L2 steady-state (see
# "Expected results" below). Without it `hybrid_l2_dir` timestamps each
# repeat's L2 directory and every hybrid repeat starts cold.
python run_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --dram-gb 4 --l2-gb 30 --nvme-dir /mnt/nvme/lance-l2 \
    --k-list 10,100,1000 --nprobes 32 \
    --warmup-queries 1024 --measure-queries 5000 \
    --repeats 3 --reuse-l2

# Plot
python plot_results.py
```

Results land in `out/`:

- `results.jsonl` — one record per scenario × repeat with pre/post
  `size_bytes` snapshots (the v6 stats surface — v4 hit/miss/entry
  counters are gone), per-k latency arrays, `duration_s`
- `summary.csv` — one row per scenario × repeat × k with p50/p95/p99/mean/n
  and `session_size_bytes_pre` / `_post` / `_delta`
- `plots/{latency_cdf,p99_bars,l1_size}.png` — `l1_size.png` is the v6
  replacement for the v4 `hit_ratio.png` panel, charting per-scenario
  `Session.size_bytes()` pre vs. post the measure phase (the v4
  hit-ratio signal has no v6 analog).

## v6 DRAM split (`--metadata-l1-mb` / `--partition-l1-mb`)

Under Lance 6.0 the distributed scenario's per-actor DRAM is split
between two tiers, both sized by explicit CLI flags rather than the v4
90/10 implicit split:

- **metadata L1** (`--metadata-l1-mb`, default 64 MiB) — caches
  `IvfIndexState`, `IndexMetadata`, `FragReuseIndex`, `ScalarIndexDetails`,
  and the other per-query routing structures. Sizing it too small defeats
  the per-query routing path; warn floor is 4 MiB.
- **partition L1** (`--partition-l1-mb`, default 1024 MiB) — caches
  decoded IVF partitions on the read path. Pass `0` to disable
  (every partition decode then hits L2 / object storage).

The v6 L2 tier is sized by the actor's NVMe filesystem; v6 has no L2
capacity bookkeeping, so the v4 `--l2-gb` flag is ignored (operators
size the partition slice against `<nvme-dir>` directly). `--codecless-mb`
is also accepted-and-ignored — there is no codec-less Moka in v6.

The `moka` baseline still uses `--dram-gb` to size a single in-process
Moka cache via `Session(index_cache_size_bytes=...)`; the v6 metadata
budget for `moka` / `no-cache` is set via `--metadata-mb` (the legacy
session-wide `metadata_cache_size_bytes` knob), not `--metadata-l1-mb`
(which only affects the distributed scenario's `with_distributed_cache`
constructor).

```bash
# Per-actor v6 budgets: ~1 GiB partition L1 + 64 MiB metadata L1 in
# DRAM, plus the NVMe L2 tier sized by `--nvme-dir`'s filesystem.
python run_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --dram-mb 4096 --metadata-l1-mb 64 --partition-l1-mb 1024 \
    --nvme-dir /mnt/nvme/lance-l2 \
    --k-list 10,100,1000 --nprobes 32 \
    --warmup-queries 1024 --measure-queries 5000 \
    --repeats 3 --reuse-l2
```

Requires a Lance 6.0 build that exposes `Session.with_distributed_cache`
(see `plans/benchmark/lance-v6-api-verification.md`).

## Distributed mode (multi-actor)

`run_bench.py` exercises a **single** Ray actor end-to-end. To measure how
the hybrid cache behaves under Ray-style fan-out — multiple actors with
independent Sessions, parallel prewarm, and queries split across workers —
use `run_distributed_bench.py`.

The driver supports two cache topologies. Pick based on whether you want
each actor to hold the whole index or just its slice:

- **`--mode replicated` (CLI default)** — every actor caches the entire
  index and answers queries independently; the driver round-robin splits
  queries across actors. Best paired with `--prewarm forced` for each-
  actor-stands-alone latency at full recall.
- **`--mode sharded`** — each actor caches only its `1/N` partition slice
  and a `CoordinatorActor` scatters per-query to owning workers and merges
  partial top-K into a global top-K (full recall, per-query wall-time
  bounded by the slowest fan-out leg). Best when you want **per-actor
  partition-sharded caches *and* a meaningful (full-recall) latency
  comparison.** **Shown in the example below**; mechanics in
  [Coordinator-driven sharded mode](#coordinator-driven-sharded-mode).

**Implicit flag interactions** worth knowing before reading the example:

- `--mode sharded` forces `--prewarm sharded` (the coordinator can only
  route to workers that own a known partition slice). The driver logs the
  override and rewrites whatever `--prewarm` you passed.
- `--warmup-queries N` is consumed by `--prewarm natural` only. In
  `--mode sharded`, deterministic `--prewarm sharded` populates every
  cache namespace the measurement path touches (codec-bearing IVF
  partition entries via `prewarm_vector_cache`, plus the codec-less
  top-level vector index objects opened on each worker). The driver
  then runs a one-shot `compute_partition_ids` call on the coordinator
  to force-open its own top-level vector index outside the measure
  timer (the coord opens the index lazily on first centroid routing,
  so without this the first measured query pays the index-open cost).
  The previous coordinator-driven random-query warmup is no longer
  run; if `--warmup-queries N > 0` is set it is logged and ignored.
- In `--mode sharded`, per-actor cache hits/misses are reported for the
  measure phase only: the driver snapshots counters before measure and
  reports the delta, so warmup MinIO loads do not pollute hit_ratio.

Run hybrid and moka as two sequential invocations against the same
MinIO dataset+index. Run 1 omits `--skip-setup` so `ensure_dataset(...)`
builds the dataset + IVF_RQ index if absent (idempotent — reuses if a
prior `run_bench.py` already populated the bucket); Run 2 keeps
`--skip-setup` and a distinct `--out-dir` so the per-scenario
`distributed_results.jsonl` files don't overwrite each other.

```bash
# Per-actor budgets — total resource use is num_actors × dram-gb / l2-gb.
# Under --mode replicated every actor caches the full slice and answers
# queries independently; the driver round-robins queries across actors.
#
# v6 port note: `--mode sharded` and `--prewarm sharded` are hard-failed
# at startup (return code 2) because the sharded measure path needs
# Lance PyO3 wrappers for `compute_partition_ids` / `search_partitions`
# that are not yet shipped. Use `--prewarm forced` (every actor calls
# `dataset.prewarm_index(name)` in parallel) for the v6-supported
# replicated topology. The historical sharded narrative below ("each
# actor caches only ~1/num_actors of the index") returns when those
# wrappers land — see plans/benchmark/lance-distributed-cache-6.0.md.

# Run 1 — distributed replicated (creates dataset + index in MinIO if
# absent). Forced prewarm has every actor call
# `dataset.prewarm_index(name)` in parallel; under the v6 distributed
# cache that writes one `part-ivf-<id>.bin` per partition under each
# actor's `<nvme-dir>/actor-<i>/v1/...` and admits decoded partitions
# into the in-process partition-L1 tier up to its cap. Measure-phase
# queries hit local L2 (and partition-L1 when warm).
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario distributed \
    --num-actors 4 --dram-gb 1 --l2-gb 8 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --mode replicated --prewarm forced \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 100 \
    --out-dir out/distributed-replicated \
    2>&1 | tee bench-distributed-replicated.log

# Run 2 — moka baseline. Reuses the dataset+index from Run 1. --l2-gb
# and --nvme-dir omitted because moka is pure DRAM. Natural warmup
# splits the warmup queries across actors and lets each Moka cache
# converge under its `--dram-gb` cap (v6 has no moka_ram_cap policy).
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario moka \
    --num-actors 4 --dram-gb 1 \
    --mode replicated --prewarm natural \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 256 --measure-queries 100 \
    --skip-setup \
    --out-dir out/moka-replicated \
    2>&1 | tee bench-distributed-moka-replicated.log
```

Key knobs unique to the distributed driver:

| Flag | Purpose |
|---|---|
| `--mode replicated` (default) | Every actor sees every partition; the driver fans out queries round-robin, each actor probes independently. Use with `--prewarm {natural,forced,sharded,none}`. Prewarm cost grows linearly with `--num-actors` (each actor pulls every partition from MinIO). |
| `--mode sharded` | Spin up a `CoordinatorActor` that owns the IVF centroid step and a `partition_id %% num_actors` mapping. Per query, the coord routes the `--nprobes` ids to their owning workers, gathers per-worker partial top-K via `search_partitions`, and merges to a global top-K — full recall, but per-query wall-time is bounded below by the slowest fan-out leg. Forces `--prewarm=sharded` (workers must own a known slice before routing). See [Coordinator-driven sharded mode](#coordinator-driven-sharded-mode). |
| `--num-actors N` | Spawn N parallel `HybridSearchActor`s. Each gets `<nvme-dir>/actor-<i>` as its L2 subdir so foyer's exclusive flock is uncontended. |
| `--prewarm forced` | Each actor calls `dataset.prewarm_index(<index-name>)` in parallel — exercises Lance's forced-prewarm path. |
| `--prewarm natural` | Splits `--warmup-queries` across actors (default; each cache state diverges). |
| `--prewarm sharded` | Actor `i` deterministically prewarms partitions `{i, i+N, i+2N, …}` via `dataset.prewarm_vector_cache(...)`. The driver picks the placement policy from `--scenario`: `hybrid_tiered` for hybrid (place every owned vector partition into L2, leave foyer L1 cold — query traffic later promotes decoded partitions out of L2 into volatile L1, with no L1→L2 writeback path), `moka_ram_cap` for moka (load until `--dram-gb` is full, then stop), no-op for `no-cache`. Under `--mode replicated` the measure phase uses `compute_partition_ids` + `search_partitions` so each actor only searches its owned slice (per-query recall is partial — see the sharded caveat below). Under `--mode sharded` the same prewarm feeds the coordinator topology. Per-actor prewarm cost stays flat as `--num-actors` grows. Requires lance ≥ commit `14f9e2862`. |
| `--prewarm none` | Skip prewarm; first measure query is cold. |
| `--warmup-queries N` | Used by `--prewarm natural` (split across actors). Under `--mode sharded` it is ignored: deterministic sharded prewarm already populates every cache namespace the measure path touches. |
| `--dram-gb` | **Per-actor** DRAM budget for the `moka` scenario. Ignored for `--scenario distributed` (whose DRAM is sized by `--metadata-l1-mb` + `--partition-l1-mb`). |
| `--metadata-l1-mb` | **Per-actor** v6 metadata-L1 budget (MiB) for the distributed scenario. Caches `IvfIndexState`, `IndexMetadata`, etc.; default 64. See [v6 DRAM split](#v6-dram-split---metadata-l1-mb----partition-l1-mb). |
| `--partition-l1-mb` | **Per-actor** v6 decoded-partition-L1 budget (MiB) for the distributed scenario. Pass `0` to disable; default 1024. |
| `--l2-gb` | Deprecated v4 hybrid knob. v6 has no L2 capacity bookkeeping — size the actor's NVMe filesystem yourself. Accepted but ignored. |
| `--codecless-mb N` | Deprecated v4 hybrid knob. The v6 distributed cache has no codec-less Moka tier; passing this flag prints a warning and is otherwise ignored. |
| `--prewarm-ram-fraction F` | Legacy no-op. Previously scaled the hybrid foyer L1 budget used by `policy='hybrid_tiered'` when hybrid prewarm filled L1 first and the rest spilled to L2. The current `hybrid_tiered` policy places every owned partition in L2 and never admits to L1 during prewarm, so there is no L1 budget to scale; values other than `1.0` are flagged as ignored. |
| `--pre-measure-residency-probe` | Also run the partition residency probe immediately after deterministic prewarm and before measurement. It is off by default because it walks the cache access path once per owned partition and can affect replacement policy state before the measured workload. Without it, hybrid shift reporting uses the validated cold-L1 `hybrid_tiered` baseline; moka shift reporting is skipped because there is no pre-measure L1 snapshot. |
| `--actor-resource NAME` | Optional Ray custom resource required by each `HybridSearchActor`; use this in a real cluster to pin workers to actor nodes. Each actor reserves 1.0 of the resource. |
| `--coordinator-resource NAME` | Optional Ray custom resource required by the `CoordinatorActor` in `--mode sharded`; use this in a real cluster to pin the coordinator to the head/coordinator node. |

The summary reports aggregate latency percentiles (across all actors)
and per-actor rows; per-actor cache footprint is the v6
`Session.size_bytes()` value (the v4 hit-ratio columns are gone — Lance
6.0 does not expose hit/miss counters). `<out-dir>/distributed_results.jsonl`
has one record
per actor. Whenever `--prewarm` is `forced` or `sharded` and
`--scenario` is not `no-cache`, the driver also writes
`<out-dir>/partition_residency.jsonl` after measurement; with
`--pre-measure-residency-probe`, the same file contains both `post-prewarm`
and `post-measure` labels so you can compare cache movement across the
query run. The walk happens on the actor via Ray RPC so it works in
real-cluster topologies where each actor's L2 dir is on local NVMe.
Residency rows carry the v6 aggregate-only schema:
`owned_count`, `in_l2`, `missing`, `l2_size_bytes_total`, `l2_file_count`,
`l2_prefix_dirs`, and `l1_size_bytes_at_probe` (from `Session.size_bytes()`).
The L2 half is exact under v6 — file presence under
`{l2_dir}/v1/{sanitize(prefix)}/part-ivf-{id}.bin` one-to-one maps to
L2 residency, so the v4 `l2_residency_source` / `prewarm_validated_owned_set`
inference fields are gone. The walk is scoped to a single live prefix
dir under `v1/`; if two or more coexist (e.g. a stale dir from a
previous bench shares the L2 path) the residency claim is refused
(`in_l2=[]`, `missing=owned`) and the conflicting names are listed in
`l2_prefix_dirs` so the operator notices instead of seeing a false
"healthy" report. The L1 half is reported as a session-wide byte total
rather than per-partition identity — Lance 6.0 has no no-load L1 probe
(the v4 `in_l1` / `not_in_l1` lists are dropped). See
[`check_l2_residency.py`](check_l2_residency.py) for the walk + schema.

Caveats:

- The example above is a single-host logical distributed run: all actors share
  one NVMe and one MinIO process. For a physical 3-node run, use
  [Real 3-node Ray cluster with separate MinIO](REAL_CLUSTER.md).
- Forced prewarm cost grows linearly with `--num-actors` because every
  actor independently fetches every partition from MinIO. On a single-node
  cluster the kernel page cache helps, but it's still wasteful — use
  `--prewarm sharded` to partition the prewarm work by
  `partition_id % num_actors` so per-actor prewarm cost stays flat.
- `--mode replicated --prewarm sharded` trades per-query recall for
  prewarm cost. Each actor only searches its 1/N slice of partitions
  and the bench does **not** merge top-K across actors, so the latency
  table reflects per-actor work, not full-recall query latency. Use
  `--mode sharded` instead when you want the same partition-sharded
  caches but with full recall via cross-actor merge (see below).
  Look at the `routed_owned≈X` column in the per-actor table to see
  how many of the `--nprobes 32` routed partitions actually landed on
  each actor on average.

## Coordinator-driven sharded mode

`--mode sharded` adds a `CoordinatorActor` that holds the IVF centroid
step and a partition→actor mapping (`partition_id % num_actors`). For
each query the coord:

1. Computes the routed partition ids via `dataset.compute_partition_ids`
   (centroid-only matvec, sub-millisecond, no partition I/O).
2. Groups the `--nprobes` ids by owning actor (round-robin mod N).
3. Scatters `search_partitions(query, ids_for_actor_i, k)` in parallel
   to each non-empty bucket (skips actors with no owned ids).
4. Merges the per-worker `(distance, row_id)` partial top-K into a
   global top-K and returns it.

Workers serve `search_partitions` on demand and own only their assigned
partition slice — per-actor L2 footprint is `index_size / num_actors`
and prewarm cost is flat in `--num-actors` (each partition is fetched
from MinIO exactly once across the cluster). The aggregate latency table
is owned by the coord (centroid + scatter + slowest worker + merge) and
is directly comparable across scenarios. The two-run hybrid-vs-moka
example in [Distributed mode (multi-actor)](#distributed-mode-multi-actor)
above exercises this topology directly.

Mode-specific output:

- The summary prints a `coord per-query mean: centroid=… ms scatter=…
  ms merge=… ms workers_invoked≈X/N routed_partitions≈Y/nprobes` line
  so you can see where wall-time goes.
- The per-actor table shows `hit=…  entries=…  bytes=…  owned=…
  calls_handled=…` — the worker doesn't time per-query work in this
  mode, but cache health and per-actor RPC count make fan-out balance
  visible.
- `<out-dir>/distributed_results.jsonl` has the coordinator's aggregate row
  first (`actor_id="coordinator"`, full `latencies_by_k`) followed by
  one row per worker carrying cache stats only.

Caveats specific to `--mode sharded`:

- Per-query latency is bounded below by the slowest worker leg, not
  by `1/N` of the replicated path. With `--nprobes 32` and
  `--num-actors 4` the slowest leg can carry more than `32/4` partitions
  due to variance in `id % N` distribution. The bench's
  `coord per-query mean: scatter=…` line is what you watch.
- Single-replica only (R=1). If a worker dies, its partitions are
  unavailable; v1 has no failover. Re-run after restarting the cluster.
- The coordinator opens the dataset only for centroid metadata — its
  Session is DRAM-only with a 64 MiB index cache; it does not use the
  NVMe L2 tier and does not contend with workers for `<nvme-dir>` flocks.

## Expected results

- `no-cache.p99 > moka.p99` — always. MinIO through 15 ms netem is the floor.
- `hybrid.p99 < moka.p99` — **steady-state**. With deterministic sharded
  prewarm every owned vector partition is placed in L2 before measurement,
  so the first measured query already reads from local NVMe instead of
  MinIO and the hybrid steady-state win shows up immediately. The
  `run_bench.py` single-actor path still relies on `--reuse-l2` across
  repeats because its prewarm comes from random warmup queries rather
  than deterministic L2 placement — the first repeat there reads as cold.
- `session_size_bytes`: `Session.size_bytes()` (the v6 substitute for v4
  hit ratios) grows as partition entries land in the partition-L1 tier.
  Distributed runs settle near `--partition-l1-mb` once the working set
  is warm; moka cycles within `--dram-gb` as the Moka LRU thrashes
  against the cap. Cache effectiveness is judged from measure-phase
  latency vs. the `no-cache` baseline — Lance 6.0 does not expose
  hit-ratio counters.

## Teardown

```bash
sudo bash infra/netem_down.sh
docker compose -f infra/docker-compose.yml down -v
```

## Pitfalls

- **`session.close()` releases the L2 flock** — close the session at the
  end of every run so foyer drops `{l2_dir}/lance-hybrid.lock` and the
  next run on the same `l2_dir` can attach. Vector-partition durability
  does *not* depend on close: deterministic hybrid prewarm with
  `wait_for_disk=True` already blocks until foyer's storage flusher
  reports each partition durable in L2, and the no-vector-L1-writeback
  policy means partitions never flow back from L1 to L2 during query
  traffic. The local `ScenarioActor.run` and `HybridSearchActor.close`
  already release the session; don't wrap them in a bare try that
  swallows exceptions or the flock stays held until process exit.
- **`l2_dir` is exclusive per process** — foyer flocks `{l2_dir}/lance-hybrid.lock`.
  `hybrid_l2_dir(..., reuse_l2=False)` timestamps each repeat's path so this
  doesn't collide; `--reuse-l2` reuses a single dir and requires only one
  hybrid actor alive at a time.
- **`l2_capacity_bytes ≥ 1 GiB`** — foyer's default 256 MiB block size
  requires ≥ 4× that (1 GiB). 30 GiB is safe.
- **Tmpfs trap** — on WSL2 `/tmp` is RAM-backed; setting `--nvme-dir /tmp/...`
  silently measures DRAM twice. `scenarios.py` rejects `/tmp`, `/var/tmp`,
  `/dev/shm`, and filesystem root, but any large local-disk mount is valid
  (`/mnt/nvme/...`, `/data/fast/...`, etc.).
- **netem scope** — `infra/netem_up.sh` uses a `prio + u32` filter so only
  port 9000 traffic is delayed. Never `tc qdisc add dev lo root netem delay
  15ms` — that slows Ray's actor RPC too.
- **Page cache** — repeats can be unfairly fast because the Linux page cache
  holds parts of the L2 file. Pass `--drop-page-cache` (needs passwordless
  sudo) for clean repeats.
- **MinIO restart** resets the bucket. Use a named volume (the compose file
  does) so dataset+index survive between sessions.

## Tuning knobs

| Flag | Purpose |
|---|---|
| `--num-bits 8` | Index size is ~10 GB; `--num-bits 1` gives ~1.3 GB which fits in 4 GiB moka and makes scenarios indistinguishable. |
| `--dram-gb 4` | DRAM budget for the `moka` scenario. Ignored for `--scenarios distributed` (whose DRAM is sized by `--metadata-l1-mb` + `--partition-l1-mb`). Must be < total index size or moka doesn't thrash. |
| `--metadata-l1-mb 64` | v6 metadata-L1 budget for the distributed scenario (MiB). Default 64; negative values rejected. |
| `--partition-l1-mb 1024` | v6 decoded-partition-L1 budget for the distributed scenario (MiB). Pass `0` to disable; default 1024. |
| `--codecless-mb 64` | Deprecated v4 hybrid knob. The v6 distributed cache has no codec-less Moka tier; passing this flag prints a warning and is otherwise ignored. |
| `--l2-gb 30` | Deprecated v4 hybrid L2-capacity knob. v6 has no L2 capacity bookkeeping; size the NVMe filesystem yourself. Ignored. |
| `--measure-queries 5000` | ≥ 1000 recommended for stable p99 at these latencies. 100 is enough to smoke-test the topology — p50/mean stay informative, but p99 collapses to a single sample. |
| `--nprobes 32` | Keep fixed across scenarios; recall is constant, only latency varies. |
| `--reuse-l2` | Keep the per-actor L2 dir across repeats. Useful once steady-state is proven; otherwise each repeat gets a fresh timestamped subdir for honest cold-start latency. |

## Relationship to the benchmark plan

The current plan lives at
[`../../plans/benchmark/lance-hybrid-cache-ivf-rq.md`](../../plans/benchmark/lance-hybrid-cache-ivf-rq.md).
The original Rust-subprocess architecture is superseded. The implemented path
uses the existing Lance Python bindings for session creation, deterministic
prewarm, partition search, and residency checks, so there is no Rust
subprocess toolchain in the Ray benchmark driver.
