# Lance 6.0 distributed-cache (NVMe + DRAM) IVF_RQ benchmark

Measures vector-search latency for Lance 6.0's per-actor distributed
cache (metadata L1 + decoded-partition L1 + NVMe L2) against two
baselines: *no cache* and *Moka DRAM-only*.

The driver targets `Session.with_distributed_cache` — the v6
replacement for the v4 `Session.with_hybrid_cache` /
`with_hybrid_cache_advanced` factories — and the strict
`dataset.prewarm_index(name, partition_ids=...)` path. The v4 `hybrid`
scenario name is accepted as a deprecated alias for `distributed`;
v4-only knobs (`--codecless-mb`, `--prewarm-ram-fraction`, `--l2-gb`)
are accepted but ignored. Session observability comes from
`Session.size_bytes()` only — `index_cache_stats()` is gone in v6, so
the v4 hit/miss/entry counters and the `hit_ratio.png` panel are
replaced by the per-actor `l1_size.png` panel (pre/post `size_bytes`
on the measure phase) and per-query latency vs. the `no-cache`
baseline.

- **Dataset**: 10M × 1024-d f32 embeddings
- **Index**: IVF_RQ, 3000 partitions, `num_bits=8` (~10 GB total)
- **Scenarios**: `no-cache` | `moka` (DRAM-only) | `distributed`
  (per-actor metadata L1 + partition L1 + NVMe L2; `hybrid` is a
  deprecated alias for `distributed`). Both `moka` and `distributed`
  get a comparable per-actor DRAM budget — the distributed scenario's
  extra resource is the NVMe L2 tier. Each actor's L2 subdirectory is
  created in-process by the actor (`HybridSearchActor.__init__` and
  `ScenarioActor.run`) just before constructing the session, so driver
  hosts do not need write access to worker-local NVMe paths.
- **Top-K**: 10, 100, 1000 (`nprobes=32` fixed)
- **Storage**: MinIO on localhost, with `tc netem` adding 15 ms on MinIO's port only
  (Lance does not support HDFS; this simulates a remote object store)
- **Orchestration**: Ray standalone cluster on the local node, one actor per scenario.
  The single-actor driver, distributed driver, actors, helpers, and host-specific
  defaults all live in this directory.

## Pylance build

The benchmark targets Lance 6.0; build pylance from a local checkout
on the v6 distributed-cache branch rather than pulling from PyPI:

- Required: `Session.with_distributed_cache`,
  `Session.invalidate_index_cache`, `Session.size_bytes`, and the
  strict `dataset.prewarm_index(name, partition_ids=...)` path. Pin to
  `../lance-open-source` branch `private-cache-6.0-ver-1` at commit
  `9ebfe4de0` or newer. The PyPI `pylance` wheel is not a substitute —
  the v6 distributed-cache surface is not on PyPI yet.
- Required for `--mode sharded` / `--prewarm sharded`:
  `dataset.compute_partition_ids(name, query, nprobes)` and
  `dataset.search_partitions(name, query, partition_ids, k)`. The
  actor (`HybridSearchActor.measure_sharded`,
  `HybridSearchActor.search_partitions`, `CoordinatorActor.__init__`)
  gates both via `hasattr` and raises a clear `RuntimeError` on first
  use if either wrapper is missing. A pylance build that ships both
  runs end-to-end; a build that is missing either still runs
  `--mode replicated --prewarm forced` (every actor calls
  `dataset.prewarm_index(name)` in parallel) without modification. See
  [`plans/benchmark/lance-v6-api-verification.md`](../../plans/benchmark/lance-v6-api-verification.md)
  for the current binding status against a candidate build.

The v4 plan
[`plans/benchmark/lance-hybrid-cache-ivf-rq.md`](../../plans/benchmark/lance-hybrid-cache-ivf-rq.md)
is **superseded** by
[`plans/benchmark/lance-distributed-cache-6.0.md`](../../plans/benchmark/lance-distributed-cache-6.0.md);
read the v6 plan for the live design.

## One-time setup

```bash
# Activate the same venv used to build Ray (see ../../python/venv)
source "$HOME/git/ray-open-source/python/venv/bin/activate"

cd "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache"

# 1. Lance + bench deps
# Pin the lance-open-source checkout to the Lance 6.0 distributed-cache
# branch (`private-cache-6.0-ver-1` at commit `9ebfe4de0` or newer) so
# the v6 Session / Dataset APIs the driver targets are available.
pip install -e "$HOME/git/lance-open-source/python"
# After this command:
# - `import lance` resolves to lance-open-source/python/python/lance/ (live —
#   edits to .py files take effect with no reinstall).
# - The native `_lance.so` exposes `lance.Session.with_distributed_cache(...)`,
#   `lance.Session.invalidate_index_cache(...)`, `lance.Session.size_bytes()`,
#   and the strict `dataset.prewarm_index(name, partition_ids=...)` path
#   under the Lance 6.0 distributed-cache crate. The v4
#   `with_hybrid_cache` / `with_hybrid_cache_advanced` factories and the
#   `dataset.prewarm_vector_cache(...)` API are gone in v6; see
#   `plans/benchmark/lance-distributed-cache-6.0.md`.
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
# "Expected results" below). Without it the per-repeat L2 directory is
# timestamped and every `distributed` repeat starts cold.
python run_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --dram-gb 4 --metadata-l1-mb 64 --partition-l1-mb 1024 \
    --nvme-dir /mnt/nvme/lance-l2 \
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
  hit-ratio signal has no v6 analog because Lance 6.0 does not expose
  hit/miss counters).

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
# `--dram-gb` is consumed by the moka scenario only; the distributed
# scenario's DRAM comes from `--metadata-l1-mb` + `--partition-l1-mb`.
python run_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --dram-gb 4 --metadata-l1-mb 64 --partition-l1-mb 1024 \
    --nvme-dir /mnt/nvme/lance-l2 \
    --k-list 10,100,1000 --nprobes 32 \
    --warmup-queries 1024 --measure-queries 5000 \
    --repeats 3 --reuse-l2
```

## Distributed mode (multi-actor)

`run_bench.py` exercises a **single** Ray actor end-to-end. To measure
how the v6 distributed cache behaves under Ray-style fan-out —
multiple actors with independent Sessions, parallel prewarm, and
queries split across workers — use `run_distributed_bench.py`.

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
  comparison.** Mechanics in
  [Coordinator-driven sharded mode](#coordinator-driven-sharded-mode).
  Requires a pylance build that exposes `dataset.compute_partition_ids`
  / `dataset.search_partitions`; the actor raises a clear
  `RuntimeError` on first use if either wrapper is missing.

**Implicit flag interactions** worth knowing before reading the example:

- `--mode sharded` forces `--prewarm sharded` (the coordinator can only
  route to workers that own a known partition slice). The driver logs the
  override and rewrites whatever `--prewarm` you passed.
- `--warmup-queries N` is consumed by `--prewarm natural` only. In
  `--mode sharded`, deterministic `--prewarm sharded` populates every
  cache namespace the measurement path touches (one
  `part-ivf-<id>.bin` file per owned partition written to L2 via the
  v6 strict `dataset.prewarm_index(name, partition_ids=...)` path,
  plus the top-level vector index objects opened on each worker). The
  driver then runs a one-shot `compute_partition_ids` call on the
  coordinator to force-open its own top-level vector index outside
  the measure timer (the coord opens the index lazily on first
  centroid routing, so without this the first measured query pays the
  index-open cost). The previous coordinator-driven random-query
  warmup is no longer run; if `--warmup-queries N > 0` is set it is
  logged and ignored.
- In `--mode sharded`, the per-actor table reports
  `bytes=<Session.size_bytes()>` post-measure plus `owned=<count>` and
  `calls_handled=<count>` — no per-query latency on the worker side
  (the coordinator owns the timer) and no hit-ratio column (Lance 6.0
  does not expose hit/miss counters).

Run distributed and moka as two sequential invocations against the same
MinIO dataset+index. Run 1 omits `--skip-setup` so `ensure_dataset(...)`
builds the dataset + IVF_RQ index if absent (idempotent — reuses if a
prior `run_bench.py` already populated the bucket); Run 2 keeps
`--skip-setup` and a distinct `--out-dir` so the per-scenario
`distributed_results.jsonl` files don't overwrite each other.

```bash
# Per-actor budgets — for the distributed scenario, per-actor DRAM is
# `--metadata-l1-mb` + `--partition-l1-mb` (total DRAM use scales with
# num_actors) and per-actor L2 is operator-sized via the `--nvme-dir`
# filesystem (v6 has no L2 capacity bookkeeping, so `--l2-gb` is
# ignored). The moka scenario uses `--dram-gb` per actor and no L2.
# Under --mode replicated every actor caches the full slice and answers
# queries independently; the driver round-robins queries across actors.

# Run 1 — distributed replicated (creates dataset + index in MinIO if
# absent). Forced prewarm has every actor call
# `dataset.prewarm_index(name)` in parallel — the v6 *best-effort*
# all-partitions path: it walks every partition and writes
# `part-ivf-<id>.bin` files under each actor's
# `<nvme-dir>/actor-<i>/v1/...`, but unlike the strict
# `prewarm_index(name, partition_ids=[...])` form it can swallow L2
# write errors and tombstoned-prefix skips rather than raising. The
# driver therefore does not pre-check L2 file counts before
# measurement; rely on the post-prewarm L2 snapshot and residency
# probe to confirm placement. Decoded partitions are admitted into
# the in-process partition-L1 tier up to its cap and measure-phase
# queries hit local L2 (and partition-L1 when warm).
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario distributed \
    --num-actors 4 --dram-gb 1 \
    --metadata-l1-mb 64 --partition-l1-mb 1024 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --mode replicated --prewarm forced \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 100 \
    --out-dir out/distributed-replicated \
    2>&1 | tee bench-distributed-replicated.log

# Run 2 — moka baseline. Reuses the dataset+index from Run 1.
# --nvme-dir / --partition-l1-mb / --metadata-l1-mb omitted because the
# moka session is pure DRAM under the plain Session(...) constructor.
# Natural warmup splits the warmup queries across actors and lets each
# Moka cache converge under its `--dram-gb` cap (v6 has no
# moka_ram_cap deterministic prewarm policy).
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
| `--mode sharded` | Spin up a `CoordinatorActor` that owns the IVF centroid step and a `partition_id % num_actors` mapping. Per query, the coord routes the `--nprobes` ids to their owning workers, gathers per-worker partial top-K via `search_partitions`, and merges to a global top-K — full recall, but per-query wall-time is bounded below by the slowest fan-out leg. Forces `--prewarm=sharded` (workers must own a known slice before routing). See [Coordinator-driven sharded mode](#coordinator-driven-sharded-mode). Requires a pylance build that exposes `dataset.compute_partition_ids` / `dataset.search_partitions`. |
| `--num-actors N` | Spawn N parallel `HybridSearchActor`s. Each gets `<nvme-dir>/actor-<i>` as its L2 subdir so the v6 backend's exclusive `lance-distributed.lock` is uncontended. |
| `--prewarm forced` | Each actor calls `dataset.prewarm_index(<index-name>)` in parallel — the v6 *best-effort* all-partitions form (no `partition_ids` arg). It walks every partition and writes `part-ivf-<id>.bin` files but can swallow L2 write errors / tombstoned-prefix skips, so the driver does not pre-check L2 file counts before measurement. For strict fail-fast behavior, use `--prewarm sharded` (which calls `dataset.prewarm_index(name, partition_ids=[...])`). |
| `--prewarm natural` | Splits `--warmup-queries` across actors (default; each cache state diverges). |
| `--prewarm sharded` | Actor `i` deterministically prewarms partitions `{i, i+N, i+2N, …}` via the v6 strict `dataset.prewarm_index(name, partition_ids=...)` path; the actor walks its L2 dir post-prewarm and reports `l2_validation` (`l2_file_count` / `missing_count` / `extra_count`) so the driver can hard-fail on placement drift. The v4 `policy` / `ram_bytes` knobs (`hybrid_tiered`, `moka_ram_cap`) are gone in v6: for `distributed` the cache controls placement itself, writing one `part-ivf-<id>.bin` per partition into L2 atomically; for `no-cache` the call is a no-op that registers ownership only. Under `--mode replicated` the measure phase uses `compute_partition_ids` + `search_partitions` so each actor only searches its owned slice (per-query recall is partial — see the sharded caveat below). Under `--mode sharded` the same prewarm feeds the coordinator topology. Per-actor prewarm cost stays flat as `--num-actors` grows. Requires a pylance build that exposes `compute_partition_ids` / `search_partitions`; the actor raises a clear `RuntimeError` on first use if either is missing. |
| `--prewarm none` | Skip prewarm; first measure query is cold. |
| `--warmup-queries N` | Used by `--prewarm natural` (split across actors). Under `--mode sharded` it is ignored: deterministic sharded prewarm already populates every cache namespace the measure path touches. |
| `--dram-gb` | **Per-actor** DRAM budget for the `moka` scenario. Ignored for `--scenario distributed` (whose DRAM is sized by `--metadata-l1-mb` + `--partition-l1-mb`). |
| `--metadata-l1-mb` | **Per-actor** v6 metadata-L1 budget (MiB) for the distributed scenario. Caches `IvfIndexState`, `IndexMetadata`, etc.; default 64. See [v6 DRAM split](#v6-dram-split---metadata-l1-mb----partition-l1-mb). |
| `--partition-l1-mb` | **Per-actor** v6 decoded-partition-L1 budget (MiB) for the distributed scenario. Pass `0` to disable; default 1024. |
| `--l2-gb` | Deprecated v4 hybrid knob. v6 has no L2 capacity bookkeeping — size the actor's NVMe filesystem yourself. Accepted but ignored. |
| `--codecless-mb N` | Deprecated v4 hybrid knob. The v6 distributed cache has no codec-less Moka tier; passing this flag prints a warning and is otherwise ignored. |
| `--prewarm-ram-fraction F` | Legacy no-op. The v6 distributed cache's strict prewarm path places every owned partition in L2 directly (and admits to partition-L1 up to its cap from within the backend); there is no operator-visible L1 budget to scale, so values other than `1.0` are flagged as ignored. |
| `--pre-measure-residency-probe` | Also run the v6 aggregate-only residency probe between prewarm and measure. Off by default for symmetry with the v4 narrative — the probe itself is side-effect-free under v6 (one filesystem walk under `{l2_dir}/v1/{prefix}/` plus `Session.size_bytes()` per actor, returned in a single RPC), but a single flag controls both the pre-measure and the post-measure probes so they are written symmetrically to `partition_residency.jsonl`. The v4 no-load per-partition L1 probe has no v6 equivalent — the L2 directory walk plus aggregate `Session.size_bytes()` is the replacement. The post-measure probe always runs for forced/sharded prewarm with `--scenario` other than `no-cache` because it cannot pollute the measurement. |
| `--simulate-invalidation` | Opt-in v6 freshness drill that fires after the first measure phase. Calls `Session.invalidate_index_cache(uri, index_addr)` on every actor (and on the `CoordinatorActor` under `--mode sharded`) with one retry on `IOError`; verifies the per-prefix L2 subdir is gone or in a `.{sanitize(prefix)}.deleting-{nonce}/` sentinel state via `snapshot_l2_dir`; reruns sharded prewarm to time the cold L2 rehydration; reruns the measure phase. Writes `<out-dir>/invalidation.json` with first/second per-k latency summaries, per-actor invalidate times, the rehydrate-prewarm wall-time, and per-k percentage deltas. Requires `--scenario distributed` and `--prewarm sharded` (only the v6 strict sharded prewarm rehydrates the L2 prefix deterministically). |
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

Pass `--simulate-invalidation` (only valid with `--scenario distributed`
and `--prewarm sharded`) to also exercise the v6 freshness contract:
after the first measure phase the driver invalidates every actor's
cache via `Session.invalidate_index_cache(uri, index_addr)` (one retry
on `IOError`, hard-fail beyond that), verifies the per-prefix L2 subdir
went away or is in a `.{sanitize(prefix)}.deleting-{nonce}/` sentinel
state, reruns sharded prewarm to time cold L2 rehydration, and reruns
measure. A second JSON file `<out-dir>/invalidation.json` carries the
first/second per-k latency summaries, per-actor invalidate times, the
rehydrate-prewarm wall-time, and per-k percentage deltas (also exposed
as plan-style top-level `measure1_p50_s` / `measure1_p95_s` /
`measure2_p50_s` / `measure2_p95_s` / `delta_*_pct` fields, computed
against the first k in `--k-list`). Recommended on any first run
against a new pylance build because it catches regressions in the
v6 generation / tombstone protocol that unit tests alone do not.

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
is directly comparable across scenarios. The
[Distributed mode (multi-actor)](#distributed-mode-multi-actor) example
above pins `--mode replicated` for cross-build safety; swap in
`--mode sharded` (the driver auto-forces `--prewarm sharded`) to
exercise this topology — typically with
`--coordinator-resource coord_node` in a real cluster.

Mode-specific output:

- The summary prints a `coord per-query mean: centroid=… ms scatter=…
  ms merge=… ms workers_invoked≈X/N routed_partitions≈Y/nprobes` line
  so you can see where wall-time goes.
- The per-actor table shows `bytes=<Session.size_bytes()>  owned=<count>
  calls_handled=<count>` — the worker doesn't time per-query work in
  this mode, but the post-measure session footprint and per-actor RPC
  count make fan-out balance visible.
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
- `distributed.p99 < moka.p99` — **steady-state**. With deterministic
  sharded prewarm every owned vector partition is placed in L2 before
  measurement, so the first measured query already reads from local
  NVMe instead of MinIO and the distributed steady-state win shows up
  immediately. The `run_bench.py` single-actor path still relies on
  `--reuse-l2` across repeats because its prewarm comes from random
  warmup queries rather than deterministic L2 placement — the first
  repeat there reads as cold.
- `session_size_bytes`: `Session.size_bytes()` (the v6 substitute for v4
  hit ratios) reflects the partition-L1 tier's occupancy. Strict
  prewarm admits decoded partition entries into L1 up to
  `--partition-l1-mb`, so for `distributed` runs the `pre` value
  entering the measure phase is often already at or near the cap; the
  measure-phase delta may be ~0 (no headroom), positive (filling
  remaining headroom), or negative (eviction churn). Treat the signed
  delta as informative for tuning `--partition-l1-mb`, not as a
  cache-effectiveness proxy. Use measure-phase latency vs. the
  `no-cache` baseline for that signal — Lance 6.0 does not expose
  hit/miss counters.

## Teardown

```bash
sudo bash infra/netem_down.sh
docker compose -f infra/docker-compose.yml down -v
```

## Pitfalls

- **Dropping the session releases the L2 flock** — the v6 backend exposes
  no `Session.close()`; it releases `{l2_dir}/lance-distributed.lock` when
  the Session object is *dropped*. End every run by letting the session
  (and the dataset that pins it) go out of scope so the next run on the
  same `l2_dir` can attach. `ScenarioActor.run` does this by returning —
  its `sess`/`ds` locals are freed on return — and `HybridSearchActor.close`
  does it by clearing its `self._sess`/`self._ds` references (it still
  calls `Session.close()` first on the v4 fork that retains it). Vector-
  partition durability does *not* depend on this: the v6 strict prewarm
  path (`dataset.prewarm_index(name, partition_ids=...)`) writes each
  `part-ivf-{id}.bin` atomically (`tmp-{nonce}` → `fsync` → `rename`
  → `fsync(parent)`), so a crash mid-prewarm leaves no half-written
  files. Don't stash the session in a longer-lived reference, or the
  flock stays held until the actor process exits.
- **`l2_dir` is exclusive per process** — the v6 backend takes an
  exclusive advisory lock on `{l2_dir}/lance-distributed.lock` for the
  lifetime of the session, and `Session.with_distributed_cache(...)`
  returns `PyValueError` if the directory is missing, a symlink, or
  already locked. `distributed_l2_dir_for_repeat(..., reuse_l2=False)`
  timestamps each repeat's path so this doesn't collide; `--reuse-l2`
  reuses a single dir and requires only one distributed actor alive
  at a time.
- **L2 sizing is operator-driven** — v6 does no L2 capacity bookkeeping
  (`l2_writes_total` and friends are exposed only on the Rust side).
  Size the actor's NVMe filesystem against `owned_count *
  partition_size`; the bench's post-prewarm L2 snapshot
  (`apparent_bytes` + `disk_bytes` + `file_count`) is the only
  Python-visible footprint readout.
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
[`../../plans/benchmark/lance-distributed-cache-6.0.md`](../../plans/benchmark/lance-distributed-cache-6.0.md).
The v4 plan
[`../../plans/benchmark/lance-hybrid-cache-ivf-rq.md`](../../plans/benchmark/lance-hybrid-cache-ivf-rq.md)
is superseded by the v6 plan and is preserved as a historical
reference only. The original Rust-subprocess architecture from the
pre-v4 design is also superseded. The implemented path uses the
existing Lance Python bindings for session creation, deterministic
prewarm, partition search, and residency checks, so there is no Rust
subprocess toolchain in the Ray benchmark driver.
