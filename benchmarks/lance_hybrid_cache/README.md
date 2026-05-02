# Lance hybrid-cache (NVMe + DRAM) IVF_RQ benchmark

Measures vector-search latency for Lance's two-tier hybrid cache (foyer-backed
L1 DRAM + L2 NVMe) against two baselines: *no cache* and *Moka DRAM-only*.

- **Dataset**: 10M × 1024-d f32 embeddings
- **Index**: IVF_RQ, 3000 partitions, `num_bits=8` (~10 GB total)
- **Scenarios**: `no-cache` | `moka` (4 GiB DRAM) | `hybrid` (4 GiB DRAM + 30 GiB NVMe L2).
  Both `moka` and `hybrid` get the same total DRAM budget — `hybrid`'s only extra
  resource is the NVMe L2 tier. Lance's default 90/10 foyer/Moka split already
  sends the bulk of hybrid DRAM through foyer L1 → L2. See [Same-DRAM hybrid split](#same-dram-hybrid-split)
  for how to override that split when the codec-less working set needs more
  headroom than the 10% reserve.
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
# - The native `_lance.so` exposes `lance.Session.with_hybrid_cache(...)` and
#   `lance.Session.close()` because the `lance` crate is built with the
#   `hybrid-cache` Cargo feature on (declared in `python/Cargo.toml`).
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
  `index_cache_stats`, per-k latency arrays, `duration_s`
- `summary.csv` — one row per scenario × repeat × k with p50/p95/p99/mean/n
  and hit ratio
- `plots/{latency_cdf,p99_bars,hit_ratio}.png`

## Same-DRAM hybrid split

`moka` always gets `--dram-gb` of DRAM and nothing else. `hybrid` gets the same
DRAM budget plus the L2 NVMe tier — `hybrid`'s win has to come from the NVMe
headroom, not from a bigger DRAM cache.

Inside hybrid the DRAM budget is split between two tiers:

- **foyer L1** — DRAM caching tier in front of L2 NVMe. Pages evicted from L1
  spill to L2 instead of MinIO.
- **codec-less embedded Moka** — separate DRAM cache for codec-less entries
  (entries that don't go through the foyer encode/decode path).

Two ways to size the split:

| Mode | Driver flags | foyer L1 | codec-less Moka | API used |
|---|---|---|---|---|
| **default 90/10** | `--dram-gb 4` | ~3.6 GiB | ~410 MiB | `Session.with_hybrid_cache` |
| **advanced** | `--dram-gb 4 --codecless-mb 64` | ~3.94 GiB | 64 MiB | `Session.with_hybrid_cache_advanced` |

The default puts 90% of DRAM in foyer L1 (so codec-bearing IVF_RQ / IVF_PQ /
IVF_SQ partition payloads spill to L2 on eviction) and reserves 10% for the
codec-less embedded Moka (top-level vector / scalar index objects, scalar
index pages, legacy IVF v1 entries, and any other keys without a
`CacheCodec`). Use `--codecless-mb` only when you need to override the
default — e.g. push the codec-less Moka smaller (foyer-dominant workloads
that want every byte of DRAM in front of L2) or larger (workloads whose
codec-less metadata working set exceeds the 10% reserve). Total hybrid DRAM
stays at `--dram-gb` either way, so the moka↔hybrid comparison stays
same-DRAM.

```bash
# Same-DRAM, codec-path-dominant comparison: 4 GiB DRAM both sides,
# hybrid gets +30 GiB NVMe L2 and is forced to use it (small Moka).
python run_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --dram-gb 4 --codecless-mb 64 --l2-gb 30 \
    --nvme-dir /mnt/nvme/lance-l2 \
    --k-list 10,100,1000 --nprobes 32 \
    --warmup-queries 1024 --measure-queries 5000 \
    --repeats 3 --reuse-l2
```

Requires lance-open-source ≥ commit `8c7c4d96c` (the `with_hybrid_cache_advanced`
classmethod). Older lance builds error on the `Session.with_hybrid_cache_advanced`
attribute lookup; rebuild with `pip install -e $HOME/git/lance-open-source/python`.

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
# Under --mode sharded each actor caches only ~1/num_actors of the index,
# so per-actor cache pressure is lower than --mode replicated at the same
# budgets. Scale --dram-gb / --l2-gb down if you want each actor's slice
# to overflow DRAM and exercise the L2 / MinIO tier.

# Run 1 — hybrid sharded (creates dataset + index in MinIO if absent).
# --mode sharded auto-forces --prewarm sharded; sharded prewarm is
# deterministic — actor i loads its slice (round-robin mod num_actors)
# via dataset.prewarm_vector_cache(policy='hybrid_tiered', ram_bytes=
# foyer_L1_budget), filling foyer L1 first and pushing the rest of its
# slice to L2. No coord-driven random-query warmup pass is needed.
# --codecless-mb 64 trims the codec-less Moka reserve below the default
# 10% so essentially all of --dram-gb sits in foyer L1; useful when the
# codec-less working set is small and you want to maximise foyer L1
# headroom for the codec-bearing IVF partitions. Drop --warmup-queries
# to 0; it is ignored under --mode sharded with deterministic prewarm.
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario hybrid \
    --num-actors 4 --dram-gb 1 --l2-gb 8 --codecless-mb 64 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --mode sharded \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 100 \
    --out-dir out/hybrid-sharded \
    2>&1 | tee bench-distributed-hybrid-sharded.log

# Run 2 — moka sharded reuses the dataset+index from Run 1. --l2-gb /
# --nvme-dir omitted because moka is pure DRAM and ignores the L2 tier.
# Deterministic prewarm uses policy='moka_ram_cap' with ram_bytes=
# --dram-gb per actor; load stops once the per-actor DRAM budget is
# full so MinIO traffic isn't wasted churning the cache.
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario moka \
    --num-actors 4 --dram-gb 1 \
    --mode sharded \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 100 \
    --skip-setup \
    --out-dir out/moka-sharded \
    2>&1 | tee bench-distributed-moka-sharded.log
```

Key knobs unique to the distributed driver:

| Flag | Purpose |
|---|---|
| `--mode replicated` (default) | Every actor sees every partition; the driver fans out queries round-robin, each actor probes independently. Use with `--prewarm {natural,forced,sharded,none}`. Prewarm cost grows linearly with `--num-actors` (each actor pulls every partition from MinIO). |
| `--mode sharded` | Spin up a `CoordinatorActor` that owns the IVF centroid step and a `partition_id %% num_actors` mapping. Per query, the coord routes the `--nprobes` ids to their owning workers, gathers per-worker partial top-K via `search_partitions`, and merges to a global top-K — full recall, but per-query wall-time is bounded below by the slowest fan-out leg. Forces `--prewarm=sharded` (workers must own a known slice before routing). See [Coordinator-driven sharded mode](#coordinator-driven-sharded-mode). |
| `--num-actors N` | Spawn N parallel `HybridSearchActor`s. Each gets `<nvme-dir>/actor-<i>` as its L2 subdir so foyer's exclusive flock is uncontended. |
| `--prewarm forced` | Each actor calls `dataset.prewarm_index(<index-name>)` in parallel — exercises Lance's forced-prewarm path. |
| `--prewarm natural` | Splits `--warmup-queries` across actors (default; each cache state diverges). |
| `--prewarm sharded` | Actor `i` deterministically prewarms partitions `{i, i+N, i+2N, …}` via `dataset.prewarm_vector_cache(...)`. The driver picks the placement policy from `--scenario`: `hybrid_tiered` for hybrid (fill foyer L1 up to its per-actor budget, force the rest of the slice to L2), `moka_ram_cap` for moka (load until `--dram-gb` is full, then stop), no-op for `no-cache`. Under `--mode replicated` the measure phase uses `compute_partition_ids` + `search_partitions` so each actor only searches its owned slice (per-query recall is partial — see the sharded caveat below). Under `--mode sharded` the same prewarm feeds the coordinator topology. Per-actor prewarm cost stays flat as `--num-actors` grows. Requires lance ≥ commit `14f9e2862`. |
| `--prewarm none` | Skip prewarm; first measure query is cold. |
| `--warmup-queries N` | Used by `--prewarm natural` (split across actors). Under `--mode sharded` it is ignored: deterministic sharded prewarm already populates every cache namespace the measure path touches. |
| `--dram-gb` / `--l2-gb` | **Per-actor**, not aggregate. Scale down when increasing `--num-actors`. Both honoured only under `--scenario hybrid`; `moka`/`no-cache` ignore the L2 tier. |
| `--codecless-mb N` | Per-actor codec-less Moka size; switches to `with_hybrid_cache_advanced`. Foyer L1 = `--dram-gb − --codecless-mb`. See [Same-DRAM hybrid split](#same-dram-hybrid-split). |
| `--prewarm-ram-fraction F` | Scales the hybrid foyer L1 budget used by `policy='hybrid_tiered'`. Set below `1.0` when foyer shard skew spills too many nominal L1 admissions to L2; ignored for `moka` and `no-cache`. |
| `--pre-measure-residency-probe` | Also run the partition residency probe immediately after deterministic prewarm and before measurement. It is off by default because it walks the cache access path once per owned partition and can affect replacement policy state before the measured workload. |
| `--actor-resource NAME` | Optional Ray custom resource required by each `HybridSearchActor`; use this in a real cluster to pin workers to actor nodes. Each actor reserves 1.0 of the resource. |
| `--coordinator-resource NAME` | Optional Ray custom resource required by the `CoordinatorActor` in `--mode sharded`; use this in a real cluster to pin the coordinator to the head/coordinator node. |

The summary reports both aggregate latency percentiles (across all actors)
and per-actor rows, plus per-actor hit ratios so you can tell whether the
fan-out is balanced. `<out-dir>/distributed_results.jsonl` has one record
per actor. For sharded `moka` and `hybrid` runs, the driver also writes
`<out-dir>/partition_residency.jsonl` after measurement; with
`--pre-measure-residency-probe`, the same file contains both `post-prewarm`
and `post-measure` labels so you can compare cache movement across the query
run.

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
- `hybrid.p99 < moka.p99` — **only on repeat ≥ 2 with `--reuse-l2`**. The
  first hybrid repeat warms L2 from cold, so it reads like a bigger DRAM
  cache. Repeats 2+3 show the steady-state win. Without `--reuse-l2` every
  repeat starts cold and `hybrid` looks like `moka`.
- `hit_ratio`: moka ≈ 30-50% (thrashing against the 4 GiB cap), hybrid
  approaches 100% once L2 is warm.

## Teardown

```bash
sudo bash infra/netem_down.sh
docker compose -f infra/docker-compose.yml down -v
```

## Pitfalls

- **Must reach `session.close()`** — foyer flushes L1→L2 only on close. The
  local `ScenarioActor.run` already calls it; don't wrap that in a bare
  try that swallows exceptions.
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
| `--dram-gb 4` | Total DRAM budget; applied identically to moka and hybrid. Must be < total index size or moka doesn't thrash. |
| `--codecless-mb 64` | Carves a fixed codec-less Moka slice off `--dram-gb` for hybrid; the rest goes to foyer L1 (in front of L2). Use to override Lance's default 90/10 foyer/Moka split — set smaller to maximise foyer L1, set larger when the codec-less metadata working set exceeds the 10% default reserve. |
| `--l2-gb 30` | Must be ≥ 1 GiB. Size it at ≥ index size to see hybrid fully warm. |
| `--measure-queries 5000` | ≥ 1000 recommended for stable p99 at these latencies. 100 is enough to smoke-test the topology — p50/mean stay informative, but p99 collapses to a single sample. |
| `--nprobes 32` | Keep fixed across scenarios; recall is constant, only latency varies. |
| `--reuse-l2` | Keep L2 warm across repeats. Useful once steady-state is proven. |

## Relationship to the benchmark plan

The current plan lives at
[`../../plans/benchmark/lance-hybrid-cache-ivf-rq.md`](../../plans/benchmark/lance-hybrid-cache-ivf-rq.md).
The original Rust-subprocess architecture is superseded. The implemented path
uses the existing Lance Python bindings for session creation, deterministic
prewarm, partition search, and residency checks, so there is no Rust
subprocess toolchain in the Ray benchmark driver.
