# Real 3-node Ray cluster with separate MinIO

> **Status — Lance 6.0 distributed-cache port.** The driver's scenario /
> session construction targets `Session.with_distributed_cache`;
> `--scenario hybrid` is a deprecated alias for `--scenario distributed`,
> and `--codecless-mb`, `--prewarm-ram-fraction`, and `--l2-gb` are
> accepted but ignored. The per-partition residency probe is disabled
> for `--scenario distributed`. **`--mode sharded` AND `--prewarm
> sharded` are hard-failed at startup** because the sharded measure
> path (`measure_sharded`) needs PyO3 wrappers for
> `compute_partition_ids` / `search_partitions`, which are Rust-only at
> the pinned v6 commit (see
> [`plans/benchmark/lance-v6-api-verification.md`](../../plans/benchmark/lance-v6-api-verification.md)).
> The example below has been switched from `--mode sharded` /
> `--prewarm sharded` to `--mode replicated --prewarm forced` (every
> actor calls `dataset.prewarm_index(name)` in parallel — the v6
> strict prewarm path) so it runs end-to-end against a v6 Lance build;
> the coordinator/full-recall topology returns once the PyO3 wrappers
> land. The wider sharded prewarm narrative below still uses v4 hybrid
> terminology — see
> [`plans/benchmark/lance-distributed-cache-6.0.md`](../../plans/benchmark/lance-distributed-cache-6.0.md)
> for the full v6 migration plan.

This guide runs the distributed Lance hybrid-cache benchmark on one
coordinator/head node, two actor nodes, and a separate MinIO node. Under
the v6 port `--mode replicated --num-actors 2` is the supported topology:
each actor caches the full partition slice and answers queries
independently; the coordinator-routed full-recall path (`--mode sharded`)
is blocked until Lance exposes the routing primitives in Python.

Install the same Python environment, Ray version, and Lance build on all three
Ray nodes. The benchmark driver ships `benchmarks/lance_hybrid_cache/` through
Ray `runtime_env`, and the shipping helper mirrors that directory onto each
Ray node so you can also run the driver directly there. The MinIO node only
needs Docker and the benchmark `infra/` scripts.

## Node placement

Use private, routable LAN IPs for Ray and MinIO. The values below are examples;
replace them with your cluster's real addresses.

| Role | Example IP | SSH target | Ray resource |
|---|---:|---|---|
| coordinator/head | `10.42.0.10` | `ubuntu@10.42.0.10` | `{"coord_node": 1}` |
| actor node 0 | `10.42.0.11` | `ubuntu@10.42.0.11` | `{"search_actor_node": 1}` |
| actor node 1 | `10.42.0.12` | `ubuntu@10.42.0.12` | `{"search_actor_node": 1}` |
| MinIO node | `10.42.0.20` | `ubuntu@10.42.0.20` | none |

Set these on the dev machine before running the commands in this guide:

```bash
export SSH_USER=ubuntu
export COORD_IP=10.42.0.10
export ACTOR0_IP=10.42.0.11
export ACTOR1_IP=10.42.0.12
export MINIO_HOST=10.42.0.20
export CLUSTER_CIDR=10.42.0.0/24
```

If SSH uses DNS names or bastion-specific host aliases, set
`COORD_SSH_HOST`, `ACTOR0_SSH_HOST`, `ACTOR1_SSH_HOST`, and
`MINIO_SSH_HOST` for shipping while keeping the `*_IP` / `MINIO_HOST` values
as the routable benchmark IPs.

## Ubuntu precheck

Run these checks before building, installing packages, or shipping artifacts.
The Ray and Lance wheels are compiled on the dev/build machine and executed on
the Ray nodes, so compare those hosts before spending time on a build.

From the dev/build machine, define a small facts command:

```bash
host_facts() {
  . /etc/os-release
  printf 'os=%s %s\n' "$ID" "$VERSION_ID"
  printf 'arch=%s\n' "$(uname -m)"
  printf 'glibc=%s\n' "$(getconf GNU_LIBC_VERSION)"
  if command -v python3.12 >/dev/null; then
    python3.12 -V | awk '{split($2, v, "."); print "python=" v[1] "." v[2]}'
  else
    echo "python=missing"
  fi
  printf 'isa='
  grep -m1 '^flags' /proc/cpuinfo | grep -oE 'avx2|avx512[^ ]*' | sort -u | tr '\n' ' '
  printf '\n'
}
```

Print the dev/build machine facts, then the same facts from each Ray node:

```bash
echo "== dev =="
host_facts

for host in "$COORD_IP" "$ACTOR0_IP" "$ACTOR1_IP"; do
  echo "== $host =="
  ssh -o BatchMode=yes "$SSH_USER@$host" "$(declare -f host_facts); host_facts"
done
```

Interpret the output:

| Field | Requirement |
|---|---|
| `python` | Must match exactly, for example `3.12` everywhere, because wheel tags are Python-minor specific. |
| `arch` | Must match, for example `x86_64` everywhere. |
| `os` / `glibc` | Safest is the same Ubuntu release and glibc on dev and Ray nodes. A wheel built on newer glibc can fail on older nodes. |
| `isa` | Dev/build machine must not require a higher CPU baseline than the Ray nodes. Example: dev with only `avx2` and all Ray nodes with `avx2 avx512*` is fine when building on dev; do not build AVX-512-native artifacts and run them on AVX2-only hosts. |

Hostnames, private IPs, CPU count, RAM size, and disk size can differ. The
coordinator does not need the actor NVMe mount.

Before installing anything, also check the actor-local NVMe mount on actor
node 0 and actor node 1 only:

```bash
df -h /mnt/nvme
```

On the Ray nodes, install only the OS packages required to run Ray and the
benchmark Python environment:

```bash
sudo apt-get update
sudo apt-get install -y \
    ca-certificates curl iproute2 lsb-release netcat-openbsd \
    openssh-client openssh-server rsync \
    python3.12 python3.12-venv

sudo systemctl enable --now ssh
```

Only the actor nodes need the benchmark NVMe L2 path. Do not create or use this
path on the coordinator:

```bash
sudo mkdir -p /mnt/nvme/lance-l2/distributed && \
  sudo chown -R "$USER" /mnt/nvme/lance-l2 && \
  test -w /mnt/nvme/lance-l2/distributed && echo writable
```

`test -w` prints nothing on success. Therefore the expected output
on each actor node is `writable`, or some errors on fail.

Only the MinIO node needs Docker:

```bash
sudo apt-get update
sudo apt-get install -y \
    ca-certificates curl openssh-client openssh-server rsync \
    docker.io docker-compose-v2

sudo systemctl enable --now ssh
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
```

After adding the user to the `docker` group on the MinIO node, log out and
back in before using `docker compose` without `sudo`.

If `ufw` is active on the Ray nodes, open the fixed ports used in the
`ray start` commands below from the private benchmark subnet:

```bash
export CLUSTER_CIDR=10.42.0.0/24

sudo ufw allow from "$CLUSTER_CIDR" to any port 22 proto tcp
sudo ufw allow from "$CLUSTER_CIDR" to any port 6379 proto tcp
sudo ufw allow from "$CLUSTER_CIDR" to any port 8265 proto tcp
sudo ufw allow from "$CLUSTER_CIDR" to any port 8076:8079 proto tcp
sudo ufw allow from "$CLUSTER_CIDR" to any port 52365 proto tcp
sudo ufw allow from "$CLUSTER_CIDR" to any port 10002:10100 proto tcp
sudo ufw status numbered
```

On the MinIO node, only SSH and MinIO need to be reachable from the benchmark
subnet:

```bash
export CLUSTER_CIDR=10.42.0.0/24

sudo ufw allow from "$CLUSTER_CIDR" to any port 22 proto tcp
sudo ufw allow from "$CLUSTER_CIDR" to any port 9000 proto tcp
sudo ufw allow from "$CLUSTER_CIDR" to any port 9001 proto tcp
sudo ufw status numbered
```

From the dev machine, verify SSH and the node IP placement:

```bash
for host in "$COORD_IP" "$ACTOR0_IP" "$ACTOR1_IP"; do
  ssh -o BatchMode=yes "$SSH_USER@$host" \
    'hostname; hostname -I; python3.12 --version'
done
```

Then verify the actor-node local disks:

```bash
for host in "$ACTOR0_IP" "$ACTOR1_IP"; do
  ssh -o BatchMode=yes "$SSH_USER@$host" \
    'test -w /mnt/nvme/lance-l2/distributed && echo writable'
done
```

The expected output is `writable` from both actor nodes.

## Deploy without compiling on Ray nodes

Treat the dev machine as a build host that is separate from the cluster. Build
all artifacts there, then ship them to all three Ray nodes. No Ray node needs a
git checkout, Bazel, Rust, maturin, or a C++ toolchain. The MinIO node receives
only the benchmark `infra/` directory.

Prerequisites that must match between this dev machine and every Ray node,
otherwise the prebuilt binaries may not run:

- Same Python minor, for example 3.12.x. The wheel ABI tag must match.
- Same OS/glibc family. Easiest: the same Ubuntu image everywhere.
- Same CPU baseline. If nodes differ, build on the lowest common denominator.

If those cannot be guaranteed, package the install in a container instead and
load that image on each node. The rest of this guide assumes matched hosts.

### 1. On dev: build the wheels and wheelhouse

The earlier `pip install -e ...` steps already gave you a venv with both
projects buildable. Reuse it to produce wheels:

```bash
source "$HOME/git/ray-open-source/python/venv/bin/activate"

# Ray wheel. `pip wheel` runs setup.py, which drives the Bazel build.
cd "$HOME/git/ray-open-source/python"
pip wheel . -w "$HOME/git/ray-open-source/dist" --no-deps
# Produces dist/ray-3.0.0.dev0-cp312-cp312-linux_x86_64.whl

# pylance wheel. The PyPI/distribution name is `pylance`; the Python import
# remains `lance`.
cd "$HOME/git/lance-open-source/python"
maturin build --release --out dist

# Wheelhouse: pre-resolve every dep so Ray nodes do zero compilation.
cd "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache"
pip wheel -r requirements.txt -w "$HOME/wheelhouse"
cp "$HOME/git/ray-open-source/dist/"ray-*.whl "$HOME/wheelhouse/"
cp "$HOME/git/lance-open-source/python/dist/"pylance-*.whl "$HOME/wheelhouse/"
```

### 2. On dev: ship wheelhouse and benchmark source

The `benchmarks/lance_hybrid_cache/` driver directory needs to land on every
Ray node alongside the wheelhouse. The coordinator invokes
`run_distributed_bench.py` from there. Workers also receive task code via Ray
`runtime_env`, but shipping the dir to all three Ray nodes keeps the layout
uniform and lets you re-run from any Ray node.

Use the checked-in shipping helper instead of an inline `rsync` loop:

```bash
cd "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache"

SSH_USER=ubuntu \
COORD_IP=10.42.0.10 \
ACTOR0_IP=10.42.0.11 \
ACTOR1_IP=10.42.0.12 \
MINIO_HOST=10.42.0.20 \
bash infra/ship_real_cluster.sh
```

Optional overrides:

```bash
RAY_CHECKOUT="$HOME/git/ray-open-source" \
WHEELHOUSE="$HOME/wheelhouse" \
REMOTE_HOME="/home/ubuntu" \
MINIO_SSH_HOST=10.42.0.20 \
bash infra/ship_real_cluster.sh
```

Mirroring this dev machine's layout means the `cd` in the driver invocation
block below works unchanged on every Ray node. The `git/ray-open-source/python`
directory is created empty so the next step can drop a venv at the path the
rest of this README references; nothing else from the Ray repo gets cloned to
the Ray nodes. On the MinIO node, the ship script creates only
`git/ray-open-source/benchmarks/lance_hybrid_cache/infra`.

### 3. On each Ray node: create venv and install from wheels

Run this once per Ray node. Each Ray node creates its own venv locally so
shebangs and `pyvenv.cfg` get correct absolute paths. Do not rsync a venv from
the dev machine:

```bash
python3.12 -m venv "$HOME/git/ray-open-source/python/venv"
source "$HOME/git/ray-open-source/python/venv/bin/activate"

pip install --no-index --find-links="$HOME/wheelhouse" \
    ray pylance \
    -r "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache/requirements.txt"
```

`--no-index` forbids PyPI fallback, so a missing wheel fails fast instead of
silently triggering a source build. `pylance` installs the package imported as
`lance`.

### Iterating after the initial deploy

Always rebuild on this dev machine, never on a Ray node:

- Benchmark driver/helper edits under `benchmarks/lance_hybrid_cache/`: re-run
  `infra/ship_real_cluster.sh`. No wheel rebuild.
- Lance Python or Rust edits: rebuild on dev with `maturin build --release`,
  copy the new `pylance` wheel into `$HOME/wheelhouse/`, run
  `infra/ship_real_cluster.sh`, then on each host run
  `pip install --force-reinstall --no-deps "$HOME/wheelhouse"/pylance-*.whl`.
- Ray package edits outside this benchmark directory: rebuild the Ray wheel and
  redeploy to all three Ray nodes.

## Start Ray with physical placement

Use Ray custom resources to make placement physical instead of best-effort. The
coordinator/head node gets only the coordinator resource. Each worker node gets
exactly one unit of the worker resource. The commands below also pin Ray to a
small, explicit port set so the Ubuntu firewall rules can be concrete.

Stop any existing Ray cluster before changing placement:

```bash
ray stop
```

On each node, export the same placement values before running its `ray start`
command:

```bash
export COORD_IP=10.42.0.10
export ACTOR0_IP=10.42.0.11
export ACTOR1_IP=10.42.0.12
export MINIO_HOST=10.42.0.20
```

On node 0, the coordinator/head:

```bash
ray start --head \
    --node-ip-address="$COORD_IP" \
    --port=6379 \
    --dashboard-host=0.0.0.0 \
    --dashboard-port=8265 \
    --node-manager-port=8076 \
    --object-manager-port=8077 \
    --runtime-env-agent-port=8078 \
    --dashboard-agent-grpc-port=8079 \
    --dashboard-agent-listen-port=52365 \
    --min-worker-port=10002 \
    --max-worker-port=10100 \
    --resources='{"coord_node": 1}'
```

On node 1, actor node 0:

```bash
ray start --address="$COORD_IP:6379" \
    --node-ip-address="$ACTOR0_IP" \
    --node-manager-port=8076 \
    --object-manager-port=8077 \
    --runtime-env-agent-port=8078 \
    --dashboard-agent-grpc-port=8079 \
    --dashboard-agent-listen-port=52365 \
    --min-worker-port=10002 \
    --max-worker-port=10100 \
    --resources='{"search_actor_node": 1}'
```

On node 2, actor node 1:

```bash
ray start --address="$COORD_IP:6379" \
    --node-ip-address="$ACTOR1_IP" \
    --node-manager-port=8076 \
    --object-manager-port=8077 \
    --runtime-env-agent-port=8078 \
    --dashboard-agent-grpc-port=8079 \
    --dashboard-agent-listen-port=52365 \
    --min-worker-port=10002 \
    --max-worker-port=10100 \
    --resources='{"search_actor_node": 1}'
```

After the head is up, verify from each actor node that the coordinator ports are
reachable:

```bash
nc -vz "$COORD_IP" 6379
nc -vz "$COORD_IP" 8265
```

## MinIO

Run MinIO on the separate MinIO node with a LAN bind, then pass
`--endpoint-url http://$MINIO_HOST:9000` to the benchmark:

```bash
cd "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache"
MINIO_BIND_ADDR=0.0.0.0 docker compose -f infra/docker-compose.yml up -d
bash infra/make_bucket.sh
```

Do not use `127.0.0.1` in the benchmark endpoint URL, because each worker would
resolve that to itself. `MINIO_HOST` is the host or IP where MinIO is exposed;
for this topology it is the separate MinIO node, not the Ray coordinator. The
MinIO console is on port 9001. If you do not want to expose the console beyond
the private subnet, access it through an SSH tunnel:

```bash
ssh -L 9001:127.0.0.1:9001 "$SSH_USER@$MINIO_HOST"
```

Then open `http://127.0.0.1:9001`.

The single-host `infra/netem_up.sh` only shapes loopback traffic and does not
add delay to remote worker-to-MinIO traffic. If MinIO is already running with
loopback-only ports, recreate the container with:

```bash
MINIO_BIND_ADDR=0.0.0.0 docker compose -f infra/docker-compose.yml up -d --force-recreate
```

The named volume is preserved unless you run `docker compose down -v`.

Verify MinIO from each Ray node after the container is up:

```bash
curl -fsS -o /dev/null -w "%{http_code}\n" "http://$MINIO_HOST:9000/minio/health/live"
# expect: 200
```

Each hybrid actor uses its local `<nvme-dir>/actor-<i>` L2 directory on the
actor node where it runs. If your actor disks are mounted somewhere else,
adjust `--nvme-dir`. The coordinator does not use the NVMe L2 path.

## Run the benchmark

Run the driver from the coordinator/head node:

```bash
cd "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache"
source "$HOME/git/ray-open-source/python/venv/bin/activate"
export RAY_ADDRESS="$COORD_IP:6379"
export MINIO_HOST=10.42.0.20

# Run 1: distributed scenario, replicated topology across two physical
# workers. Under v6 every actor caches the full slice and answers queries
# independently; --mode replicated --prewarm forced has both actors call
# `dataset.prewarm_index(name)` in parallel, which writes one
# part-ivf-<id>.bin per partition under each actor's
# <nvme-dir>/actor-<i>/v1/... atomically (rename + fsync; LanceError
# on any mid-prewarm failure). Measure-phase queries hit local L2 and
# the in-process partition-L1 tier. --warmup-queries 0 because forced
# prewarm already covers every partition the measure path touches.
# Full-recall sharded topology (each actor owning ~1/N partitions and a
# coordinator merging top-K across actors) is the v4 baseline and
# returns once the Lance PyO3 sharded-measure wrappers ship — see the
# status banner above.
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario distributed \
    --num-actors 2 --dram-gb 1 --l2-gb 8 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --mode replicated --prewarm forced \
    --actor-resource search_actor_node \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 1000 \
    --endpoint-url "http://$MINIO_HOST:9000" \
    --out-dir out/distributed-real-2actors \
    2>&1 | tee bench-distributed-real-2actors.log

# Run 2: moka baseline against the dataset/index created by Run 1.
# v6 port: the v4 `policy='moka_ram_cap'` deterministic prewarm and
# the per-partition residency probe are not bound in Lance 6.0
# (`--mode sharded` / `--prewarm sharded` / `--pre-measure-residency-probe`
# are blocked — see the status banner above). `--mode replicated
# --prewarm natural` splits the warmup queries across actors and lets
# each per-actor Moka cache converge under its `--dram-gb` cap.
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario moka \
    --num-actors 2 --dram-gb 1 \
    --mode replicated --prewarm natural \
    --actor-resource search_actor_node \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 256 --measure-queries 1000 \
    --endpoint-url "http://$MINIO_HOST:9000" \
    --skip-setup \
    --out-dir out/moka-real-2actors \
    2>&1 | tee bench-distributed-moka-real-2actors.log
```

The expected console shape is:

- `Distributed summary (2 actors, mode=sharded, ...)`
- One `[driver] sharded prewarm (deterministic, policy='hybrid_tiered') —
  placing every owned vector partition into L2; foyer L1 remains cold`
  block (no `ram_budget=...` field, hybrid_tiered ignores it) followed
  by per-actor lines reporting
  `ram=0 disk=<loaded_to_disk> skipped_l2=<n> ram_deep=0B
  disk_serialized=<bytes>`. The driver hard-fails if `ram` is not zero or
  if `disk_bytes_unknown_spills` is reported — those indicate a stale
  pylance build that still admits to L1 during hybrid prewarm.
- `[driver] L2 snapshot (post-prewarm):` followed by per-actor lines
  reporting `files=<n> apparent=<bytes> disk=<bytes>`.
- `[driver] coord routing warmup done in <seconds>s` (one-shot
  `compute_partition_ids` call that force-opens the coordinator's
  top-level vector index outside the measure timer).
- `coord per-query mean: centroid=... scatter=... merge=...`
- Two per-actor rows with balanced `owned=...` and `calls_handled=...`
- `=== Partition residency check: post-prewarm ===` before measurement,
  then `=== Partition residency check: post-measure ===` after measurement,
  with per-actor lines reporting
  `probed=<n> in_l1=<n> not_in_l1=<n> in_l2=<n> missing=<n>` plus a sample
  of partition ids in each tier — the verification that prewarm placed
  the expected partitions into L2 and that query traffic promoted some
  partitions into L1 without losing any from L2. The `l2_source=...`
  field at end of each line marks whether per-partition L2 residency came
  from a bound index probe or, today, from
  `prewarm_validated_owned_set+dir_snapshot(no_index_probe)` (the Rust
  `partition_is_in_l2` is not yet exposed to Python).
- `=== L2 directory snapshot (post-measure) ===` with per-actor lines
  reporting `files=<n> apparent=<bytes> disk=<bytes>` plus a
  `Δ(apparent=..., disk=..., files=...)` delta against the
  post-prewarm snapshot. Stable totals are the coarse fallback signal
  that vector-partition L1 eviction did not produce extra L2 writes.

### Partition residency verification

> **v6 port — disabled for `--scenario distributed`.** The
> per-partition residency probe calls v4
> `prewarm_vector_cache(name, [p], policy='moka_ram_cap', ram_bytes=0)`
> as a no-load probe; Lance 6.0 has no equivalent and the driver skips
> the probe for the distributed scenario. The expected console blocks
> and `<out-dir>/partition_residency.jsonl` described below apply to
> the v4 `--scenario hybrid` path only. See
> [`plans/benchmark/lance-distributed-cache-6.0.md`](../../plans/benchmark/lance-distributed-cache-6.0.md)
> ("Residency probe") for the aggregate-only v6 replacement.

Under v4 the post-measure probe always ran for hybrid actors: it fires
after the measured workload, so it cannot perturb the measurement, and
it is the only block written to `<out-dir>/partition_residency.jsonl`.
The probe called Lance's
`prewarm_vector_cache(name, [p], policy='moka_ram_cap', ram_bytes=0)`
once per owned partition: pass 1 charged DRAM-resident
partitions into `skipped_existing`, pass 2 short-circuited on
`ram_bytes_deep_size >= ram_bytes` (i.e. `0 >= 0`) before loading
anything — no storage read either way.

Adding `--pre-measure-residency-probe` enabled a second probe between
prewarm and measure. The flag was opt-in because the probe is no-*load*
but not no-*touch*: it walks the cache access path once per owned
partition immediately before the measured workload, which can shift
replacement-policy state (recency / frequency / admission). The
hit/miss counter subtraction in the measure phase canceled the count
bump but not that policy bump, so by default the cache between prewarm
and measure was left untouched. With the flag set, both probes were
written to `<out-dir>/partition_residency.jsonl`, one JSON line per
actor per probe, with a `"label"` field of `"post-prewarm"` or `"post-measure"`
so downstream tooling can demux.

Each line carries the full sorted lists of in-RAM and not-in-RAM
partition ids, the session-level cache footprint (under v6 the
`session_stats` field is `{"size_bytes": int}` from `Session.size_bytes()`;
under the v4 hybrid path it was the dict returned by the now-removed
`index_cache_stats()`), and (for the v4 hybrid path) the per-actor L2
directory footprint.

Per-partition L2 residency is not yet surfaced by pylance (the underlying
Rust helpers `partition_is_cached` / `partition_is_in_l2` exist but are
not bound). Until the binding lands, hybrid reports set
`l2_residency_source="prewarm_validated_owned_set"` and fill `in_l2` from
each actor's owned partition slice. This is a safe default because
deterministic `hybrid_tiered` prewarm with `wait_for_disk=True` places
every owned partition in L2 by construction and the
no-vector-L1-writeback policy means subsequent query traffic cannot
remove a partition from L2. Cross-reference `in_l2` against the
prewarm-time `loaded_to_disk + skipped_existing` count and the per-actor
L2 directory footprint: stable byte/file totals across the measurement
phase confirm no extra L2 writes occurred, and any growth there warrants
investigation. The driver's pre/post L2 snapshot delta block surfaces
this directly.

The same probe is exposed as a standalone script that can attach to a
live Ray cluster while the bench's actors are still alive — useful for
ad-hoc snapshots between phases or after manual experiments:

```bash
python -u check_partition_residency.py \
    --num-actors 2 --num-partitions 3000 \
    --index-name vector_idx \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --label adhoc \
    --out out/hybrid-real-2actors/partition_residency.jsonl
```

The script resolves the bench's named actors (`hybrid-search-actor-<i>`)
via `ray.get_actor(...)`, so it only works during a run where the
driver's actors have not yet been killed.

### Legacy: `--prewarm-ram-fraction` and the foyer `spilled` column

Earlier hybrid prewarm filled foyer L1 first and let foyer's
`WriteOnEviction` policy spill the remainder of each actor's slice to
L2. Hash skew across foyer's 16-shard L1 meant a subset of L1-admitted
partitions could end up on L2 instead of DRAM, surfaced as a
`spilled=<count>` column on the per-actor prewarm log and rolled into
`disk=<loaded_to_disk>`. `--prewarm-ram-fraction <f>` existed to scale
the nominal L1 target down (~0.5 for deterministic full-L1 residency)
in exchange for losing fill.

The current Lance vector cache no longer writes L1 admissions back to
L2 for IVF vector partitions. Deterministic `hybrid_tiered` prewarm
places every owned partition straight into L2 and leaves L1 entirely
cold, so there is no L1 admission to spill and no L1 budget for the
fraction knob to scale. `disk_bytes_unknown_spills` is always zero —
the driver hard-fails if it sees a non-zero value, since that indicates
a stale pylance build that still spills. `--prewarm-ram-fraction` is
kept as a backward-compatibility flag; values other than `1.0` print a
warning and are otherwise ignored.

In sharded mode, `<out-dir>/distributed_results.jsonl` writes the coordinator
aggregate latency row first (`actor_id="coordinator"`) and then one row per
worker with cache statistics.

## Shutdown

After the driver exits, stop Ray on the actor nodes first and the coordinator
last. This leaves MinIO running, so the generated dataset and IVF_RQ index stay
available for the next benchmark run:

```bash
export SSH_USER=ubuntu
export COORD_IP=10.42.0.10
export ACTOR0_IP=10.42.0.11
export ACTOR1_IP=10.42.0.12

for host in "$ACTOR0_IP" "$ACTOR1_IP" "$COORD_IP"; do
  ssh "$SSH_USER@$host" \
    '. "$HOME/git/ray-open-source/python/venv/bin/activate" && ray stop'
done
```

Verify that Ray processes stopped on all Ray nodes. The command should print
nothing:

```bash
for host in "$ACTOR0_IP" "$ACTOR1_IP" "$COORD_IP"; do
  ssh "$SSH_USER@$host" \
    "pgrep -af '[r]aylet|[g]cs_server|[d]ashboard|[r]untime_env|[p]lasma_store' || true"
done
```

Stop MinIO on the separate MinIO node when you are done with object-store
traffic. Use `docker compose down` without `-v` so the named volume, dataset,
and index are preserved:

```bash
export MINIO_HOST=10.42.0.20

ssh "$SSH_USER@$MINIO_HOST" \
  'cd "$HOME/git/ray-open-source/benchmarks/lance_hybrid_cache" && \
   docker compose -f infra/docker-compose.yml down'
```

If you opened an SSH tunnel for the MinIO console, stop that local SSH process
with `Ctrl-C`.

Only after copying any benchmark outputs you need, reclaim local L2 scratch
space on the actor nodes:

```bash
for host in "$ACTOR0_IP" "$ACTOR1_IP"; do
  ssh "$SSH_USER@$host" \
    'rm -rf /mnt/nvme/lance-l2/distributed/*'
done
```

To delete the MinIO dataset and index as well, run `docker compose down -v` on
the MinIO node instead of `docker compose down`.
