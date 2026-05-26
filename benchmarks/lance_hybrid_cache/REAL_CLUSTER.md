# Real 3-node Ray cluster with separate MinIO

This guide runs the Lance 6.0 distributed-cache benchmark on one
coordinator/head node, two actor nodes, and a separate MinIO node. The
example pins `--mode replicated --num-actors 2 --scenario distributed`
as the cross-build-safe topology: each actor caches the full partition
slice via the v6 best-effort `dataset.prewarm_index(name)` path (no
`partition_ids` arg; can swallow L2 write errors / tombstoned-prefix
skips — confirm placement via the post-prewarm L2 snapshot and
residency probe) and answers queries independently. The coordinator-routed full-recall path
(`--mode sharded` / `--prewarm sharded`) runs against any pylance
build that exposes `dataset.compute_partition_ids` /
`dataset.search_partitions`; in builds that do not, the actor
(`HybridSearchActor.measure_sharded`,
`HybridSearchActor.search_partitions`, `CoordinatorActor.__init__`)
raises a clear `RuntimeError` on first use, so an end-to-end sharded
run requires verifying that build first (see
[`plans/benchmark/lance-v6-api-verification.md`](../../plans/benchmark/lance-v6-api-verification.md)).

The wider v6 design is documented in
[`plans/benchmark/lance-distributed-cache-6.0.md`](../../plans/benchmark/lance-distributed-cache-6.0.md);
the v4 plan
[`plans/benchmark/lance-hybrid-cache-ivf-rq.md`](../../plans/benchmark/lance-hybrid-cache-ivf-rq.md)
is superseded by the v6 plan and is preserved as a historical
reference only.

## Pylance build

Pin `../lance-open-source` to branch `private-cache-6.0-ver-1` at
commit `9ebfe4de0` or newer. Build pylance from that local checkout
rather than pulling from PyPI; the v6 distributed-cache surface
(`Session.with_distributed_cache`, `Session.invalidate_index_cache`,
`Session.size_bytes`, and the strict
`dataset.prewarm_index(name, partition_ids=...)` path) is not on
PyPI. For sharded mode you additionally need
`dataset.compute_partition_ids` / `dataset.search_partitions`;
without those wrappers `--mode sharded` raises on first use, but
`--mode replicated --prewarm forced` runs unchanged.

Backward-compatibility notes on the driver:
`--scenario hybrid` is a deprecated alias for `--scenario distributed`
and `--codecless-mb`, `--prewarm-ram-fraction`, and `--l2-gb` are
accepted but ignored under the v6 distributed cache (which does no L2
capacity bookkeeping and has no codec-less Moka tier).

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

Each `HybridSearchActor` uses its local `<nvme-dir>/actor-<i>` L2
directory on the actor node where it runs; the v6
`Session.with_distributed_cache(...)` constructor takes an exclusive
advisory lock on `{l2_dir}/lance-distributed.lock` for the lifetime
of the session, so the directory must be exclusive to one process.
If your actor disks are mounted somewhere else, adjust `--nvme-dir`.
The coordinator does not use the NVMe L2 path.

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
# `dataset.prewarm_index(name)` in parallel — the v6 best-effort
# all-partitions form. It walks every partition and writes
# part-ivf-<id>.bin files under each actor's
# <nvme-dir>/actor-<i>/v1/..., but unlike the strict
# `prewarm_index(name, partition_ids=[...])` form it can swallow L2
# write errors and tombstoned-prefix skips rather than raising, so
# the driver does not pre-check L2 file counts before measurement —
# verify placement via the post-prewarm L2 snapshot and residency
# probe. Measure-phase queries hit local L2 and the in-process
# partition-L1 tier. --warmup-queries 0 because forced prewarm
# already covers every partition the measure path touches. To
# exercise the v6 freshness contract on a first run against a new
# pylance build, switch to `--mode sharded --prewarm sharded` and
# add `--simulate-invalidation` — see the Freshness drill section
# below.
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario distributed \
    --num-actors 2 --dram-gb 1 \
    --metadata-l1-mb 64 --partition-l1-mb 1024 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --mode replicated --prewarm forced \
    --actor-resource search_actor_node \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 1000 \
    --endpoint-url "http://$MINIO_HOST:9000" \
    --out-dir out/distributed-real-2actors \
    2>&1 | tee bench-distributed-real-2actors.log

# Run 2: moka baseline against the dataset/index created by Run 1.
# v6 has no `policy='moka_ram_cap'` deterministic prewarm and no v4
# per-partition L1 residency probe — the L2 directory walk plus
# aggregate `Session.size_bytes()` is the v6 aggregate-only
# replacement. `--mode replicated --prewarm natural` splits the
# warmup queries across actors and lets each per-actor Moka cache
# converge under its `--dram-gb` cap.
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

To run the coordinator-routed full-recall topology instead, switch to
`--mode sharded` (the driver auto-forces `--prewarm sharded`) and add
`--coordinator-resource coord_node`; that path requires a pylance
build that exposes `dataset.compute_partition_ids` /
`dataset.search_partitions`.

The expected console shape for Run 1 is:

- `Distributed summary (2 actors, mode=replicated, ...)` — the mode
  field reflects `--mode`, so it reads `mode=sharded` under a sharded
  invocation.
- One `[driver] forced prewarm — <num_actors> actors call
  dataset.prewarm_index(<index-name>) in parallel` block, followed by
  per-actor lines reporting `actor=<id> prewarm=<s> bytes=<n>` from
  `Session.size_bytes()` after the parallel `prewarm_index` call.
  This is the v6 best-effort all-partitions form (no `partition_ids`
  arg): it can swallow L2 write errors and tombstoned-prefix skips
  rather than raising `LanceError`, and the driver does **not**
  validate the per-actor L2 file count against the full owned range;
  rely on the post-prewarm residency / L2 directory snapshot below
  to confirm placement. (The pre-measure L2 file-count hard-fail and
  the strict `LanceError`-on-any-failure contract only fire under
  `--prewarm sharded`, which calls
  `dataset.prewarm_index(name, partition_ids=[...])` — the strict v6
  form — and where each actor's expected slice is deterministic.)
- `[driver] L2 snapshot (post-prewarm):` followed by per-actor lines
  reporting `files=<n> apparent=<bytes> disk=<bytes>`.
- Under `--mode sharded` only:
  `[driver] coord routing warmup done in <seconds>s` (one-shot
  `compute_partition_ids` call that force-opens the coordinator's
  top-level vector index outside the measure timer), then
  `coord per-query mean: centroid=... scatter=... merge=...`.
- Two per-actor rows. Under `--mode replicated` they include
  per-query latency percentiles; under `--mode sharded` they report
  `bytes=<Session.size_bytes()>  owned=<count>  calls_handled=<count>`
  (no per-query latency on the worker side because the coordinator
  owns the timer). The v4 `hit_ratio=...` column is gone — Lance 6.0
  has no hit-ratio counters.
- `=== L2 residency check: post-prewarm ===` (if
  `--pre-measure-residency-probe` is enabled) and
  `=== L2 residency check: post-measure ===` after measurement, with
  per-actor lines reporting
  `owned=<n> in_l2=<n> missing=<n> l2_files=<n> l2_bytes=<bytes>
  l1_bytes=<bytes> probe=<seconds>` — file presence under
  `{l2_dir}/v1/{sanitize(prefix)}/part-ivf-{id}.bin` one-to-one maps
  to L2 residency under the v6 layout. The L1 half is a session-wide
  `Session.size_bytes()` readout; Lance 6.0 has no no-load L1 probe,
  so the v4 per-partition `in_l1` / `not_in_l1` lists and the
  `l2_source=...` inference field are gone.
- `=== L2 directory snapshot (post-measure) ===` with per-actor lines
  reporting `files=<n> apparent=<bytes> disk=<bytes>` plus a
  `Δ(apparent=..., disk=..., files=...)` delta against the
  post-prewarm snapshot. Stable totals confirm query traffic did not
  trigger extra L2 writes. A `tombstones_added=True` field appended
  to the delta is a hard error: it means an invalidation rename hit
  the failure path and the v6 backend wrote a tombstone so future
  opens of that prefix skip it.

### Partition residency verification

The residency probe is eligible whenever `--prewarm` is `forced` or
`sharded` and `--scenario` is not `no-cache` — the v6 aggregate-only
report needs a defined per-actor expected partition set (full range
for `forced`, round-robin slice for `sharded`) and an L2 tier (any
scenario except `no-cache`). It fires for `distributed` (the scenario
it was designed for) and for `moka` (where the L2 half is trivially
empty and the row is dominated by `l1_size_bytes_at_probe`).

The post-measure probe fires after the measured workload, so it cannot
perturb the measurement, and it is the only block written to
`<out-dir>/partition_residency.jsonl` by default. It walks each actor's
L2 directory under `{l2_dir}/v1/{sanitize(prefix)}/` and counts the
`part-ivf-{id}.bin` files — file presence one-to-one maps to L2
residency under the v6 layout, so the report needs no inference and
gives no false positives. The walk runs on the actor process (via
`HybridSearchActor.check_l2_residency.remote(...)`) so it sees the
actor node's local NVMe; a driver-side walk would see an empty path
in a multi-node topology. The walk is scoped to a single live prefix
under `v1/`: if exactly one non-`.deleting-` subdir exists it is the
target; if two or more coexist (e.g. a stale prefix from an earlier
bench run sharing the same L2 path) the row is reported with empty
`in_l2`, full `missing`, and the conflicting names listed in
`l2_prefix_dirs` so the operator notices instead of seeing a stale-
prefix-masked "healthy" report. The L1 half is a session-wide
`Session.size_bytes()` readout, returned in the same RPC (Lance 6.0
has no no-load L1 probe).

Adding `--pre-measure-residency-probe` enables a second probe between
prewarm and measure. The flag is opt-in mainly for symmetry with the
v4 narrative — the v6 probe is a filesystem walk plus one
`Session.size_bytes()` read per actor, both returned in a single RPC,
no cache access path involved. With the flag set, both probes are
written to `<out-dir>/partition_residency.jsonl`, one JSON line per
actor per probe, with a `"label"` field of `"post-prewarm"` or
`"post-measure"` so downstream tooling can demux.

Each row carries:

| Field | Meaning |
|---|---|
| `actor_id` | Driver-assigned actor index. |
| `label` | `post-prewarm` or `post-measure`. |
| `owned_count` | Partitions the actor was expected to cache. |
| `in_l2` | Sorted partition ids found on disk (intersected with the owned slice). |
| `missing` | Owned partitions not on disk. |
| `l2_size_bytes_total` | Sum of apparent bytes across `part-ivf-{id}.bin` files in the active prefix dir. |
| `l2_file_count` | Number of partition files on disk in the active prefix dir. |
| `l2_prefix_dirs` | Sorted live (non-`.deleting-`) prefix subdirs under `v1/`. Length > 1 means the residency claim was refused due to stale-prefix ambiguity. |
| `l1_size_bytes_at_probe` | `Session.size_bytes()` at probe time; `-1` when the cluster is no longer reachable. |
| `probe_duration_s` | Wall-time of the walk + RPC. |

The v4 per-partition `in_l1` / `not_in_l1` lists, the `in_ram` /
`not_in_ram` aliases, and the `l2_residency_source` field are gone.
The driver's pre/post L2 directory snapshot delta block (above)
surfaces unexpected L2 growth and any new tombstones
(`tombstones_added=True` is a hard error from a failed invalidation
rename).

### Freshness drill (`--simulate-invalidation`)

Pair `--scenario distributed --prewarm sharded` with
`--simulate-invalidation` to exercise the v6 freshness contract
end-to-end after the first measure phase. The drill:

1. Calls `Session.invalidate_index_cache(uri, index_addr)` on every
   worker (and the `CoordinatorActor` under `--mode sharded`) with one
   retry on `IOError` — anything beyond that re-raises and aborts the
   run.
2. Walks each actor-local L2 directory via `snapshot_l2_dir` and
   verifies the per-prefix `v1/{sanitize(prefix)}/` subdir is gone or
   has been atomically renamed to `.{sanitize(prefix)}.deleting-{nonce}/`.
   A surviving non-deleting subdir is a freshness-contract violation
   and the drill hard-fails.
3. Reruns the v6 strict sharded prewarm to measure the cold L2
   rehydration cost.
4. Reruns the measure phase. Per-k latency deltas (`(measure2 -
   measure1) / measure1 * 100`) are written to
   `<out-dir>/invalidation.json` alongside per-actor invalidate
   times, the rehydrate-prewarm wall-time, and full first/second
   percentile summaries; both are expected to fall within noise
   (~5%) once L2 is rehydrated.

Recommended on any first run against a new pylance build because it
catches regressions in the Rust generation / tombstone protocol that
unit tests alone do not. Skip it (default) when an existing build is
already known good and the run is purely measuring steady-state
latency.

The same probe is exposed as a standalone script that can attach to a
live Ray cluster while the bench's actors are still alive — useful for
ad-hoc snapshots between phases or after manual experiments:

```bash
python -u check_l2_residency.py \
    --num-actors 2 --num-partitions 3000 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --label adhoc \
    --out out/distributed-real-2actors/partition_residency.jsonl
```

The script resolves the bench's named actors (`hybrid-search-actor-<i>`)
via `ray.get_actor(...)` to fetch `Session.size_bytes()`; pass
`--no-attach` for an L2-walk-only report (`l1_size_bytes_at_probe` is
then reported as `-1`). `--help` and `--no-attach` work in environments
without ray installed — the ray import is deferred to the attach path.

### Deprecated v4 flags

`--codecless-mb`, `--prewarm-ram-fraction`, and `--l2-gb` are accepted
by the driver for back-compat with v4 invocation scripts but ignored
under the v6 distributed cache:

- `--codecless-mb` carved a codec-less Moka tier out of the v4 hybrid
  `--dram-gb` budget. v6 has no codec-less Moka tier — the
  distributed-cache DRAM is the metadata L1 plus partition L1 tier,
  sized by `--metadata-l1-mb` + `--partition-l1-mb`.
- `--prewarm-ram-fraction` scaled the v4 foyer L1 prewarm target. The
  v6 strict prewarm path places every owned partition directly in L2
  and admits to partition L1 up to its cap from within the backend;
  there is no operator-visible L1 budget to scale. Values other than
  `1.0` are flagged as ignored.
- `--l2-gb` was the v4 informational L2 capacity. v6 does no L2
  capacity bookkeeping (`l2_writes_total` and friends are exposed only
  on the Rust side); the operator sizes the actor's NVMe filesystem
  against `owned_count * partition_size` directly.

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
