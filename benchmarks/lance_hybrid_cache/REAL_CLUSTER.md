# Real 3-node Ray cluster with separate MinIO

> **Status — Lance 6.0 distributed-cache port.** The driver's scenario /
> session construction targets `Session.with_distributed_cache`;
> `--scenario hybrid` is a deprecated alias for `--scenario distributed`,
> and `--codecless-mb`, `--prewarm-ram-fraction`, and `--l2-gb` are
> accepted but ignored. The per-partition residency probe is disabled
> for `--scenario distributed`. **`--mode sharded` / `--prewarm
> sharded` are no longer driver-blocked**; the actor
> (`measure_sharded`, `search_partitions`, and
> `CoordinatorActor.__init__`) gates `dataset.compute_partition_ids` /
> `dataset.search_partitions` via `hasattr` and raises a clear
> `RuntimeError` on first use if a pylance build is missing either
> wrapper (see
> [`plans/benchmark/lance-v6-api-verification.md`](../../plans/benchmark/lance-v6-api-verification.md)).
> Sharded prewarm calls the v6 strict
> `dataset.prewarm_index(name, partition_ids=...)` path; the driver
> hard-fails before measurement if the per-actor L2 file walk reports
> missing / extra partitions for a `distributed` actor. The example
> below pins `--mode replicated --prewarm forced` (every actor calls
> `dataset.prewarm_index(name)` in parallel — the v6 strict prewarm
> path) as the safest cross-build configuration; switch to
> `--mode sharded` once your pylance ships the sharded wrappers. The
> wider sharded prewarm narrative below still uses v4 hybrid
> terminology — see
> [`plans/benchmark/lance-distributed-cache-6.0.md`](../../plans/benchmark/lance-distributed-cache-6.0.md)
> for the full v6 migration plan.

This guide runs the distributed Lance hybrid-cache benchmark on one
coordinator/head node, two actor nodes, and a separate MinIO node. Under
the v6 port `--mode replicated --num-actors 2` is the topology used in
the example: each actor caches the full partition slice and answers
queries independently. The coordinator-routed full-recall path
(`--mode sharded`) runs against any pylance build that ships the
sharded wrappers; in builds that do not, the actor raises on first use
rather than the driver pre-blocking the run.

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
# the v4 per-partition L1 residency probe have no v6 no-load
# equivalents (the L2 directory walk + `Session.size_bytes()` is the
# v6 aggregate-only replacement; see the status banner above).
# `--mode replicated --prewarm natural` splits the warmup queries
# across actors and lets each per-actor Moka cache converge under its
# `--dram-gb` cap.
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
- `=== L2 residency check: post-prewarm ===` before measurement, then
  `=== L2 residency check: post-measure ===` after measurement, with
  per-actor lines reporting
  `owned=<n> in_l2=<n> missing=<n> l2_files=<n> l2_bytes=<bytes>
  l1_bytes=<bytes> probe=<seconds>` — the verification that prewarm
  placed the expected partitions on disk (file presence under
  `{l2_dir}/v1/{sanitize(prefix)}/part-ivf-{id}.bin` one-to-one maps to
  L2 residency under the v6 layout) and a coarse `Session.size_bytes()`
  readout for the L1 tier. The v4 per-partition `in_l1` / `not_in_l1`
  lists and `l2_source=...` inference field are gone — Lance 6.0 has
  no no-load L1 probe, so the L1 half is byte-total only.
- `=== L2 directory snapshot (post-measure) ===` with per-actor lines
  reporting `files=<n> apparent=<bytes> disk=<bytes>` plus a
  `Δ(apparent=..., disk=..., files=...)` delta against the
  post-prewarm snapshot. Stable totals confirm query traffic did not
  trigger extra L2 writes (no L1→L2 writeback for vector partitions). A
  `tombstones_added=True` field appended to the delta is a hard error:
  it means an invalidation rename hit the failure path and the v6
  backend wrote a tombstone so future opens of that prefix skip it.

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

The same probe is exposed as a standalone script that can attach to a
live Ray cluster while the bench's actors are still alive — useful for
ad-hoc snapshots between phases or after manual experiments:

```bash
python -u check_l2_residency.py \
    --num-actors 2 --num-partitions 3000 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --label adhoc \
    --out out/hybrid-real-2actors/partition_residency.jsonl
```

The script resolves the bench's named actors (`hybrid-search-actor-<i>`)
via `ray.get_actor(...)` to fetch `Session.size_bytes()`; pass
`--no-attach` for an L2-walk-only report (`l1_size_bytes_at_probe` is
then reported as `-1`). `--help` and `--no-attach` work in environments
without ray installed — the ray import is deferred to the attach path.

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
