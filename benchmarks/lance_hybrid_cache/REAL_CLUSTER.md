# Real 3-node Ray cluster with separate MinIO

This guide runs the distributed Lance hybrid-cache benchmark on one
coordinator/head node, two actor nodes, and a separate MinIO node. Keep
`--mode sharded` and set `--num-actors 2`: the coordinator owns IVF routing
and top-K merge, while the two workers own disjoint partition slices.

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

# Run 1: hybrid, full-recall sharded topology across two physical workers.
# Each actor owns 1500 of 3000 partitions (round-robin mod 2). With
# --dram-gb 1 --codecless-mb 64 the per-actor foyer L1 budget is
# ~960 MiB; deterministic prewarm fills that first, then forces the
# rest of the actor's ~5 GiB slice into the actor-local NVMe L2 dir.
# --warmup-queries 0 because deterministic sharded prewarm now covers
# every cache namespace the measure path touches.
# --pre-measure-residency-probe verifies placement right after prewarm;
# the driver also runs the post-measure probe before closing actors.
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario hybrid \
    --num-actors 2 --dram-gb 1 --l2-gb 8 --codecless-mb 64 \
    --nvme-dir /mnt/nvme/lance-l2/distributed \
    --mode sharded \
    --actor-resource search_actor_node \
    --coordinator-resource coord_node \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 1000 \
    --pre-measure-residency-probe \
    --endpoint-url "http://$MINIO_HOST:9000" \
    --out-dir out/hybrid-real-2actors \
    2>&1 | tee bench-distributed-hybrid-real-2actors.log

# Run 2: moka baseline against the dataset/index created by Run 1.
# Deterministic prewarm uses policy='moka_ram_cap' with ram_bytes=1 GiB
# per actor; load stops once the per-actor DRAM budget is full so the
# remaining ~4 GiB of the actor's slice is not pulled from MinIO.
# The same residency probes show which owned partitions remain in DRAM
# before and after the measured query run.
python -u run_distributed_bench.py \
    --scale 10000000 --dim 1024 --num-partitions 3000 --num-bits 8 \
    --scenario moka \
    --num-actors 2 --dram-gb 1 \
    --mode sharded \
    --actor-resource search_actor_node \
    --coordinator-resource coord_node \
    --k-list 1000 --nprobes 32 \
    --warmup-queries 0 --measure-queries 1000 \
    --pre-measure-residency-probe \
    --endpoint-url "http://$MINIO_HOST:9000" \
    --skip-setup \
    --out-dir out/moka-real-2actors \
    2>&1 | tee bench-distributed-moka-real-2actors.log
```

The expected console shape is:

- `Distributed summary (2 actors, mode=sharded, ...)`
- One `[driver] sharded prewarm (deterministic, policy=..., ram_budget=...)`
  block followed by per-actor lines reporting
  `ram=<loaded_to_ram> disk=<loaded_to_disk> spilled=<count>
  stopped_before=<id|—> ram_deep=<bytes> disk_serialized=<bytes>`. For
  hybrid this is followed by a per-actor L2 directory footprint line.
- `[driver] coord routing warmup done in <seconds>s` (one-shot
  `compute_partition_ids` call that force-opens the coordinator's
  top-level vector index outside the measure timer).
- `coord per-query mean: centroid=... scatter=... merge=...`
- Two per-actor rows with balanced `owned=...` and `calls_handled=...`
- `=== Partition residency check: post-prewarm ===` before measurement,
  then `=== Partition residency check: post-measure ===` after measurement,
  with per-actor lines
  reporting `probed=<n> in_ram=<n> not_in_ram=<n>` plus a sample of
  partition ids in each tier — the verification that prewarm placed the
  expected partitions and that the cache still holds the expected state after
  the measured workload.

Because the commands above include `--pre-measure-residency-probe`, a
`Residency shift (post-prewarm → post-measure)` block reporting
`stayed_in_ram / evicted / promoted_into_ram` counts per actor prints after
the post-measure block. If you omit the flag for a purer latency run, the
driver still runs the post-measure probe, but skips the post-prewarm block and
the shift comparison.

### Partition residency verification

Without `--pre-measure-residency-probe`, only the post-measure probe runs: it
fires after the measured workload, so it cannot perturb the measurement, and it
is the only block written to `<out-dir>/partition_residency.jsonl`. The probe
calls Lance's `prewarm_vector_cache(name, [p], policy='moka_ram_cap',
ram_bytes=0)` once per owned partition: pass 1 charges DRAM-resident
partitions into `skipped_existing`, pass 2 short-circuits on
`ram_bytes_deep_size >= ram_bytes` (i.e. `0 >= 0`) before loading
anything — no storage read either way.

The commands above add `--pre-measure-residency-probe` to also probe between
prewarm and measure. The flag is opt-in because the probe is no-*load* but not
no-*touch*: it walks the cache access path once per owned partition
immediately before the measured workload, which can shift
replacement-policy state (recency / frequency / admission). The
hit/miss counter subtraction in the measure phase cancels the count
bump but not that policy bump, so by default the cache between prewarm
and measure is left untouched. Enable the flag only when you are
investigating prewarm placement and accept that the measure phase
observes a post-residency-scan cache rather than the raw post-prewarm
cache. With the flag set, both probes are written to
`<out-dir>/partition_residency.jsonl`, one JSON line per actor per
probe, with a `"label"` field of `"post-prewarm"` or `"post-measure"`
so downstream tooling can demux.

Each line carries the full sorted lists of in-RAM and not-in-RAM
partition ids, the session-level `index_cache_stats`, and (for hybrid)
the per-actor L2 directory footprint.

Per-partition L2 residency is not surfaced by current pylance (the
underlying Rust helpers `partition_is_cached` / `partition_is_in_l2`
exist but are not bound). Cross-reference `not_in_ram` against the
prewarm-time `loaded_to_disk` count plus the L2 directory footprint to
distinguish L2-resident partitions from absent ones: if
`len(not_in_ram)` equals the prewarm `loaded_to_disk` and the L2 dir
holds the expected on-disk bytes, every partition is accounted for.

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

### Foyer L1 sharding and the `spilled` column

Foyer's L1 is internally sharded (16 shards by default) with per-shard
S3FIFO eviction. Hash skew across shards can push one shard over its
slice of the budget while the global L1 footprint is still under
`ram_bytes`; under foyer's `WriteOnEviction`, the displaced entry is
routed to L2 rather than dropped. Requesting the full foyer L1
capacity (the default) therefore means a subset of L1-admitted
partitions may end up on L2 instead of DRAM. Those partitions stay
reachable through the cache — the spill only erodes the nominal L1
budget.

The `spilled=<count>` field on each per-actor prewarm line reports
this directly (it is also rolled into `disk=<loaded_to_disk>`). Use
`--prewarm-ram-fraction <f>` to scale the requested L1 budget if you
need deterministic full-L1 residency: `--prewarm-ram-fraction 0.5` is
a conservative starting point; smaller values trade nominal L1 fill
for guaranteed no spill. Only affects `--scenario hybrid`.

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
