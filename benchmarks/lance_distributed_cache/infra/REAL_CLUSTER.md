# Run on a real cluster

This follows the older `lance_hybrid_cache` deployment: build artifacts on your
development machine, ship them to a Ray head and two workers, and use a fourth
machine for MinIO. The head runs the driver; each worker runs one replicated
search actor with its own local L2 directory. These scripts use the Lance 12
benchmark and its strict prewarm, restart, and invalidation checks.

Run the commands below from `benchmarks/lance_distributed_cache` on the
development machine. SSH must work without prompts. All Ray machines must use
the same Python minor, architecture, compatible glibc, and CPU instruction
baseline as the build machine. The default Python is 3.12.

All machines need Bash and `rsync`. The Ray machines and development machine
need Python with `venv` support; the development machine also needs `unzip` and
`sha256sum`. The MinIO machine needs Docker Compose v2 with permission to use
Docker. No compiler is needed on the cluster nodes.

## Configure once

```bash
cp infra/real_cluster.env.example infra/real_cluster.env
$EDITOR infra/real_cluster.env
```

Set `SSH_USER`, `COORD_IP`, `ACTOR0_IP`, `ACTOR1_IP`, and `MINIO_HOST` to your
private, routable addresses. Optional `*_SSH_HOST` settings support SSH aliases
or bastions independently of those addresses. The local config is ignored by
Git and excluded from shipping. `CLUSTER_ENV=/path/to/settings` selects another
Bash config file; exported variables also work when no config file is present.

Set `NVME_DIR` to the same absolute, writable, local-disk path on both workers.
For example, prepare it once on each worker:

```bash
sudo mkdir -p /mnt/nvme/lance-cache-bench
sudo chown "$USER" /mnt/nvme/lance-cache-bench
df -h /mnt/nvme/lance-cache-bench
```

Use a physical SSD/NVMe filesystem with enough free space for all retained
runs. Each actor persists the whole index; L2 has no capacity manager. The head
does not need this disk. Keep the Ray control ports reachable among the three
Ray machines: TCP 6379, 8265, 8076–8079, 52365, and 10002–10100. MinIO must be
reachable from all three at TCP 9000. The scripts bind the dashboard and MinIO
to the LAN; use these on your private benchmark network.

## Build the wheelhouse and deploy

Build pylance from the cache checkout as described in the benchmark
[README](../README.md#environment). Set `LANCE_WHEEL` in your config to that
exact wheel. If there is exactly one wheel in the sibling
`lance-open-source/python/dist`, it is selected automatically. Building the
wheelhouse reuses this artifact, so repeating a deploy does not compile Lance.

Use a regular distribution wheel from `maturin build`. An editable wheel from
`uv sync`, `maturin develop`, or uv's editable cache can contain only
`pylance.pth` and metadata. That file points at the build machine's source
checkout and cannot supply Lance on another node. To build a regular wheel
from the benchmark directory:

```bash
(
  cd ../../../lance-open-source/python
  uv run --frozen maturin build --profile release-with-debug --out dist/cluster
)
```

Set `LANCE_WHEEL` in `infra/real_cluster.env` to the exact new wheel under
`lance-open-source/python/dist/cluster`. Use the filename produced by the build;
the platform tag may differ from the previous wheel.

```bash
bash infra/build_wheelhouse.sh
bash infra/ship_real_cluster.sh
```

The build helper creates a dedicated local venv under `out/build-venv` and
downloads binary dependencies into `out/wheelhouse`. `BUILD_PYTHON` can select
an existing venv with pip. It resolves the exact pylance artifact together with
`requirements.txt`, including `ray[default]>=2.58,<2.59` and the Lance 12
namespace dependency. Optional `RAY_WHEEL` supplies a local Ray wheel compatible
with that requirement. Use the same wheelhouse for all nodes; it must contain
exactly one pylance and one Ray wheel. Checksums and API markers are checked
before shipping. A failed dependency resolution leaves the previous staged
wheels in place and retains its temporary download directory for inspection.
The selected Lance wheel is checked before downloading dependencies. Run the
shipping command only after the wheelhouse build succeeds.

The verifier accepts Python modules at the wheel root or inside its
`.data/purelib` and `.data/platlib` directories. If a required module is missing,
the error names the selected wheel. Inspect it with `unzip -l /path/to/pylance.whl`
and set `LANCE_WHEEL` to the intended cache build; this helper reuses a prebuilt
wheel and does not rebuild Lance.

Deployment mirrors the benchmark and wheelhouse onto all three Ray nodes,
creates a venv on each, installs entirely offline, and validates the installed
Lance API. It force-reinstalls the artifacts because cache-branch builds can
share the same `12.0.0` version. The MinIO node receives only `infra/`.

For later benchmark-only edits:

```bash
bash infra/ship_real_cluster.sh --code-only
```

For a new Lance/Ray wheel, stop the benchmark cluster first, rebuild the
wheelhouse, deploy, and start again. Deployment preserves remote `.venv`,
`out`, and `results` directories during source synchronization. Wheelhouse
contents are mirrored into the dedicated remote wheelhouse directory.

## Start and smoke test

```bash
bash infra/run_real_cluster.sh start
bash infra/run_real_cluster.sh minio
bash infra/run_real_cluster.sh status
bash infra/run_real_cluster.sh smoke
```

`start` starts a head with zero schedulable CPUs and two workers, each with
exactly one `search_actor_node` resource. It expects no existing Ray instance
on those machines. `minio` starts Docker Compose with a LAN bind, waits for
health, and creates the bucket from the head's venv. The existing Compose
configuration uses `minioadmin` credentials. Set `MINIO_IMAGE` to an available
image if the default cannot be pulled.

If MinIO is already running elsewhere, set `ENDPOINT_URL` and skip `minio`.
Create the bucket in that service first. The benchmark's existing
`start_minio_native.sh` is also available if container images are unavailable;
run it on the storage machine with `MINIO_BIND_ADDR=0.0.0.0` and then create the
bucket using `make_bucket.sh` from a Ray node's venv.

The smoke run uses 10,000 vectors, 32 dimensions, eight partitions, and two
actors. It exercises all three cache scenarios, actor restart, result equality,
and invalidation. Every run checks that both expected worker IPs advertise the
placement resource before starting the driver. Actor restarts retain their
original node placement and L2 directory.

## Run and collect

The full run uses the driver's defaults: 10 million vectors, 1024 dimensions,
3000 IVF_RQ partitions, and all three cache scenarios. It can take a long time
to create and index this dataset. Extra arguments are passed to the benchmark:

```bash
# Full benchmark; include invalidation verification on the first run.
bash infra/run_real_cluster.sh run --invalidation-drill

# Smaller performance run, suitable for an initial comparison.
bash infra/run_real_cluster.sh run \
  --scale 1000000 --dim 256 --num-partitions 512 \
  --metadata-l1-mb 16 --partition-l1-mb 32 \
  --k-list 10 --nprobes 16 --measure-queries 256

# Repeated small-L1, fitting-L1, and L2-only comparisons, two physical workers.
bash infra/run_real_cluster.sh comparison

bash infra/run_real_cluster.sh fetch
```

Keep `--num-actors 2` and `--cpus-per-actor 1` for this resource layout. Thread
defaults are one Lance CPU thread, eight Lance IO threads, and one BLAS thread.
Each invocation uses a new L2 run directory. Rerunning the same dataset shape
reuses the source dataset/index; `--skip-setup` bypasses its validation. Pass
`--partition-l1-mb 0` to isolate L2, or
`--drop-l2-page-cache-on-restart` to request file-page eviction before restart.
Set `LANCE_BUILD_COMMIT` to the commit used for the supplied wheel to record
its provenance in `run.json`.

The comparison uses the same profiles and rotated scenario order as the local
comparison runner. Set `REPETITIONS` or `EXPERIMENT_ID` in the config to change
its trial count or name. It also accepts driver arguments, such as
`comparison --drop-l2-page-cache-on-restart`. Failure prints the end of the
driver log and returns a nonzero exit status.

Results stay under `out/` on the head and are fetched to local
`out/real-cluster/`: each successful run has `run.json`, `events.jsonl`, and
`summary.csv`.

`run`, `smoke`, and `comparison` also stream stdout and stderr to the terminal
and a unique local `out/real-cluster/logs/client-<action>-<timestamp>-<suffix>.log`.
This includes worker precheck output and SSH errors. Set `LOCAL_LOG_DIR` to
choose another local directory. The benchmark command's output is also written
on the head to `out/logs/<action>-<timestamp>-<suffix>.log`, with start/end times
and its exit status. Both paths are printed before the precheck starts. Logs
remain available after failures, and failures still return a nonzero status.
`fetch` collects the head logs into local `out/real-cluster/logs/` without
overwriting the client logs. Comparison trials retain their individual console
logs as well. Analyze a fetched comparison with the local benchmark venv:

```bash
.venv/bin/python analyze_comparison.py out/real-cluster/<comparison-id> \
  --out-dir out/real-cluster/<comparison-id>/analysis
```

Run long commands inside `tmux` on the development machine to keep the SSH
session alive. Stop the dedicated Ray cluster when finished:

```bash
bash infra/run_real_cluster.sh stop
```

This stops Ray on both workers and the head. MinIO, its dataset volume, and
worker L2 files remain available for reuse. Inspect and remove retained run
directories manually when disk space is needed.
