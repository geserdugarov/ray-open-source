# Integration architecture

Ray integrates with other projects in more than one way. Understanding which
side owns execution, data, and durability prevents accidental coupling.

## 1. Four integration patterns

```text
                         Ray application
                               |
          ┌────────────────────┼────────────────────┐
          v                    v                    v
  1. Native Ray adapter  2. Library on Ray   3. Native client in task
  read_*/write_* API     external project    arbitrary Python/Rust/C++
  Train/Tune callback    uses Ray backend     client called by workers
          |                    |                    |
          └────────────────────┼────────────────────┘
                               v
                   4. Shared interchange layer
                   Arrow / NumPy / pandas / files
```

### Native adapter

Ray contains a datasource, datasink, trainer, callback, or deployment adapter.
Ray owns distributed execution while the external library owns its native
protocol. The Lance Ray Data connector is this pattern.

### External library using Ray as a backend

The integration lives mostly in another project, which translates its own API
into Ray tasks/actors. Modin, Daft's distributed Ray runner, and RayDP are
examples of this broader model.

### Native client inside tasks or actors

Application code calls an external project's ordinary client from Ray remote
functions or actors. This is the most flexible pattern and is how Lance 12's
distributed index-segment API can be orchestrated today.

### Interchange without shared scheduling

Systems can exchange Arrow tables, pandas DataFrames, NumPy tensors, files, or
URIs without one becoming the other's execution backend. The copy and lifetime
behavior depends on process/node boundaries.

## 2. Data and table integrations

Ray Data's repository includes readers, writers, or conversion APIs across
several categories. This is representative rather than a stability promise for
every direction.

| Category | Examples in this checkout | Typical boundary |
|---|---|---|
| Files | Parquet, CSV, JSON, NumPy, TFRecord, WebDataset, images, audio, video, MCAP, Zarr | Ray tasks read/write files; Arrow/pandas blocks flow through Ray |
| Versioned tables | Lance, Iceberg, Delta Lake, Delta Sharing, Hudi, Unity Catalog/Databricks tables | Native table client plans files/fragments and owns commits |
| Databases/warehouses | SQL, MongoDB, BigQuery, ClickHouse, Snowflake | Partitioned queries or batched writes through native connectors |
| Streams/messages | Kafka | Tasks consume/produce records; broker owns retention and ordering |
| AI/vector services | Turbopuffer sink, Lance storage/query integration | Ray prepares/batches data; external service/format owns indexes and queries |
| In-memory/distributed frames | Arrow, pandas, NumPy, Dask, Modin, Spark, Mars, Daft | Conversion or object-reference exchange |
| Dataset hubs | Hugging Face datasets, LeRobot | Native metadata/files converted into Ray blocks |

The recurring read architecture is:

```text
 external metadata/listing
          |
          v
 driver creates partitions/read descriptions
          |
          v
 Ray workers call native readers in parallel
          |
          v
 Arrow/pandas blocks in Ray object stores
```

The recurring transactional write architecture is:

```text
 Ray workers write data/staging objects
          |
          v
 driver collects task metadata
          |
          v
 native table/catalog commit publishes one new state
```

Not every sink implements that last step. The external project's transaction
model—not `Dataset.write_*` by itself—defines visibility and conflict handling.

## 3. ML-framework integrations

Ray Train contains integrations or examples for PyTorch, TensorFlow, JAX,
XGBoost, LightGBM, Horovod, Hugging Face Transformers, Lightning, DeepSpeed,
Accelerate, and related tools. Responsibilities commonly divide as follows:

| Ray owns | Framework/native library owns |
|---|---|
| Worker lifecycle and placement | Tensor operations and autograd |
| CPU/GPU/custom-resource allocation | Model/optimizer semantics |
| Runtime environments | Collective algorithms and device kernels |
| Failure/restart policy around workers | Framework checkpoint contents |
| Ray Data shard delivery | Per-step training loop |
| Cluster elasticity/orchestration | NCCL/Gloo/MPI traffic between workers |

```text
 Train coordinator
      |
      +─ actor rank 0 ─┐
      +─ actor rank 1 ─┼── NCCL/Gloo/MPI/framework channel
      +─ actor rank N ─┘
      |
      +─ Ray control: placement, health, retries, checkpoints
      +─ Ray Data: input block shards
```

Large tensors should use the framework's collective/data path where supported,
instead of routing every gradient through the driver or GCS.

## 4. Serving and application integrations

Ray Serve turns deployment definitions into controller/router/replica actors.
A replica can host almost any Python model server or client: PyTorch,
TensorFlow, Transformers, vLLM-style engines, database clients, and native
Lance dataset handles. Ray owns replica scheduling and lifecycle; the embedded
engine owns inference kernels or storage-query execution.

For Lance ANN serving, a long-lived Serve replica or named actor is generally a
better fit than constructing a Ray Dataset for each request:

```text
 request
    |
 Serve router
    |
 replica actor (long-lived Lance dataset/session/index cache)
    |
 native Lance nearest query
    |
 response
```

This preserves Lance's session and index caches and avoids a Ray Data global
candidate-reduction stage for a latency-sensitive point query.

## 5. Execution-engine integrations

Ray can coexist with or back other distributed engines:

- Dask collections can be converted to/from Ray Data, and Dask-on-Ray patterns
  can use Ray scheduling in supported integrations.
- Modin distributes pandas-style work over Ray.
- RayDP runs Spark alongside/on Ray and exchanges data for downstream Ray work.
- Daft can use Ray as its distributed runner.
- Custom actors can wrap MPI/NCCL systems or services that use their own RPC
  transport.

These systems may have their own scheduler, memory manager, shuffle, and retry
semantics. An integration must establish which scheduler owns each unit of work
and avoid double-counting resources or materializing the same dataset in two
object systems unintentionally.

## 6. Infrastructure integrations

Ray can run on local machines, VM clusters, clouds, or Kubernetes. KubeRay maps
Ray head/worker nodes to pods and handles Kubernetes-native cluster lifecycle.
The Ray autoscaler reacts to logical resource demand from pending tasks and
actors; it does not replace a database autoscaler or scale from application QPS
unless that QPS creates Ray resource demand.

External durable stores remain external even when deployed in the same
Kubernetes cluster:

```text
 Kubernetes / cloud infrastructure
 ├── Ray head pod/node
 ├── Ray worker pods/nodes
 ├── object storage endpoint / mounted filesystem
 └── catalog or namespace service

 Ray schedules compute; storage/catalog services retain durable state.
```

## 7. Lance's position in the ecosystem

Ray and Lance are complementary:

```text
                       Ray                              Lance 12
          ┌────────────────────────┐       ┌────────────────────────────┐
          │ distributed compute    │       │ durable columnar dataset   │
          │ tasks and actors       │       │ immutable fragments        │
          │ CPU/GPU scheduling     │◀─────▶│ versioned manifests        │
          │ streaming batch ETL    │ Arrow │ scalar/vector/FTS indexes  │
          │ serving replicas       │       │ scanner and transactions   │
          └────────────────────────┘       └────────────────────────────┘
                     │                                  │
                     └──────── local/S3/GCS/Azure ──────┘
```

| Concern | Ray responsibility | Lance responsibility |
|---|---|---|
| Parallel batch ingestion | Schedule and backpressure write tasks | Encode fragment files and validate fragment metadata |
| Atomic data publication | Gather worker results; call final callback | Commit one new manifest version |
| Parallel extraction | Partition fragment groups into Ray read tasks | Scan, filter, decode, and optionally use indexes |
| Vector index build | Optional external orchestration with tasks/actors | Train/build segment payloads, validate/merge, commit |
| ANN query | Host clients/replicas or reduce distributed candidates | Index selection, partition search, delta reconciliation, refine |
| Version/time travel | Pass requested version | Define and open immutable snapshots |
| Credentials | Transport options and run providers on workers | Object-store and namespace integration |
| Durable build job record | Application/external database | Not supplied by Lance's segment protocol or Ray ObjectRefs |

For the exact baselines in this documentation, that ownership design is
sound but the write adapter is not source-compatible: Ray uses the removed
`storage_options_provider=` Python keyword, while Lance 12 uses
`namespace_client=` and `table_id=`. Batch reads have matching inspected
signatures; batch writes require the port described in
[05-ray-lance-integration.md](05-ray-lance-integration.md#51-compatibility-gate).

## 8. Focused Lance integration modes

### Batch ingest

Use Ray Data to read many sources, transform/embed rows, and call
`write_lance` after adapting its pylance-4-era provider arguments to Lance 12's
`namespace_client`/`table_id` API. Ray then distributes physical fragment
creation and Lance performs the single manifest commit. Until that adapter is
present and tested, orchestrate Lance 12's native fragment/commit calls from Ray
tasks instead. Build/append indexes afterward as a separate workflow.

### Batch extract and preprocessing

Use `read_lance` with a pinned version, explicit `columns`, and Ray expression
filters. Lance performs fragment scans and predicate evaluation; Ray streams
the resulting Arrow blocks through CPU/GPU transforms or training shards.

### Online search

Keep a native Lance dataset/session in Ray Serve replicas or actors and issue
normal Lance scanner/nearest calls. This uses Lance's index cache and complete
logical-index query plan.

### Distributed index build

Use Ray Core tasks as the external scheduler for Lance 12's
`create_index_uncommitted` calls, persist the job contract externally, validate
the barrier, optionally perform one-level compatible merges, and commit exactly
once. This is not implemented by `Dataset.write_lance`.

See [05-ray-lance-integration.md](05-ray-lance-integration.md) for the concrete
API and correctness contract.

## 9. Integration design checklist

Before combining Ray with any external project, answer:

1. Which system owns partitioning and global reduction?
2. Where does bulk data move: Ray object store, native RPC, or object storage?
3. Which system supplies the authoritative transaction/commit?
4. Are task retries safe for external side effects?
5. What state survives driver, actor, node, and cluster failure?
6. How are credentials made available and refreshed on every worker?
7. Which system accounts for heap, shared memory, temporary disk, and GPU
   memory?
8. What compatibility matrix is tested for Arrow and native-library versions?

## 10. Primary source map

- Ray Data public readers: [`python/ray/data/read_api.py`](../../python/ray/data/read_api.py)
- Ray Data public writers: [`python/ray/data/dataset.py`](../../python/ray/data/dataset.py)
- Ray Train integrations: [`python/ray/train`](../../python/ray/train)
- Ray Tune integrations: [`python/ray/tune/integration`](../../python/ray/tune/integration)
- Ray Serve: [`python/ray/serve`](../../python/ray/serve)
- Ray ecosystem catalog: [`doc/source/ray-overview/ray-libraries.rst`](../../doc/source/ray-overview/ray-libraries.rst)
- Out-of-band communication guidance: [`doc/source/ray-core/actors/out-of-band-communication.rst`](../../doc/source/ray-core/actors/out-of-band-communication.rst)
- Lance connector source map: [05-ray-lance-integration.md](05-ray-lance-integration.md#13-source-map)
