# Ray project overview

## 1. Purpose

Ray is a general-purpose distributed runtime for Python and AI applications.
It lets an application express concurrency with functions and classes while
the runtime schedules work, moves immutable objects, manages resources, and
recovers selected failures across a cluster.

Ray's public surface has two broad layers:

```text
 Application code
      |
      +---------------- Ray AI libraries ----------------+
      |  Data  Train  Tune  Serve  RLlib                 |
      +-----------------------+----------------------------+
                              |
                              v
      +---------------- Ray Core API ---------------------+
      |  tasks  actors  ObjectRefs  placement groups     |
      +-----------------------+----------------------------+
                              |
                              v
      +--------------- Distributed runtime --------------+
      | scheduling | object transfer | metadata | faults |
      +-----------------------+----------------------------+
                              |
                              v
             machines / VMs / containers / Kubernetes
```

The AI libraries do not replace the runtime. They compile higher-level work
into Ray tasks, actors, objects, and resource requests.

## 2. Core programming model

### Tasks

A Ray task is an asynchronous invocation of a remote function. It is normally
stateless, may declare CPU/GPU/custom-resource needs, and returns one or more
`ObjectRef` values immediately. The referenced results become available later.

### Actors

A Ray actor is a stateful worker process created from a remote class. Actor
methods execute on that worker and can mutate its in-process state. Actors are
the natural abstraction for services, caches, parameter servers, coordinators,
and long-lived model replicas.

### Objects

Ray objects are immutable values addressed by `ObjectRef`. Large values are
stored in a per-node shared-memory object store and transferred between nodes
as dependencies require. Small values may be carried inline in RPCs. Object
metadata is tracked by the worker that owns the reference; object data can have
multiple physical copies.

### Placement and resources

Tasks and actors request logical resources. Per-node schedulers place them
according to availability, locality, scheduling strategy, labels, and placement
groups. Placement groups reserve multiple bundles atomically and support gang
placement such as `PACK`, `SPREAD`, `STRICT_PACK`, and `STRICT_SPREAD`.

## 3. Higher-level libraries

| Library | Role | Runtime mapping |
|---|---|---|
| Ray Data | Streaming batch ETL, preprocessing, batch inference, and data feeding | Lazy operator DAG executed by task or actor pools over object-store blocks |
| Ray Train | Distributed training coordination | Worker actors, collective communication, checkpoints, and Ray Data shards |
| Ray Tune | Hyperparameter and experiment scheduling | Trials composed of tasks/actors plus search and scheduling logic |
| Ray Serve | Online model/application serving | Long-lived deployment actors, routers, replicas, and autoscaling control loops |
| RLlib | Reinforcement learning | Distributed environment runners, learners, replay components, and coordinators |

These libraries are composable. A common AI pipeline uses Ray Data for ingest
and feature generation, Train for fitting, Tune for experiment search, and
Serve for deployment on the same underlying runtime.

## 4. Cluster model

A Ray cluster has one head node and zero or more worker nodes. Every node can
host user workers, a local scheduler (`raylet`), and a local object store. The
head additionally hosts singleton cluster-management components such as the
Global Control Service (GCS), dashboard, and usually the autoscaler.

The driver is the worker process running a Ray job's entry point. It is a job
concept, not necessarily the head node. Jobs submitted through the Ray Jobs API
normally start their driver on the cluster; an interactive client can connect
from elsewhere.

Two design choices are central:

1. Scheduling and object transfer are distributed across nodes instead of
   routing every task and byte through the head.
2. Control metadata and bulk object data take different paths. The GCS manages
   cluster-level metadata and operations; object managers move large values
   peer to peer.

[01-architecture.md](01-architecture.md) expands this topology.

## 5. Ray Data in the project

Ray Data is the part of Ray most relevant to Lance. A `Dataset` is a lazy,
distributed collection. Its rows are partitioned into blocks, normally Arrow
tables. API calls build a logical plan; consumption creates an optimized
physical plan whose operators stream block references through task or actor
pools.

```text
 external files/table
        |
        v
    Datasource --read tasks--> Arrow/pandas blocks in Ray object stores
                                       |
                         map / filter / shuffle / inference
                                       |
                                       v
                              Datasink write tasks
                                       |
                                       v
                             external files/table
```

Blocks are execution units, not persistent table fragments. When Ray reads
Lance, one read task can emit multiple Ray blocks. When Ray writes Lance, one
write task can create one or more Lance fragments. No permanent one-to-one
mapping is guaranteed.

## 6. What Ray deliberately does not own

Ray provides execution and orchestration, but delegates several concerns:

| Concern | Typical owner |
|---|---|
| Durable table snapshots and transactions | Lance, Iceberg, Delta Lake, or another table system |
| Vector/scalar/full-text indexes | Lance, a vector database, or a search system |
| Durable workflow/job state across cluster loss | Application database or workflow system |
| Container and node lifecycle | Kubernetes/KubeRay, cloud VM launcher, or another infrastructure layer |
| Tensor collectives | NCCL, Gloo, MPI, or framework-specific communication libraries |
| Model serialization and registry | Training framework, MLflow, cloud registry, or application code |

This boundary matters for Lance: Ray can parallelize fragment reads, fragment
writes, and Lance 12 index-segment builds, but Lance remains authoritative for
manifests, commit validation, query behavior, and on-disk compatibility.

## 7. Repository map

```text
ray-open-source/
├── src/ray/                 C++ runtime
│   ├── core_worker/         task submission/execution and object ownership
│   ├── raylet/              per-node scheduling and worker management
│   ├── object_manager/      object store and cross-node transfer
│   └── gcs/                 cluster-level control metadata and operations
├── python/ray/              Python API and libraries
│   ├── data/                Ray Data API, planner, executor, sources/sinks
│   ├── train/               distributed training
│   ├── tune/                experiment execution
│   ├── serve/               serving control/data plane
│   └── rllib/               reinforcement learning
├── rllib/                   repository-level RLlib link/layout
├── doc/source/              Sphinx/MyST documentation
└── release/                 release tests, workloads, and tooling
```

The Lance connector specifically lives under:

```text
python/ray/data/read_api.py
python/ray/data/dataset.py
python/ray/data/_internal/datasource/lance_datasource.py
python/ray/data/_internal/datasource/lance_datasink.py
python/ray/data/_internal/datasource/lance_utils.py
python/ray/data/tests/datasource/test_lance.py
```

## 8. Execution and durability expectations

- Ray Dataset transformations are lazy until a consumption method runs.
- Ray objects are immutable but not durable database records. They can spill to
  configured storage, but spilling is a runtime memory mechanism.
- Task lineage can reconstruct selected lost task outputs while the owner and
  required lineage survive. It is not equivalent to a table transaction log.
- Actor state is process state unless the application checkpoints it.
- External sink commits define durable visibility. For the Lance sink, worker
  tasks first write uncommitted fragments and the driver publishes them with
  one Lance manifest commit.

## 9. Primary source map

- Project summary: [`README.rst`](../../README.rst)
- Core abstractions: [`doc/source/ray-core/key-concepts.rst`](../../doc/source/ray-core/key-concepts.rst)
- Cluster concepts: [`doc/source/cluster/key-concepts.rst`](../../doc/source/cluster/key-concepts.rst)
- Ray Data internals: [`doc/source/data/data-internals.rst`](../../doc/source/data/data-internals.rst)
- Current version: [`python/ray/_version.py`](../../python/ray/_version.py)
- C++ runtime: [`src/ray`](../../src/ray)
- Python libraries: [`python/ray`](../../python/ray)

