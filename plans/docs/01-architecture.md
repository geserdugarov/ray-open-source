# Ray architecture

This document describes the current Ray runtime and Ray Data architecture. It
uses “control plane” for metadata, placement, and lifecycle decisions and “data
plane” for task RPCs and bulk object movement.

## 1. Layered architecture

```text
┌──────────────────────────────────────────────────────────────────────┐
│ Applications                                                         │
│ Python/Java/C++ jobs, notebooks, services, training programs         │
├──────────────────────────────────────────────────────────────────────┤
│ Ray libraries                                                        │
│ Data | Train | Tune | Serve | RLlib                                  │
├──────────────────────────────────────────────────────────────────────┤
│ Ray Core language APIs                                               │
│ remote functions | actors | ObjectRefs | resources | placement      │
├──────────────────────────────────────────────────────────────────────┤
│ Ray Core distributed runtime                                         │
│ CoreWorker | raylet | object manager/store | GCS | RPC/pub-sub       │
├──────────────────────────────────────────────────────────────────────┤
│ Cluster management and infrastructure                                │
│ Jobs | dashboard | autoscaler | KubeRay/VM launcher | metrics/logs   │
├──────────────────────────────────────────────────────────────────────┤
│ External systems                                                     │
│ object stores | table formats | databases | model frameworks        │
└──────────────────────────────────────────────────────────────────────┘
```

Dependencies normally point downward. Higher layers can use external systems
directly when their native protocol is more appropriate—for example, training
workers use NCCL for tensor collectives and Ray Data workers use Lance's native
scanner and fragment writer.

## 2. Cluster topology

```text
                                      control metadata / heartbeats
                         ┌───────────────────────────────────────────┐
                         │                 HEAD NODE                 │
                         │                                           │
 client / Jobs API ─────▶│  driver(s)                                │
                         │      │                                    │
                         │      v                                    │
                         │  CoreWorker ◀──────────────┐               │
                         │      │ task RPCs           │               │
                         │      v                     │               │
                         │  raylet + local scheduler  │               │
                         │      │                     │               │
                         │  object manager/store      │               │
                         │                            │               │
                         │  GCS ◀── dashboard         │               │
                         │   ▲  ◀── autoscaler        │               │
                         └───┼────────────────────────┼───────────────┘
                             │                        │
             node/resource   │                        │ worker leases,
             metadata        │                        │ task/actor RPCs
                             │                        │
               ┌─────────────┴───────┐    ┌──────────┴──────────────┐
               │    WORKER NODE A    │    │     WORKER NODE B       │
               │                     │    │                         │
               │ raylet / scheduler  │    │ raylet / scheduler      │
               │ worker processes    │    │ worker processes        │
               │ object manager/store│    │ object manager/store    │
               └──────────┬──────────┘    └──────────┬──────────────┘
                          │                          │
                          └──── bulk objects ────────┘
                               peer-to-peer
```

The head node is also a normal Ray node unless its resources are deliberately
reserved. It can run tasks and hold objects. The driver can run on the head,
another cluster node, or connect through supported client/job mechanisms.

### Component responsibilities

| Component | Scope | Primary responsibility |
|---|---|---|
| Driver/CoreWorker | Per job/process | Submit tasks and actor methods, own object metadata, resolve dependencies, receive results |
| Worker/CoreWorker | Per worker process | Execute Python/Java/C++ user code, produce objects, serve task/actor RPCs |
| Raylet | Per node | Resource accounting, distributed scheduling, worker leases/processes, local object lifecycle coordination |
| Object store (Plasma-derived) | Per node | Shared-memory storage for immutable large objects |
| Object manager | Per node | Locate, pull, push, spill, restore, and evict object copies |
| GCS | Cluster singleton | Nodes, actors, placement groups, jobs, cluster resources, and other cluster-level metadata/operations |
| Autoscaler | Cluster | Add/remove nodes based on logical resource demand |
| Dashboard/agents | Cluster/per node | State, logs, metrics, and management APIs |

The GCS is important but is not a central byte pipe and is not the executor for
every task. Running workers communicate directly after placement.

## 3. Task scheduling architecture

The simplified normal-task path is:

```text
 caller CoreWorker
      |
      | 1. build TaskSpecification; register return ObjectRefs
      v
 dependency resolver ── waits for top-level ObjectRef arguments
      |
      | 2. RequestWorkerLease
      v
 selected raylet ── local schedule or redirect to another raylet
      |
      | 3. leased worker address
      v
 caller CoreWorker ─────── PushTask RPC ───────▶ executor CoreWorker
                                                   |
                                  4. fetch local/remote arguments
                                                   |
                                  5. invoke user function
                                                   |
                             6a. small result inline in RPC
                                  or
                             6b. large result in local object store
                                                   |
 caller ObjectRef becomes ready ◀──────────────────┘
```

Actors use the same object and RPC substrate but add actor creation, lifetime,
restart, and ordered method execution semantics. Once an actor is placed,
methods target its dedicated worker rather than obtaining a generic worker for
every invocation.

## 4. Object and ownership architecture

```text
                         metadata owner
                    (caller/creating worker)
                    ┌────────────────────┐
 ObjectRef ─────────▶ location + lineage │
                    └───────┬────────────┘
                            │ knows copies
           ┌────────────────┼────────────────┐
           v                v                v
    node A object     node B object     spill storage
       store             store          (if configured)
      [copy]  ◀──── peer transfer ────▶ [copy]
```

Data and metadata have different fault domains. Ray can find another object
copy or reconstruct selected task-produced objects from lineage, but it cannot
generally recover an object after its owner process dies. `ray.put` values have
no producing task to replay.

Within a node, Arrow and NumPy-compatible buffers can often be read zero-copy
from shared memory. Cross-node movement still requires network transfer, and
Python objects that are not zero-copy-compatible must be deserialized.

## 5. Ray Data architecture

### 5.1 Planning layers

```text
 Dataset API calls (lazy)
 read_lance(...).filter(...).map_batches(...).write_*(...)
                         |
                         v
                logical operator DAG
       Read -> Filter -> MapBatches -> Write
                         |
              logical optimization
       predicate/projection pushdown, operator rewrites
                         |
                         v
                  physical planner
                         |
              physical optimization/fusion
                         |
                         v
                physical operator DAG
 InputDataBuffer -> TaskPoolMapOperator -> ... -> MapOperator(Write)
                         |
                         v
              streaming executor loop
                         |
             Ray tasks and actor pools
                         |
                         v
      object-store Block refs between operators
```

Logical operators describe what to do and are immutable. Physical operators
hold execution state and describe how to do it. A logical `Read` normally
becomes an input buffer plus a task/actor-backed map operator; compatible map
operators can be fused to avoid intermediate serialization.

### 5.2 Driver-side executor and cluster-side workers

```text
 driver or SplitCoordinator actor
 ┌────────────────────────────────────────────────────────────┐
 │ StreamingExecutor                                         │
 │                                                            │
 │  Op A out queue ──refs──▶ Op B input ──refs──▶ Op C queue │
 │     ▲                         ▲                            │
 │     │ task events             │ backpressure/resource data│
 └─────┼─────────────────────────┼────────────────────────────┘
       │                         │
       v                         v
 cluster tasks/actors       cluster tasks/actors
     produce blocks             consume/produce blocks
       │                         │
       └──────── Ray object stores ───────────────────────────
```

The executor manipulates object references; it does not normally pull every
data block through the driver. This keeps the driver out of the bulk-data path.
The executor schedules an operator when it has input, resources are available,
and the operator is not backpressured. Among viable operators, it favors the
one with the smallest output queue.

### 5.3 Blocks and streaming

A Ray Data block is usually a PyArrow `Table`, with pandas used when Arrow
cannot represent the data. Blocks are independently processed and live in Ray
object stores between stages. The current default target range is approximately
1 MiB to 128 MiB, with dynamic splitting above the safe-size threshold.

Non-shuffle operators can overlap in a streaming pipeline. Global sort,
group-by, repartition, and similar all-to-all operations create materialization
boundaries because rows must be redistributed.

```text
 read task 0 ──block──┐
 read task 1 ──block──┼─▶ CPU map ─▶ GPU map ─▶ filter ─▶ sink
 read task N ──block──┘      ▲           ▲
                              stages overlap while blocks flow

 shuffle boundary:
 upstream blocks ─▶ partition all inputs ─▶ exchange ─▶ reduce blocks
                    (requires wider coordination/materialization)
```

## 6. Memory architecture

Each node must accommodate three distinct consumers:

```text
 node memory
 ├── worker heap / logical task memory
 │     Python, pandas, tensors, native libraries, temporary Lance buffers
 ├── Ray object-store shared memory
 │     immutable blocks and task arguments/results
 └── system/runtime memory
       raylet, object manager, dashboard agent, OS, filesystem cache
```

Ray Data uses backpressure to bound queued object-store blocks and memory hints
to reduce heap oversubscription. Hints guide scheduling; they are not OS memory
limits. Ray Core may spill object-store values when shared memory fills, but it
cannot automatically spill arbitrary Python/native heap allocations. Lance
index builders and fragment writers therefore need explicit task memory sizing
and local temporary-disk capacity.

## 7. Fault and durability boundaries

| Failure | Runtime response | Durable-system implication |
|---|---|---|
| Task attempt fails | Retry according to task policy | Side effects must be idempotent or attempt-isolated |
| Worker/node loses an object | Fetch another copy or reconstruct lineage when possible | Does not alter a committed Lance dataset |
| Object owner dies | Referenced object may become unrecoverable | Persist index-build state outside ephemeral ObjectRefs |
| Actor dies | Restart according to actor options; in-memory state may be lost | Coordinator actors need durable checkpoints/job records |
| GCS fails | Cluster control is unavailable; durable GCS backend can support restart | Separate from Lance manifest durability |
| Sink task fails after writing files | Ray fails/retries the task | Uncommitted external files may remain and need cleanup |
| Driver fails before sink commit | No final sink callback | Lance fragments/index segments written so far remain unreferenced |

## 8. Extension points

Ray integrates with other systems at several clean boundaries:

- `Datasource` and `ReadTask` for external reads;
- `Datasink` with start/write/complete/failed callbacks for external writes;
- Ray tasks/actors for arbitrary native clients, including Lance index APIs;
- runtime environments for dependencies;
- custom resources and placement groups for accelerator/topology constraints;
- Serve deployments for online clients; and
- Train backends and out-of-band collectives for ML frameworks.

## 9. Primary source map

- Cluster model: [`doc/source/cluster/key-concepts.rst`](../../doc/source/cluster/key-concepts.rst)
- Task lifecycle: [`doc/source/ray-core/internals/task-lifecycle.rst`](../../doc/source/ray-core/internals/task-lifecycle.rst)
- Object behavior: [`doc/source/ray-core/objects.rst`](../../doc/source/ray-core/objects.rst)
- Object ownership/faults: [`doc/source/ray-core/fault_tolerance/objects.rst`](../../doc/source/ray-core/fault_tolerance/objects.rst)
- GCS behavior: [`doc/source/ray-core/fault_tolerance/gcs.rst`](../../doc/source/ray-core/fault_tolerance/gcs.rst)
- Ray Data internals: [`doc/source/data/data-internals.rst`](../../doc/source/data/data-internals.rst)
- Read planner: [`python/ray/data/_internal/planner/plan_read_op.py`](../../python/ray/data/_internal/planner/plan_read_op.py)
- Write planner: [`python/ray/data/_internal/planner/plan_write_op.py`](../../python/ray/data/_internal/planner/plan_write_op.py)
- Core runtime directories: [`src/ray`](../../src/ray)

