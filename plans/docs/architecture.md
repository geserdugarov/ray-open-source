# Ray Architecture

## 1. Cluster-Level Architecture

A Ray cluster consists of a **head node** and zero or more **worker nodes**. The head node runs the Global Control Store (GCS) and the Dashboard. Every node runs a Raylet and one or more worker processes.

```
+===========================================================================+
|                           RAY CLUSTER                                     |
|                                                                           |
|  +----------------------------------+  +-------------------------------+  |
|  |          HEAD NODE               |  |        WORKER NODE 1          |  |
|  |                                  |  |                               |  |
|  |  +----------+  +-------------+  |  |  +----------+  +-----------+  |  |
|  |  |   GCS    |  |  Dashboard  |  |  |  |  Raylet  |  |  Plasma   |  |  |
|  |  | Server   |  |  (Web UI)   |  |  |  |          |  | (Objects) |  |  |
|  |  +----------+  +-------------+  |  |  +----------+  +-----------+  |  |
|  |  +----------+  +-------------+  |  |  +-----------+ +-----------+  |  |
|  |  |  Raylet  |  |   Plasma    |  |  |  |  Worker1  | |  Worker2  |  |  |
|  |  |          |  |  (Objects)  |  |  |  | (CoreWkr) | | (CoreWkr) |  |  |
|  |  +----------+  +-------------+  |  |  +-----------+ +-----------+  |  |
|  |  +-----------+ +-----------+    |  |                               |  |
|  |  |  Worker1  | |  Worker2  |    |  +-------------------------------+  |
|  |  | (CoreWkr) | | (CoreWkr) |    |                                     |
|  |  +-----------+ +-----------+    |  +-------------------------------+  |
|  |                                  |  |        WORKER NODE N          |  |
|  +----------------------------------+  |                               |  |
|                                        |  +----------+  +-----------+  |  |
|                                        |  |  Raylet  |  |  Plasma   |  |  |
|                                        |  +----------+  +-----------+  |  |
|                                        |  +-----------+ +-----------+  |  |
|                                        |  |  Worker1  | |  Worker2  |  |  |
|                                        |  | (CoreWkr) | | (CoreWkr) |  |  |
|                                        |  +-----------+ +-----------+  |  |
|                                        +-------------------------------+  |
+===========================================================================+
```

## 2. Node-Level Architecture

Each node runs a Raylet process and multiple worker processes. The Raylet manages local scheduling, the worker pool, and coordinates with the Plasma object store.

```
+-----------------------------------------------------------------------+
|                         SINGLE RAY NODE                               |
|                                                                       |
|  +--------------------------------------------------------------+    |
|  |                        RAYLET PROCESS                         |    |
|  |                                                               |    |
|  |  +------------------+  +------------------+  +------------+  |    |
|  |  |   Node Manager   |  | Worker Pool Mgr  |  |  Agent Mgr |  |    |
|  |  | (task scheduling)|  | (start/stop wkrs)|  | (dashboard)|  |    |
|  |  +------------------+  +------------------+  +------------+  |    |
|  |  +------------------+  +------------------+  +------------+  |    |
|  |  | Local Resource   |  | Object Manager   |  | Placement  |  |    |
|  |  | Manager          |  | (pull/push objs) |  | Group Mgr  |  |    |
|  |  +------------------+  +------------------+  +------------+  |    |
|  +--------------------------------------------------------------+    |
|       |               |               |               |              |
|       | gRPC          | gRPC          | gRPC          | shared mem   |
|       v               v               v               v              |
|  +-----------+  +-----------+  +-----------+   +---------------+     |
|  |  Worker 1 |  |  Worker 2 |  |  Worker N |   | Plasma Store  |     |
|  |           |  |           |  |           |   | (shared mem   |     |
|  | CoreWkr   |  | CoreWkr   |  | CoreWkr   |   |  object store)|     |
|  | TaskExec  |  | TaskExec  |  | TaskExec  |   |               |     |
|  | RefCount  |  | RefCount  |  | RefCount  |   | - put/get     |     |
|  | ActorMgr  |  | ActorMgr  |  | ActorMgr  |   | - eviction    |     |
|  +-----------+  +-----------+  +-----------+   | - spilling    |     |
|                                                 +---------------+     |
+-----------------------------------------------------------------------+
```

## 3. Task Execution Flow

```
  User Python Code                    C++ Runtime
  ================                    ===========

  @ray.remote
  def f(x):
      return x + 1

  ref = f.remote(42)
        |
        v
  +------------------+
  | remote_function  |
  | .py / actor.py   |
  +------------------+
        |
        v
  +------------------+
  | _raylet.pyx      |  <-- Cython binding layer
  | (Python -> C++)  |
  +------------------+
        |
        v
  +------------------+    gRPC    +------------------+
  |   CoreWorker     | --------> |     Raylet        |
  | (task submit)    |           | (scheduling)      |
  +------------------+           +------------------+
                                        |
                          resource check + worker select
                                        |
                                        v
                                 +------------------+
                                 | Target Worker    |
                                 | CoreWorker       |
                                 | (task execute)   |
                                 +------------------+
                                        |
                                   store result
                                        |
                                        v
                                 +------------------+
                                 |  Plasma Store    |
                                 | (shared memory)  |
                                 +------------------+
                                        |
  ray.get(ref) <------------------------+
  result = 43        (retrieve via ObjectRef)
```

## 4. C++ Component Map

```
src/ray/
|
+-- common/                 Shared types, IDs, resource sets, configs
|   +-- scheduling/         Resource representations & label selectors
|   +-- task/               TaskSpec definitions
|   +-- asio/               Async I/O utilities (instrumented ASIO)
|   +-- cgroup2/            Linux cgroup v2 resource isolation
|
+-- gcs/                    Global Control Store (head node only)
|   +-- gcs_server/         GCS server: node, actor, job, placement group mgrs
|   +-- store_client/       Storage backends: Redis, in-memory
|   +-- pubsub/             State change pub/sub
|
+-- raylet/                 Node Manager (every node)
|   +-- scheduling/         Scheduling policies (spread, affinity, label-based)
|   +-- node_manager        Worker pool, task dispatch, resource tracking
|
+-- core_worker/            In-process runtime (every worker)
|   +-- task_submission/    Normal task + actor task submitters
|   +-- task_execution/     Execution queues (ordered, unordered, fiber)
|   +-- actor_management/   Actor lifecycle, handles, creator
|   +-- store_provider/     Plasma + memory store access
|   +-- reference_counter   Distributed reference counting
|
+-- object_manager/         Distributed object transfer
|   +-- plasma/             Plasma shared-memory object store
|   +-- pull_manager        Cross-node object pulling
|   +-- push_manager        Cross-node object pushing
|
+-- protobuf/               gRPC service & message definitions (.proto)
+-- rpc/                    gRPC server infrastructure & clients
+-- pubsub/                 Publish/subscribe messaging
+-- ray_syncer/             Cluster state synchronization
+-- stats/                  Runtime metrics collection
+-- observability/          Event recording
```

## 5. Module Dependency Graph

```
+-----------------------------------------------------------------------+
|                                                                       |
|                      User Application Code                            |
|                                                                       |
+---+----------+----------+---------+-----------+-----------+-----------+
    |          |          |         |           |           |
    v          v          v         v           v           v
+--------+ +--------+ +--------+ +--------+ +--------+ +--------+
|  Ray   | |  Ray   | |  Ray   | |  Ray   | | RLlib  | |  Ray   |
|  Data  | | Train  | |  Tune  | | Serve  | |        | |  DAG   |
+--------+ +--------+ +--------+ +--------+ +--------+ +--------+
    |          |  |        |  |       |          |  |        |
    |          |  |        |  |       |          |  |        |
    |     uses |  | tunes  |  | tunes |     is a |  |        |
    |     Data |  | Train  |  | RLlib |   Tune   |  |        |
    |      +---+  +--------+  +------+ trainable|  |        |
    |      |                                +----+  |        |
    |      v                                |       |        |
    |  +--------+                           |       |        |
    +->|Ray AIR |<--------------------------+-------+        |
    |  | Config |  (RunConfig, ScalingConfig, Result)         |
    |  +--------+                                             |
    |      |                                                  |
    v      v                                                  v
+-----------------------------------------------------------------------+
|                                                                       |
|                     Ray Core (Tasks, Actors, Objects)                  |
|                                                                       |
|   Python API  -->  _raylet.pyx (Cython)  -->  C++ CoreWorker          |
|                                                                       |
+-----------------------------------------------------------------------+
    |              |                |                |
    v              v                v                v
+----------+ +-----------+ +---------------+ +-------------+
|  Raylet  | |    GCS    | |    Plasma     | |   Object    |
| (sched)  | | (metadata)| | (obj store)   | |   Manager   |
+----------+ +-----------+ +---------------+ +-------------+
```

## 6. Communication Patterns

```
  +----------+         +----------+         +----------+
  | Worker A |         |  Raylet  |         | Worker B |
  +----------+         +----------+         +----------+
       |                    |                    |
       |--- SubmitTask ---->|                    |
       |                    |--- AssignTask ---->|
       |                    |                    |--- Execute
       |                    |                    |    Task
       |                    |                    |
       |                    |                    |--- Store result
       |                    |                    |    in Plasma
       |                    |                    |
       |--- ray.get(ref) ---|--- Pull Object ---|
       |                    |    (if remote)     |
       |<-- Object Data ----|                    |
       |                    |                    |


  +----------+         +----------+         +----------+
  |  Raylet  |         |   GCS    |         |  Raylet  |
  | (Node 1) |         | (Head)   |         | (Node 2) |
  +----------+         +----------+         +----------+
       |                    |                    |
       |--- RegisterNode -->|                    |
       |                    |<-- RegisterNode ---|
       |                    |                    |
       |--- Heartbeat ----->|<--- Heartbeat -----|
       |                    |                    |
       |--- CreateActor --->|                    |
       |                    |--- PlaceActor ---->|
       |                    |                    |
       |<-- ActorInfo ------|---- ActorInfo ---->|
       |   (pub/sub)        |    (pub/sub)       |
```

## 7. Data Flow Through AI Libraries

```
                    +----------------+
                    |  Raw Data      |
                    | (files, DBs,   |
                    |  streams)      |
                    +-------+--------+
                            |
                            v
                    +----------------+
                    |   Ray Data     |
                    |  - read_*()    |  read from sources
                    |  - map()       |  transform
                    |  - filter()    |  filter
                    +-------+--------+
                            |
              +-------------+-------------+
              |                           |
              v                           v
     +----------------+         +----------------+
     |   Ray Train    |         |   Ray Serve    |
     |  - fit model   |         |  - deploy      |
     |  - distributed |         |  - inference   |
     |    training    |         |  - autoscale   |
     +-------+--------+         +----------------+
              |                         ^
              v                         |
     +----------------+                 |
     |   Ray Tune     |     trained model
     |  - HPO search  |  ---------------+
     |  - schedulers  |
     |  - trials      |
     +----------------+
```
