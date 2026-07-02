# Ray Core

Ray Core is the distributed computing foundation. It consists of a C++ runtime, a Cython binding layer, and a Python API. It provides three primitives: **Tasks**, **Actors**, and **Objects**.

## C++ Runtime (`src/ray/`)

### Raylet — Node Manager (`src/ray/raylet/`)

The Raylet runs on every node and manages local task scheduling, the worker pool, and resource tracking.

**Key Components:**
- `node_manager.h/.cc` — Main orchestrator: receives task submissions, coordinates scheduling, manages the worker lifecycle
- `worker_pool.h` — Starts, stops, and pools worker processes; handles idle worker cleanup
- `local_object_manager.h` — Interfaces with the local Plasma store for object lifecycle
- `placement_group_resource_manager.h` — Manages resource bundles for placement groups
- `agent_manager.h` — Manages dashboard agent and runtime env agent processes
- `wait_manager.h` — Coordinates `ray.wait()` operations

**Scheduling Subsystem (`scheduling/`):**
- `cluster_resource_scheduler.h` — Makes global scheduling decisions (which node to place a task on)
- `local_resource_manager.h` — Tracks resource availability on the local node
- `cluster_lease_manager.h` — Lease-based resource allocation across the cluster
- Scheduling policies: `spread_scheduling_policy.h`, `affinity_with_bundle_scheduling_policy.h`, `node_label_scheduling_policy.h`
- Worker killing policies use shared monitor utilities from `src/ray/common/monitors/`, including time-based and owner-group strategies.

### GCS — Global Control Store (`src/ray/gcs/`)

Runs on the head node. Manages cluster-wide metadata and coordination.

**Key Components:**
- `gcs_server.h/.cc` — Main server process
- `gcs_node_manager.h` — Node membership, liveness tracking, heartbeats
- `gcs_actor_manager.h` — Actor lifecycle: creation, scheduling, reconstruction
- `gcs_actor_scheduler.h` — Actor placement decisions
- `gcs_job_manager.h` — Job tracking and cleanup
- `gcs_placement_group_manager.h` — Placement group orchestration
- `gcs_resource_manager.h` — Cluster resource capacity
- `gcs_health_check_manager.h` — Health monitoring and failure detection
- `gcs_kv_manager.h` — Key-value store interface

**Storage Backends (`store_client/`):**
- `redis_store_client.h` — Redis for persistent storage
- `in_memory_store_client.h` — In-memory alternative
- `observable_store_client.h` — Wrapper for state change notifications

### CoreWorker (`src/ray/core_worker/`)

Runs in every worker process. Handles task execution, object management, and actor lifecycle.

**Task Submission (`task_submission/`):**
- `normal_task_submitter.h` — Submit regular remote function calls
- `actor_task_submitter.h` — Submit actor method invocations
- `dependency_resolver.h` — Fetch task dependencies before execution

**Task Execution (`task_execution/`):**
- `task_receiver.h` — Accept incoming tasks from the Raylet
- `normal_task_execution_queue.h` — Queue and execute normal tasks
- `ordered_actor_task_execution_queue.h` — Sequential actor method execution
- `unordered_actor_task_execution_queue.h` — Concurrent actor method execution
- `concurrency_group_manager.h` — Manage actor concurrency groups
- `fiber.h` — Lightweight coroutine abstraction

**Actor Management (`actor_management/`):**
- `actor_manager.h` — Local actor lifecycle management
- `actor_creator.h` — Actor instantiation
- `actor_handle.h` — Remote actor references

**Object & Reference Management:**
- `reference_counter.h` — Distributed reference counting for object lifecycle
- `task_manager.h` — Track task states and lineage for reconstruction
- `object_recovery_manager.h` — Reconstruct lost objects via re-execution

**Core Files:**
- `core_worker.h/.cc` — Main class (~20K lines), the central runtime in each worker
- `core_worker_process.h` — Process initialization and shutdown
- `core_worker_options.h` — Configuration options

### Object Manager (`src/ray/object_manager/`)

Manages distributed object storage and transfer between nodes.

- `object_manager.h/.cc` — Coordinates pull/push of objects across nodes
- `object_directory.h` — Tracks which nodes hold which objects
- `pull_manager.h` — Strategies for pulling remote objects
- `push_manager.h` — Push objects to requesting nodes
- `object_buffer_pool.h` — Memory allocation for object transfers

**Plasma Store (`plasma/`):**
- `plasma.h` — Shared-memory object store server
- `store.h` — Core storage engine
- `client.h` — Client API for put/get/delete
- `allocator.h` — Memory allocator (mmap-based)
- `eviction_policy.h` — LRU eviction when memory is full
- `object_lifecycle_manager.h` — Object lifecycle management
- `shared_memory.h` — POSIX shared memory management

### Protobuf & RPC (`src/ray/protobuf/`, `src/ray/rpc/`)

Service and message definitions for all inter-process communication.

**Key Proto Files:**
- `gcs.proto` / `gcs_service.proto` — GCS service (100+ RPC methods)
- `node_manager.proto` — Raylet service interface
- `core_worker.proto` — CoreWorker service interface
- `object_manager.proto` — Object transfer service
- `common.proto` — Shared message types (TaskSpec, ObjectRef, etc.)
- `pubsub.proto` — Pub/sub messaging

### Common Infrastructure (`src/ray/common/`)

- `id.h` — Ray ID types (ObjectID, TaskID, ActorID, JobID, etc.)
- `ray_config.h` — Global configuration system (runtime flags)
- `ray_object.h` — Object representation (data + metadata)
- `status.h` — Error handling (Status/StatusCode)
- `scheduling/resource_set.h` — Resource type and quantity representation
- `task/task_spec.h` — Task definition (function, args, resources)
- `asio/` — Instrumented async I/O context
- `cgroup2/` — Linux cgroup v2 for resource isolation
- `monitors/` — CPU and memory monitors, monitor factories, and tests
- `filter_local_objects_util.h` — Helpers for filtering local object metadata

### Other C++ Components

- `pubsub/` — Publish/subscribe for state change notifications
- `ray_syncer/` — Lightweight cluster state synchronization
- `stats/` — Runtime metrics collection (Prometheus-compatible)
- `observability/` — Event recording for debugging/monitoring

## Python-C++ Bridge

### Cython Binding (`python/ray/_raylet.pyx`)

~216 KB file that bridges Python and C++. Exposes:
- CoreWorker methods (task submission, object get/put, actor creation)
- ID types (ObjectRef, ActorID, TaskID, etc.)
- GCS client for cluster state queries
- Configuration and metrics

### Cython Declarations (`python/ray/includes/`)

~324 KB of `.pxd`/`.pxi` files declaring C++ classes for Cython:
- `libcoreworker.pxd` — CoreWorker C++ class interface
- `common.pxd` — Common types
- `unique_ids.pxd` — ID type definitions
- `gcs_client.pxi` — GCS client bindings
- `object_ref.pxi` — ObjectRef Python wrapper
- `ray_config.pxd` — Configuration system

## Python API (`python/ray/`)

### Public API
- `remote_function.py` — `@ray.remote` decorator for functions (creates Tasks)
- `actor.py` — `@ray.remote` decorator for classes (creates Actors)
- `worker.py` — `ray.init()`, `ray.get()`, `ray.put()`, `ray.wait()`

### Shared Internal Utilities (`python/ray/_common/`)

Shared non-public utilities used by Ray Core and the Python libraries. Library code should put common internal APIs here instead of depending directly on `ray._private`.

- `utils.py`, `network_utils.py`, `retry.py` — Cross-library utility functions
- `ray_option_utils.py`, `ray_constants.py` — Shared option validation and constants
- `usage/usage_lib.py` — Usage telemetry support
- `observability/` — Internal and platform event helpers

### Internal Implementation (`python/ray/_private/`)

50+ modules handling the Python-side runtime:

**Worker & Node Management:**
- `worker.py` — Worker process initialization and lifecycle
- `node.py` — Node representation and communication
- `services.py` — Launch Raylet, GCS, dashboard, worker processes

**Serialization:**
- `serialization.py` — Object serialization/deserialization (CloudPickle-based)
- `arrow_serialization.py` — Apache Arrow support for tabular data

**State & GCS:**
- `gcs_utils.py` — GCS client utilities
- `state.py` — State API (list actors, tasks, nodes, objects)

**Monitoring:**
- `metrics_agent.py` — Metrics collection
- `log_monitor.py` — Log aggregation across workers
- `memory_monitor.py` — Memory usage tracking

**Constants & Configuration:**
- `ray_constants.py` — Global constants (ports, timeouts, paths)
- `parameter.py` — Configuration parameters
