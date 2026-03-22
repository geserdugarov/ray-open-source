# Supporting Modules

## Ray AIR — Common Configuration

**Location:** `python/ray/air/`

Shared abstractions used across Ray Train, Tune, and Serve.

**Key Classes (`config.py`, 28 KB):**

| Class | Purpose |
|-------|---------|
| `RunConfig` | Job-level config: experiment name, storage path, checkpoint config, failure config, callbacks |
| `ScalingConfig` | Resource config: `num_workers`, `resources_per_worker`, `use_gpu`, `placement_strategy` |
| `CheckpointConfig` | Checkpoint behavior: save frequency, keep top N, upload strategy |
| `FailureConfig` | Failure handling: max retries, retry delay |
| `Result` | Unified result: final metrics, best checkpoint, experiment path |
| `DataBatchType` | Data format enum (numpy, pandas, arrow) |

**Internal (`_internal/`):** Execution resource management, telemetry, framework integrations.

---

## Ray DAG — Task Graph Compilation

**Location:** `python/ray/dag/`

Provides a lazy task graph abstraction. Users compose computation with `.bind()` and the graph can be compiled for efficient execution.

**Key Classes:**
- `DAGNode` (`dag_node.py`, 28 KB) — Abstract base for all nodes
- `FunctionNode` (`function_node.py`) — Wraps a remote function call
- `ClassNode` (`class_node.py`) — Wraps actor creation
- `ClassMethodNode` — Wraps actor method calls
- `InputNode` (`input_node.py`) — DAG input placeholder
- `CompiledDAG` (`compiled_dag_node.py`, 140 KB) — Compiled executable graph
- `CollectiveOutputNode` (`collective_node.py`) — Collective communication ops

**Usage:** Chain `.bind()` calls to build a DAG, then compile and execute:
```python
a = func1.bind(input)
b = func2.bind(a)
dag = b.compile()
result = dag.execute(data)
```

**Used by:** Ray Serve deployment composition, advanced distributed patterns.

---

## Dashboard — Cluster Monitoring UI

**Location:** `python/ray/dashboard/`

Web-based UI for monitoring and managing Ray clusters.

### Backend (Python)
- `dashboard.py` — Main dashboard server
- `head.py` — Dashboard head node component
- `agent.py` — Dashboard agent running on each node
- `http_server_head.py` / `http_server_agent.py` — REST API servers
- `state_aggregator.py` — Aggregates metrics from all nodes

**Modules (`modules/`):**
| Module | Purpose |
|--------|---------|
| `job/` | Job submission and management |
| `node/` | Node status and metrics |
| `actor/` | Actor inspection |
| `metrics/` | Prometheus metrics integration |
| `event/` | Event log viewer |
| `serve/` | Ray Serve deployment status |
| `train/` | Ray Train job monitoring |
| `data/` | Ray Data execution monitoring |

### Frontend (`client/`)
React/TypeScript single-page application. Built with `npm ci && npm run build` during installation.

---

## Autoscaler — Cluster Scaling

**Location:** `python/ray/autoscaler/`

Automatically adjusts cluster size based on resource demand.

**Core:**
- `node_provider.py` — Abstract provider interface (start/stop nodes)
- `command_runner.py` — SSH command execution on provisioned nodes
- `batching_node_provider.py` — Batch node operations for efficiency
- `tags.py` — Node labeling/tagging system

**Cloud Providers (`_private/`):**

| Provider | Directory |
|----------|-----------|
| AWS | `aws/` (with CloudWatch) |
| GCP | `gcp/` |
| Azure | `azure/` |
| Kubernetes | `kuberay/` |
| vSphere | `vsphere/` |
| Aliyun | `aliyun/` |
| Local | `local/` |
| Spark | `spark/` |
| Read-only | `readonly/` |

**V2 Autoscaler (`v2/`):**
- `instance_manager/` — Instance lifecycle management
- Redesigned for improved scalability and responsiveness

---

## Runtime Environment

**Location:** `python/ray/runtime_env/`

Manages per-task/actor execution environments.

**Capabilities:**
- Python packages (pip dependencies, wheels)
- Conda environments
- Working directory setup
- Environment variables
- Custom executable paths

**Key Files:**
- `runtime_env.py` — Main API and configuration
- `schemas/` — JSON validation schemas
- `types/` — Type definitions for environment specs

**Usage:**
```python
@ray.remote(runtime_env={"pip": ["torch==2.0"], "env_vars": {"DEBUG": "1"}})
def train():
    ...
```

---

## Java API

**Location:** `java/`

Java bindings to Ray via JNI (Java Native Interface).

**Structure:**
| Directory | Purpose |
|-----------|---------|
| `api/` | Public Java API definitions |
| `runtime/` | Runtime implementation with JNI bridge |
| `serve/` | Ray Serve Java library |
| `test/` | Integration tests |

**Runtime (`runtime/src/main/java/io/ray/runtime/`):**
- `RayNativeRuntime.java` — JNI wrapper around C++ CoreWorker
- `actor/` — Actor implementation
- `task/` — Task submission and execution
- `object/` — Object store interface
- `gcs/` — GCS client
- `serializer/` — Java object serialization
- `config/` — Configuration
- `placementgroup/` — Placement group support

---

## Experimental Features

**Location:** `python/ray/experimental/`

Preview features and advanced capabilities:

| Module | Purpose |
|--------|---------|
| `channel/` | Async message passing channels |
| `collective/` | Collective communication primitives |
| `state/` | Distributed state management |
| `shuffle.py` | Data shuffling utilities |
| `compiled_dag_ref.py` | Compiled DAG references |
| `dynamic_resources.py` | Dynamic resource management |
| `tqdm_ray.py` | Ray-aware progress bars |
| `internal_kv.py` | Internal key-value store |
| `locations.py` | Node/actor location tracking |
| `rdt/` | Ray Distributed Tracing |
| `raysort/` | Distributed sorting |
| `multiprocessing/` | Python multiprocessing emulation on Ray |

---

## Workflow (DEPRECATED)

**Location:** `python/ray/workflow/`

Deprecated as of Ray 2.44 and removed. Users should use Ray DAG for workflow/pipeline patterns.
