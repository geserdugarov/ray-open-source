# Ray Serve

**Location:** `python/ray/serve/`
**Purpose:** Scalable model serving and online inference.

## Overview

Ray Serve is a framework for deploying ML models and business logic as scalable microservices. It supports HTTP and gRPC ingress, automatic scaling based on request metrics, request batching, and model composition via deployment graphs.

## Architecture

```
                   HTTP/gRPC
                      |
                      v
              +---------------+
              |    Proxy      |     Ingress + load balancing
              | (HTTP/gRPC)   |     (runs on each node)
              +-------+-------+
                      |
                      v
              +---------------+
              |    Router     |     Request routing
              +-------+-------+
                      |
          +-----------+-----------+
          |           |           |
          v           v           v
     +--------+  +--------+  +--------+
     |Replica |  |Replica |  |Replica |     User code execution
     |  (1)   |  |  (2)   |  |  (N)   |     (Ray actors)
     +--------+  +--------+  +--------+

              +---------------+
              |  Controller   |     State management
              | (head node)   |     (deployment configs,
              +---------------+      autoscaling decisions)
```

## Key Abstractions

### Deployment (`deployment.py`)
A decorated class or function that handles inference requests. Created with `@serve.deployment`:

```python
@serve.deployment(num_replicas=3, ray_actor_options={"num_gpus": 1})
class MyModel:
    def __init__(self, model_path):
        self.model = load_model(model_path)

    def __call__(self, request):
        return self.model.predict(request.data)
```

### Application
A collection of deployments composed together. Created by binding deployments:
```python
app = MyModel.bind(model_path="/models/v1")
serve.run(app)
```

### DeploymentHandle (`handle.py`, 52 KB)
Client-side handle for calling deployments programmatically (sync or async). Supports both in-process and cross-process calls.

### Proxy
Ingress actor handling HTTP/gRPC requests. Runs on each node to minimize network hops. Performs load balancing across replicas.

### Controller (`_private/controller.py`, 80 KB)
Central actor on the head node managing:
- Deployment state and configuration
- Replica scaling (up/down based on autoscaling policy)
- Health checks
- Deployment updates and rollbacks

### Replica Actors
The actual workers running user code. Each replica is a Ray actor executing the deployment class/function.

### AutoscalingConfig (`config.py`)
Metrics-based autoscaling policy:
- `min_replicas`, `max_replicas` — replica bounds
- `target_ongoing_requests` — target request queue depth
- `upscale_delay_s`, `downscale_delay_s` — scaling cooldowns

### Task Processing (`task_processor.py`, `taskiq_task_processor.py`)
Alpha task-queue integration for Serve deployments. The built-in Celery adapter runs synchronous task handlers, manages retry behavior, and supports failed/unprocessable task queues.

### LLM Serving (`llm/`)
Beta helpers for OpenAI-compatible LLM Serve applications:
- `LLMConfig`, `LLMServingArgs`, model loading and LoRA configs
- `build_llm_deployment()` for one deployment
- `build_openai_app()` for OpenAI-compatible multi-model apps

## Key Features

- **Request Batching** (`batching.py`) — Batch multiple requests for efficient GPU utilization
- **Model Multiplexing** (`multiplex.py`) — One replica handles multiple model versions
- **Deployment Graphs** — Compose deployments via `.bind()` for multi-model pipelines
- **HTTP/gRPC Ingress** — Built-in HTTP server and gRPC support
- **Request Timeouts** — HTTP and gRPC options support request timeouts for bounded request handling
- **Direct Ingress Utilities** — Direct ingress helpers for bypassing proxy paths in specialized deployments
- **Autoscaling** — Scale replicas based on request queue metrics
- **Health Checks** — Automatic replica health monitoring and restart

## Key Files

| File | Purpose |
|------|---------|
| `api.py` | Public API: `deployment`, `run`, `delete` (42 KB) |
| `deployment.py` | Deployment and Application classes |
| `handle.py` | DeploymentHandle (52 KB) |
| `config.py` | HTTPOptions, AutoscalingConfig, gRPCOptions |
| `batching.py` | Request batching decorator |
| `multiplex.py` | Model multiplexing |
| `_private/controller.py` | Controller actor (80 KB) |
| `_private/proxy.py` | HTTP/gRPC proxy |
| `_private/replica.py` | Replica actor implementation |
| `_private/autoscaling_policy.py` | Autoscaling logic |
| `_private/deployment_scheduler.py` | Deployment scheduling |
| `_private/direct_ingress_http_util.py` | Direct HTTP ingress helpers |
| `task_processor.py` | Task processor API and Celery adapter |
| `llm/` | LLM deployment builders and OpenAI-compatible ingress |

## Relationships

- Deploys models trained with **Ray Train**
- Can use **Ray Data** for preprocessing in inference pipelines
- Built on **Ray Core** actors for replica and controller management
- Uses **Ray DAG** `.bind()` syntax for deployment composition
