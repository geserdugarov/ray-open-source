# Ray Project Overview

## What is Ray?

Ray is a unified framework for scaling AI and Python applications. It provides a simple, universal API for building distributed applications, enabling developers to seamlessly scale code from a laptop to a cluster.

**Version:** 2.54.0 (release branch)
**Language:** Python (primary), C++ (core runtime), Java (API bindings)
**Python Requirement:** >= 3.10
**Build System:** Bazel 6.5.0
**License:** Apache 2.0
**Branch Snapshot:** This described branch is based on `upstream/releases/2.54.0` at `1ea4980a1d`, after the `ray-2.54.0` tag at `48bd1f8fa4`.

## Two-Layer Architecture

Ray is organized into two main layers:

### Ray Core
The distributed computing foundation providing three primitives:
- **Tasks** — stateless function invocations scheduled across the cluster
- **Actors** — stateful class instances running on remote workers
- **Objects** — immutable values stored in a distributed object store, referenced via `ObjectRef`

### Ray AI Libraries
Higher-level libraries built on Ray Core for common ML workloads:

| Library   | Purpose                          | Location              |
|-----------|----------------------------------|-----------------------|
| Ray Data  | Scalable dataset processing      | `python/ray/data/`    |
| Ray Train | Distributed ML training          | `python/ray/train/`   |
| Ray Tune  | Hyperparameter tuning            | `python/ray/tune/`    |
| Ray Serve | Online model serving             | `python/ray/serve/`   |
| RLlib     | Reinforcement learning           | `rllib/`              |

## Repository Structure

```
ray-open-source/
├── src/ray/           C++ core runtime (Raylet, GCS, CoreWorker, Object Manager)
├── python/ray/        Python package (core API, AI libraries, dashboard, autoscaler)
├── java/              Java API with JNI bindings
├── rllib/             RLlib reinforcement learning library
├── cpp/               C++ API and utilities
├── doc/               Sphinx documentation source
├── ci/                CI scripts and configuration
├── .buildkite/        Buildkite CI pipelines (primary CI)
├── AGENTS.md          Top-level guidance for coding agents
├── docker/            Docker build files
├── release/           Release management scripts
├── bazel/             Bazel build configuration
└── thirdparty/        Third-party dependencies
```

## Core Concepts

### Task Execution Flow
1. User decorates a function with `@ray.remote` and calls `.remote()`
2. The Python layer submits the task through Cython bindings to C++ CoreWorker
3. CoreWorker sends the task to the local Raylet via gRPC
4. Raylet schedules the task on a suitable worker based on resource requirements
5. The target worker's CoreWorker executes the task
6. Results are stored in the distributed object store (Plasma)
7. The caller retrieves results via `ray.get()` using the ObjectRef

### Resource Model
Ray tracks resources (CPU, GPU, memory, custom) per node. Tasks and actors declare resource requirements; the scheduler places them on nodes with available resources.

### Fault Tolerance
- Tasks can be retried on failure
- Actors support checkpointing and reconstruction
- Objects can be reconstructed by re-executing their creating task
- GCS provides cluster-level failure detection

## Development Workflows

### Python-only Development
For changes to AI libraries, autoscaler, dashboard, or most Python files:
```bash
pip install -U "ray==2.54.0"
python python/ray/setup-dev.py   # symlinks local code into installed package
```

### Full Source Build (C++)
```bash
sudo apt-get install -y build-essential curl clang-12 pkg-config psmisc unzip
ci/env/install-bazel.sh   # installs Bazelisk; Bazel 6.5.0 is supported
pip install -e python/
```

### Testing
```bash
pytest python/ray/tests/test_basic.py              # single file
pytest python/ray/tests/test_basic.py::test_name    # single test
```
Default timeout: 180 seconds.

### Linting
- Python: ruff (imports + linting), black (formatting)
- C/C++: clang-format (Google style, 90 columns), cpplint
- Bazel: buildifier
- Shell: shellcheck

## CI/CD

Primary CI is **Buildkite** (`.buildkite/*.rayci.yml`). Tests are conditionally run based on changed files (`.buildkite/test.rules.txt` and CI helpers). GitHub Actions handle lightweight checks only. Read the Docs builds are configured in `.readthedocs.yaml`; PR previews skip non-doc-affecting changes and use an incremental doc cache with a clean fallback.

## Documentation

Built with Sphinx from `doc/source/`. New documentation pages should be MyST Markdown (`.md`); edits to existing `.rst` files are still supported. Local build: `make -C doc html`. Read-the-Docs-faithful verification: `make -C doc rtd-build`.
