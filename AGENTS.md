# AGENTS.md

This file provides guidance to AI coding assistants when working with code in this repository.

## Repository Overview

Ray is a unified framework for scaling AI and Python applications. It consists of **Ray Core** (Tasks, Actors, Objects primitives) and **Ray AI Libraries** (Data, Train, Tune, RLlib, Serve) built on top of Core.

## Build & Development

### Python-only development (no C++ compilation)

For changes to Tune, RLlib, Autoscaler, Serve, Data, Train, or most Python files:

```bash
# Install latest nightly wheel
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl

# Replace installed package directories with symlinks to your local code
python python/ray/setup-dev.py
```

### Full source build (includes C++)

```bash
# Install dependencies (Ubuntu)
sudo apt-get install -y build-essential curl clang-12 pkg-config psmisc unzip
ci/env/install-bazel.sh  # installs bazelisk; only Bazel 6.5.0 is supported

# Build dashboard (requires Node.js)
cd python/ray/dashboard/client && npm ci && npm run build && cd -

# Editable install
cd python && pip install -r requirements.txt && pip install -e . --verbose
```

### Build variants

```bash
bazel run -c fastbuild //:gen_ray_pkg  # Fast build (less optimization)
bazel run -c dbg //:gen_ray_pkg        # Debug build
bazel run -c opt //:gen_ray_pkg        # Optimized build
```

Key environment variables: `RAY_BUILD_CORE=1`, `RAY_INSTALL_JAVA=1`, `SKIP_BAZEL_BUILD=1`, `RAY_DEBUG_BUILD=debug|asan|tsan`, `BAZEL_ARGS`.

### Building wheels

```bash
./build-wheel.sh 3.12          # Build manylinux wheel
./build-wheel.sh 3.12 ./dist   # With custom output directory
```

## Testing

Default pytest timeout is 180 seconds (configured in `pytest.ini`).

```bash
# Run a single test file
pytest python/ray/tests/test_basic.py

# Run a single test
pytest python/ray/tests/test_basic.py::test_function_name

# Run tests via CI docker (how CI runs them)
bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests/... core
```

Test files are distributed throughout the codebase in `tests/` subdirectories alongside their modules (e.g., `python/ray/serve/tests/`, `python/ray/data/tests/`, `rllib/tests/`).

## Linting & Formatting

The project uses pre-commit hooks. Key tools:

- **Python**: ruff (import sorting + linting), black (formatting)
- **C/C++**: clang-format (Google style, 90 col), cpplint
- **Bazel**: buildifier
- **Shell**: shellcheck
- **JS/Dashboard**: ESLint, prettier
- **Cython**: cython-lint

```bash
# Run all lint checks
./ci/lint/lint.sh pre_commit

# Individual checks
./ci/lint/lint.sh clang_format
./ci/lint/lint.sh code_format
./ci/lint/lint.sh bazel_buildifier
./ci/lint/lint.sh pytest_format
./ci/lint/lint.sh copyright_format
```

### Import ordering convention

Ruff isort is configured with a custom section order (see `pyproject.toml`). Ray imports (`ray`, `ray_release`) are `local-folder`. `psutil` and `setproctitle` go in a special `afterray` section (they're bundled with Ray).

## Architecture

### Core runtime (C++ — `src/ray/`)

- **Raylet** (`src/ray/raylet/`) — node manager running on every node; manages local worker pool, schedules tasks, handles resource allocation
- **GCS** (`src/ray/gcs/`) — Global Control Store on the head node; manages cluster metadata (nodes, actors, jobs, placement groups, resources)
- **CoreWorker** (`src/ray/core_worker/`) — runs in every worker process; handles task execution, object reference counting, actor management
- **Object Manager** (`src/ray/object_manager/`) — distributed object store management with Plasma; handles object location tracking, pull/push between nodes
- **Protobuf definitions** (`src/ray/protobuf/`) — gRPC service and message definitions

### Python-C++ bridge

`python/ray/_raylet.pyx` is the main Cython binding layer (~200K lines). It exposes C++ CoreWorker, ID types, and config to Python. Header declarations are in `python/ray/includes/`.

### Python package (`python/ray/`)

- `remote_function.py`, `actor.py` — `@ray.remote` decorator implementations
- `_private/` — internal implementation (worker management, serialization, constants, GCS utils)
- `runtime_env/` — runtime environment management
- `dashboard/` — web dashboard (Python backend + React/TS frontend in `dashboard/client/`)
- `autoscaler/` — cluster autoscaling

### AI Libraries

| Library | Location | Purpose |
|---------|----------|---------|
| Ray Data | `python/ray/data/` | Scalable dataset processing |
| Ray Train | `python/ray/train/` | Distributed ML training |
| Ray Tune | `python/ray/tune/` | Hyperparameter tuning |
| Ray Serve | `python/ray/serve/` | Model serving (controller + proxy + replica actors) |
| RLlib | `rllib/` | Reinforcement learning |
| Ray AIR | `python/ray/air/` | Common config/result types shared across libraries |

### Java (`java/`)

Java API with JNI bindings to the C++ CoreWorker.

### Task execution flow

1. User calls `ray.remote()` → Python `remote_function.py` / `actor.py`
2. Cython binding (`_raylet.pyx`) → C++ CoreWorker
3. CoreWorker submits task to Raylet via gRPC
4. Raylet schedules to a worker based on resource requirements
5. Worker's CoreWorker executes the task
6. Results stored in object store, retrieved via `ray.get()` using ObjectRef

## CI

Primary CI is **Buildkite** (`.buildkite/*.rayci.yml`). Tests are conditionally run based on changed files (`ci/pipeline/determine_tests_to_run.py`). GitHub Actions (`.github/workflows/`) handle only lightweight checks like stale PR management.

## Documentation

Built with Sphinx from `doc/source/`. Build with `make -C doc/ html`.
