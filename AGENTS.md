# AGENTS.md

This file provides guidance to coding agents working in this repository. `CLAUDE.md` is a symlink to this file for tools that expect that filename.

## Branch Context

This described branch is based on `upstream/releases/2.54.0` at `1ea4980a1d` (after the `ray-2.54.0` tag at `48bd1f8fa4`). The branch adds repository guidance and the high-level summaries under `plans/docs/`; keep those summaries synchronized when rebasing or replaying across release-branch changes.

## Repository Overview

Ray is a unified framework for scaling AI and Python applications. It consists of **Ray Core** (Tasks, Actors, Objects primitives) and **Ray AI Libraries** (Data, Train, Tune, RLlib, Serve) built on top of Core.

Important top-level paths:

- `src/ray/` - C++ core runtime
- `python/ray/` - Python API and AI libraries
- `python/ray/_common/` - Shared non-public Python utilities for Ray libraries
- `python/ray/_private/` - Core Python runtime internals
- `rllib/` - RLlib reinforcement learning library
- `doc/source/` - Sphinx documentation source
- `.buildkite/` - Primary CI pipeline definitions
- `plans/docs/` - High-level repo/module summaries for this described branch

## Build and Development

### Python-only development

For changes to Tune, RLlib, Autoscaler, Serve, Data, Train, or most Python files:

```bash
pip install -U "ray==2.54.0"
python python/ray/setup-dev.py
```

### Full source build

```bash
sudo apt-get install -y build-essential curl clang-12 pkg-config psmisc unzip
ci/env/install-bazel.sh  # installs Bazelisk; Bazel 6.5.0 is supported

cd python/ray/dashboard/client
npm ci
npm run build
cd -

cd python
pip install -r requirements.txt
pip install -e . --verbose
```

Key environment variables: `RAY_BUILD_CORE=1`, `RAY_INSTALL_JAVA=1`, `RAY_INSTALL_CPP=1`, `SKIP_BAZEL_BUILD=1`, `RAY_DEBUG_BUILD=debug|asan|tsan`, `BAZEL_ARGS`.

### Build variants

```bash
bazel run -c fastbuild //:gen_ray_pkg
bazel run -c dbg //:gen_ray_pkg
bazel run -c opt //:gen_ray_pkg
```

### Wheels and images

```bash
./build-wheel.sh 3.12
./build-wheel.sh 3.12 ./dist

./build-image.sh ray
./build-image.sh ray -p 3.12
./build-image.sh ray --platform cu12.8.1-cudnn
```

## Testing

Default pytest timeout is 180 seconds (`pytest.ini`).

```bash
pytest python/ray/tests/test_basic.py
pytest python/ray/tests/test_basic.py::test_function_name
bazel run //ci/ray_ci:test_in_docker -- //python/ray/tests/... core
```

Test files are distributed through module-local `tests/` directories, such as `python/ray/serve/tests/`, `python/ray/data/tests/`, and `rllib/tests/`.

## Linting and Formatting

The project uses pre-commit hooks. Key tools:

- Python: ruff, black
- C/C++: clang-format, cpplint
- Bazel: buildifier
- Shell: shellcheck
- JS/Dashboard: ESLint, prettier
- Cython: cython-lint

```bash
./ci/lint/lint.sh pre_commit
./ci/lint/lint.sh clang_format
./ci/lint/lint.sh code_format
./ci/lint/lint.sh bazel_buildifier
```

Ruff isort has Ray-specific sections in `pyproject.toml`; `ray` and `ray_release` are treated as local imports, and bundled packages such as `psutil` and `setproctitle` use the configured special section.

## Documentation

Docs are built with Sphinx from `doc/source/`.

```bash
make -C doc html
make -C doc rtd-build
```

Use MyST Markdown (`.md`) for new files under `doc/source/`. Edits to existing `.rst` files are still supported. Read the Docs uses `.readthedocs.yaml`; PR previews skip non-doc-affecting changes and use an incremental cache with a clean fallback.

Documentation-specific contribution guidance lives in `doc/source/ray-contribute/docs.md` and `doc/README.md`.

## Architecture Notes

Core runtime components:

- Raylet (`src/ray/raylet/`) - node manager, worker pool, local scheduling, resource allocation
- GCS (`src/ray/gcs/`) - head-node metadata and coordination service
- CoreWorker (`src/ray/core_worker/`) - task execution, object references, actor management
- Object Manager (`src/ray/object_manager/`) - distributed object transfer and Plasma integration
- Common monitors (`src/ray/common/monitors/`) - CPU and memory monitor implementations shared by runtime components

Python bridge:

- `python/ray/_raylet.pyx` is the main Cython binding layer.
- `python/ray/includes/` contains Cython declarations for C++ APIs.
- Shared internal library APIs belong in `python/ray/_common/`; avoid adding cross-library dependencies on `ray._private`.

AI libraries:

- Ray Data: `python/ray/data/`
- Ray Train: `python/ray/train/`
- Ray Tune: `python/ray/tune/`
- Ray Serve: `python/ray/serve/`
- RLlib: `rllib/`
- Ray AIR common configs: `python/ray/air/`

## CI

Primary CI is Buildkite (`.buildkite/*.rayci.yml`). `.buildkite/test.rules.txt` maps changed files to tag sets that drive CI suite selection. GitHub Actions cover lightweight repository checks.

Every commit on a `ray-project/ray` PR needs a `Signed-off-by:` trailer for DCO. Use `git commit --signoff` when creating commits.

## Agent Workflow

- Prefer source-of-truth docs such as `doc/source/ray-contribute/development.rst` over stale command snippets.
- For C++ or Cython changes, rebuild using the narrowest command that covers the affected component.
- For C++ code that reads time, use an injected `ray::ClockInterface` unless the file is the clock implementation or a benchmark.
- Keep docs-only changes scoped to docs paths where possible; non-doc code changes can trigger broader CI suites.
