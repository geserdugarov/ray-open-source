# Ray Train

**Location:** `python/ray/train/`
**Purpose:** Distributed ML training with support for multiple frameworks.

## Overview

Ray Train provides a unified API for distributed training across popular ML frameworks. It follows a Single Program Multiple Data (SPMD) paradigm — the same training function runs on multiple workers, each processing a shard of the data.

Training is configured via `ScalingConfig` (number of workers, resources) and `RunConfig` (experiment name, storage, checkpointing), both from Ray AIR.

## Key Abstractions

### BaseTrainer (`base_trainer.py`, 37 KB)
Abstract base class for all trainers. Defines the training lifecycle:
1. Setup worker group
2. Distribute data shards
3. Execute training function on each worker
4. Collect results and checkpoints

### DataParallelTrainer (`data_parallel_trainer.py`)
The primary trainer for data-parallel training. Distributes a training function across workers, each receiving a shard of the dataset. Used by all framework-specific trainers.

### TrainContext (`context.py`)
In-training context accessible via `ray.train.get_context()`. Provides:
- `get_world_size()` — total number of workers
- `get_world_rank()` — this worker's rank
- `get_local_rank()` — rank on the local node
- `get_dataset_shard()` — this worker's data shard

### Checkpoint (`_checkpoint.py`)
Distributed checkpoint abstraction. Supports saving/loading model state across workers. Can be backed by Ray object store references or cloud storage.

### Result
Training result with final metrics, best checkpoint, and experiment path. Returned by `trainer.fit()`.

## Framework Integrations

| Directory | Framework | Trainer Class |
|-----------|-----------|---------------|
| `torch/` | PyTorch | TorchTrainer |
| `lightning/` | PyTorch Lightning | LightningTrainer |
| `tensorflow/` | TensorFlow | TensorflowTrainer |
| `huggingface/` | Hugging Face Transformers | TransformersTrainer |
| `xgboost/` | XGBoost | XGBoostTrainer |
| `lightgbm/` | LightGBM | LightGBMTrainer |

Each integration provides:
- Framework-specific backend configuration (e.g., `TorchConfig` for PyTorch distributed)
- Utility functions for distributed setup (e.g., `prepare_model()`, `prepare_data_loader()`)
- Callback/reporting integration

### Additional Directories
- `v2/` — Newer training API iteration
- `collective/` — Collective communication primitives (allreduce, allgather)
- `_internal/` — Session management, backend executor, checkpoint handling

## Key Files

| File | Purpose |
|------|---------|
| `base_trainer.py` | BaseTrainer abstract class (37 KB) |
| `trainer.py` | Trainer utilities and registry |
| `data_parallel_trainer.py` | DataParallelTrainer implementation |
| `context.py` | TrainContext (in-training info) |
| `_checkpoint.py` | Checkpoint abstraction |
| `torch/` | PyTorch integration |
| `huggingface/` | Hugging Face integration |
| `xgboost/` | XGBoost integration |
| `lightgbm/` | LightGBM integration |
| `lightning/` | PyTorch Lightning integration |
| `tensorflow/` | TensorFlow integration |

## Relationships

- Uses **Ray Data** for distributed dataset loading and per-worker sharding
- Configured via **Ray AIR** (`RunConfig`, `ScalingConfig`, `CheckpointConfig`)
- Can be used as a **Ray Tune** trainable for hyperparameter optimization
- Trained models can be deployed with **Ray Serve**
- Built on **Ray Core** actors for distributed worker management
