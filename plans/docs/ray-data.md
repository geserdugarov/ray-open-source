# Ray Data

**Location:** `python/ray/data/`
**Purpose:** Scalable, distributed dataset processing for ML workloads.

## Overview

Ray Data provides a lazy-evaluation Dataset API for reading, transforming, and writing data at scale. It is the standard way to load and preprocess data for Ray Train, Ray Tune, and Ray Serve.

Data is partitioned into **Blocks** (PyArrow Tables or Pandas DataFrames) that are distributed across the cluster. Operations are executed lazily — the computation graph is built up and only materialized when results are needed.

## Key Abstractions

### Dataset (`dataset.py`, 340 KB)
The core API class. Supports:
- **Transformations:** `map()`, `flat_map()`, `filter()`, `map_batches()`
- **Aggregations:** `groupby()`, `aggregate()`, `count()`, `sum()`, `mean()`, `min()`, `max()`
- **Joins:** `union()`, `zip()`
- **Shuffling:** `random_shuffle()`, `repartition()`
- **Conversion:** `to_pandas()`, `to_arrow()`, `to_torch()`, `to_tf()`
- **I/O:** `write_parquet()`, `write_csv()`, `write_json()`

### Block (`block.py`)
A distributed unit of data — a single partition of a Dataset. Each block is either a PyArrow Table or a Pandas DataFrame. Blocks are processed in parallel across Ray workers.

### BlockAccessor
Uniform interface for accessing block data regardless of underlying type (Arrow or Pandas).

### DataIterator (`iterator.py`)
Streaming iteration interface for memory-efficient consumption of datasets. Supports batched iteration compatible with PyTorch DataLoaders and TensorFlow datasets.

### Datasource / Datasink (`datasource/`, `_internal/datasource/`)
Pluggable I/O abstractions. Built-in readers and writers include:
- `parquet_datasource.py` — Apache Parquet
- `csv_datasource.py` — CSV files
- `json_datasource.py` — JSON/JSONL
- `sql_datasource.py` — SQL databases
- `kafka_datasource.py` — Apache Kafka streams
- `image_datasource.py` — Image files
- Additional sources and sinks for BigQuery, ClickHouse, Delta Sharing, Hudi, Iceberg, Lance, MongoDB, TFRecords, WebDataset, Zarr, audio, video, and more

### Preprocessor (`preprocessors/`)
Fit/transform abstraction for feature engineering (~16 preprocessors):
- Encoding (OneHotEncoder, LabelEncoder, OrdinalEncoder)
- Scaling (StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler)
- Imputation (SimpleImputer)
- Tokenization, hashing, etc.

### GroupedData (`grouped_data.py`)
Deferred grouping with aggregation — returned by `Dataset.groupby()`.

### Read API (`read_api.py`, 224 KB)
Top-level functions for reading data:
- `ray.data.read_parquet()`, `read_csv()`, `read_json()`
- `ray.data.read_images()`, `read_text()`
- `ray.data.from_pandas()`, `from_arrow()`, `from_numpy()`
- `ray.data.read_sql()`, `read_kafka()`

### LLM Batch Processing (`llm.py`)
Public beta API for building Ray Data processors around HTTP endpoints and LLM engines:
- `HttpRequestProcessorConfig`
- `vLLMEngineProcessorConfig`
- `SGLangEngineProcessorConfig`
- `ServeDeploymentProcessorConfig`
- `build_processor()`

The Serve deployment processor includes request timeout support to avoid indefinite hangs.

### Usage Collection (`_internal/usage/`)
Execution callbacks and collectors record Ray Data operator usage and cluster-level signals, including dead-node counts for workload telemetry.

## Key Files

| File | Purpose |
|------|---------|
| `dataset.py` | Core Dataset class (340 KB) |
| `read_api.py` | Read functions (224 KB) |
| `block.py` | Block abstraction |
| `iterator.py` | DataIterator interface |
| `grouped_data.py` | GroupedData for aggregations |
| `aggregate.py` | Aggregation function definitions |
| `context.py` | DataContext configuration |
| `expressions.py` | Lazy evaluation expressions |
| `datasource/` | I/O source/sink implementations |
| `_internal/usage/` | Data usage collection and execution callbacks |
| `preprocessors/` | Feature engineering transforms |
| `_internal/` | Compute strategies, execution engine, block builders |
| `llm.py` | LLM-specific data utilities |

## Relationships

- **Ray Train** uses Ray Data for distributed dataset loading and splitting across training workers
- **Ray Tune** passes datasets to trial functions
- **Ray Serve** uses Ray Data for preprocessing in inference pipelines
- Built on **Ray Core** tasks for parallel block processing
