# Ray and Lance 12 integration

## 1. Executive summary

Ray and Lance integrate at two distinct levels:

1. **Built-in Ray Data connector** — `ray.data.read_lance` distributes
   fragment scans. `Dataset.write_lance` is designed to distribute fragment
   creation followed by one driver-side Lance commit, but its pylance-4-era
   calls require the Lance 12 port in section 5.1.
2. **Application-level Ray Core orchestration** — Ray tasks or actors can call
   Lance 12's native index-segment APIs to distribute index creation. Ray does
   not currently wrap this protocol in a Ray Data index operator.

The ownership boundary is:

```text
 Ray owns                                 Lance owns
 ───────────────────────────────────      ────────────────────────────────
 task/actor placement                     dataset schema and fragments
 CPU/GPU/custom resources                 immutable files and manifests
 object transport and backpressure        snapshot/version semantics
 task retries and cluster scaling         scalar/vector/FTS index semantics
 external build orchestration             segment validation and merge
                                           atomic dataset commits
```

For Lance 12, the built-in read connector is the batch-extract path to validate;
the built-in write connector first needs the API adaptation described below.
Use native Lance in a long-lived actor/Serve replica for online queries, and use
explicit Ray Core tasks plus a durable external job record for distributed
index construction.

## 2. Compatibility baseline

This analysis combines:

- Ray `2.58.0` source at `01b49e4fa77b30bc28c69ed702dfe523fd6421d8`;
- Lance `12.0.0` source/docs at `cbeec97cb`; and
- the public Python APIs visible in those trees.

This is not an assertion that Ray 2.58's existing dependency lock was tested
against Lance 12. The checked-in compiled requirements still show
`pylance==4.0.0` and `lance-namespace==0.6.1`, while Lance 12's
`python/pyproject.toml` requires `lance-namespace>=0.11.1,<0.12`. The current
Ray development lock therefore has a concrete dependency conflict with the
target.

There is also a deterministic Python API mismatch in the inspected sources:

```text
 Ray 2.58 Lance sink                      Lance 12.0.0 public API
 ───────────────────────────────────      ─────────────────────────────────
 storage_options_provider=provider   ->   namespace_client=client,
                                            table_id=table_id

 from lance import                         provider is constructed internally;
   LanceNamespaceStorageOptionsProvider    no matching public Python export
```

Ray supplies `storage_options_provider=` to `fragment.write_fragments`, the
`LanceDataset` constructor, and `LanceDataset.commit`. Lance 12 accepts the
right-hand arguments instead. The sink passes the obsolete keyword even for a
plain URI, with value `None`, so every unmodified `write_lance` mode reaches a
`TypeError` (append can fail earlier while opening the dataset). Treat the
write path in this chapter as the connector architecture plus a required port,
not as a drop-in-compatible pair. Resolve the dependency lock and this API
mismatch in a clean virtual environment, then run the matrix in section 11.

### API map

| Workflow | Ray surface | Lance 12 surface called | Status in this Ray tree |
|---|---|---|---|
| Batch read | `ray.data.read_lance` | `lance.dataset`, `get_fragments`, `scanner`, `to_reader` | Signatures align; runtime matrix still required |
| Batch write | `Dataset.write_lance` | `fragment.write_fragments`, `LanceOperation`, `LanceDataset.commit` | Architecture built in; old provider keyword blocks Lance 12 unmodified |
| Namespace-backed write | `write_lance(..., namespace_impl, table_id, ...)` | Ray expects public provider; Lance 12 expects client + table ID | API port required; Ray surface is create-only |
| Scalar-filter acceleration | Ray expression pushdown | Lance scanner/filter/index planner | Indirect, native Lance decision |
| ANN query | `scanner_options={"nearest": ...}` passthrough | Lance scanner nearest path | No Ray global top-k wrapper |
| Central index build | No Ray Data method | `LanceDataset.create_index` | Call Lance directly |
| Distributed index build | No Ray Data method | `create_index_uncommitted` | Orchestrate with Ray Core |
| Segment merge | No Ray Data method | `merge_existing_index_segments` | Orchestrate with Ray Core |
| Segment publication | No Ray Data method | `commit_existing_index_segments` | Single coordinator call |
| Index optimization/prewarm | No Ray Data method | Lance optimize/prewarm APIs | Call Lance directly/in actor |

## 3. Data-model mapping

Ray and Lance share Arrow as their most efficient interchange model, but their
partition units differ.

```text
 Lance manifest version V
      |
      +─ fragment 10 ── data/a.lance ─┐
      +─ fragment 11 ── data/b.lance ─┼─ durable storage
      +─ fragment 12 ── data/c.lance ─┘
                     |
             Ray read-task grouping
                     |
           ┌─────────┴──────────┐
           v                    v
     Ray read task A      Ray read task B
     frags [10,11]        frag [12]
           |                    |
       Arrow batches        Arrow batches
           |                    |
           v                    v
     Ray blocks in distributed object stores
```

On write the mapping runs in the opposite direction, but is still not 1:1:

```text
 Ray blocks --bundle--> one Ray write task
                           |
                           +─ Lance fragment/file 1
                           +─ Lance fragment/file 2 (if file limit reached)
```

Terminology must stay precise:

- Ray block: ephemeral execution partition.
- Lance fragment: versioned durable row group.
- Lance index segment: durable index payload covering a fragment bitmap.
- Ray object-store spill: runtime memory management, not a Lance commit.

## 4. Batch reads

### 4.1 Recommended basic use

```python
import ray
from ray.data.expressions import col

rows = (
    ray.data.read_lance(
        "s3://bucket/catalog/items.lance",
        version=42,
        columns=["id", "tenant", "embedding", "text"],
        storage_options={"region": "us-east-1"},
        scanner_options={"batch_size": 8192},
        override_num_blocks=64,
    )
    .filter(expr=(col("tenant") == 7))
)

for batch in rows.iter_batches(batch_size=1024, batch_format="pyarrow"):
    consume(batch)
```

Pin a numeric version for a reproducible job. A string tag is also accepted,
but an application should define whether the tag is expected to move during a
job. Use `columns=` for physical projection in this Ray implementation.

### 4.2 Exact path

```text
 driver
  read_lance
    -> LanceDatasource
    -> lance.dataset(uri, version, storage_options)
    -> enumerate Lance fragments
    -> split fragments into Ray ReadTasks

 workers
  fragment IDs
    -> reconstruct LanceFragment handles
    -> lance_ds.scanner(fragments=..., columns/filter/options)
    -> Arrow RecordBatch stream
    -> security check for pickled Python-object extension columns
    -> Ray Data Arrow blocks
```

The fragment splitter is count-based. If fragment sizes are skewed, compact or
rebalance the Lance table, choose a more suitable number of Ray blocks, or
implement a size-aware datasource variant.

### 4.3 Pushdown

Supported Ray expression filters can be moved into the datasource. The worker
combines them with the deprecated `read_lance(filter="...")` argument and lets
Lance plan the scan. Lance can then use its native scalar indexes and pruning.

Use `.filter(expr=...)`, not a Python row function, for this path. A Python
callable is a Ray post-read UDF and cannot be turned into a native Lance
predicate.

### 4.4 Snapshot and refresh behavior

`LanceDatasource` opens its Lance dataset handle when the Ray Dataset is
created. Treat that as the read snapshot. Create a new Ray Dataset when the
application wants to see a newer Lance version rather than relying on an old
logical plan to refresh.

### 4.5 Security behavior

Ray rejects Lance columns containing Ray's pickled-Python-object Arrow
extension by default. This prevents a malicious dataset from executing code
during deserialization. Only set
`RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1` on both driver and workers when the
entire dataset is trusted.

### 4.6 Local paths in a cluster

A local URI is interpreted on the process that opens it. In a multi-node Ray
cluster, use shared/mounted paths with identical semantics on every node or a
cloud/object-store URI. A path that only exists on the driver is not a
distributed data source.

## 5. Batch writes

### 5.1 Compatibility gate

Do not run this Ray tree's `Dataset.write_lance` unchanged with Lance 12. The
minimum connector port is:

| Ray call site | Required Lance 12 adaptation |
|---|---|
| Worker `fragment.write_fragments` | Omit `storage_options_provider`; for namespace use, pass the cached `namespace_client` and `table_id` |
| `_open_dataset` | Omit the old keyword; when needed, construct `LanceDataset` with `namespace_client` and `table_id` |
| Driver `LanceDataset.commit` | Omit the old keyword; when needed, pass `namespace_client` and `table_id` |
| `create_storage_options_provider` | Remove the import of the old public Python provider; Lance 12 creates its Rust provider internally |
| Dependency set | Resolve `pylance==12.0.0` with `lance-namespace>=0.11.1,<0.12` and a compatible Arrow version |

For a plain URI, the first three rows mean omitting all namespace arguments,
not passing an explicit `None` under the obsolete name. For a namespace table,
also decide and test whether the namespace owns manifest versioning; Lance 12
exposes `namespace_client_managed_versioning`, while this Ray connector was
written around URI resolution plus storage-option refresh.

### 5.2 Intended use after the Lance 12 port

```python
import pyarrow as pa
import ray
from ray.data.datasource import SaveMode

dimension = 768
schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("text", pa.string()),
        pa.field("embedding", pa.list_(pa.float32(), dimension)),
    ]
)

dataset = make_ray_dataset()
dataset.write_lance(
    "s3://bucket/catalog/items.lance",
    schema=schema,
    mode=SaveMode.CREATE,
    min_rows_per_file=1_000_000,
    max_rows_per_file=64_000_000,
    data_storage_version="2.2",
    storage_options={"region": "us-east-1"},
    concurrency=32,
)
```

This example assumes the compatibility gate above has been implemented and
tested. It is not runnable against the two unmodified source baselines.

An exact `2.2` pin makes the Lance 12 on-disk choice explicit. `stable` also
resolves to 2.2 in Lance 12, but the alias can resolve differently in a future
release. Do not use `next` for a compatibility-sensitive dataset.

### 5.3 Modes

| Mode | Ray start behavior | Final Lance operation | Concurrency note |
|---|---|---|---|
| `CREATE` | Rejects a target that can already be opened | `Overwrite(schema, fragments)` | No-clobber check is a preflight; verify behavior under concurrent creators |
| `APPEND` | Opens dataset, captures version/schema | `Append(fragments)` with `read_version` | Optimistic version validation is used |
| `OVERWRITE` | Writes replacement fragments | `Overwrite(schema, fragments)` | New snapshot replaces visible fragment set |

Every mode distributes data-file creation. Publication remains one driver-side
manifest commit.

### 5.4 Schema rules

Provide an explicit Arrow schema for production writes, especially for vector
columns. Lance represents a dense embedding as a fixed-size list such as
`FixedSizeList<Float32, 768>`. The sink reorders each block's columns to the
schema before constructing a `RecordBatchReader`.

An explicit schema avoids worker-local inference disagreements and accidental
list/vector type changes. For append, the existing Lance schema is adopted when
the caller omits one.

### 5.5 File sizing and memory

- `min_rows_per_file` is a Ray bundling target (`min_rows_per_write`). It
  controls how much input a task receives, not a global Lance file compactor.
- `max_rows_per_file` is passed to Lance's fragment writer.
- A task can write more than one fragment/file.
- Default write retries make the task's input stream replayable by first
  materializing all bundled blocks in a Python list. Size heap and object-store
  memory for that behavior.
- Lance index construction has separate memory and temporary-disk needs; the
  write API does not build an index.

### 5.6 Failure visibility

Fragment files are durable before the manifest commit. A failed task or lost
driver can leave unreferenced objects. They are invisible to normal readers but
consume storage. Use Lance cleanup only after its retention window proves that
no active/recovering job needs those objects.

## 6. Namespace-backed writes

The following is the Ray-facing configuration shape, but it is gated on the
Lance 12 port in section 5.1.

```python
from ray.data.datasource import SaveMode

dataset.write_lance(
    "ignored-when-namespace-resolves-location",
    mode=SaveMode.CREATE,
    table_id=["analytics", "items"],
    namespace_impl="rest",
    namespace_properties={"uri": "https://catalog.example"},
)
```

The current sink connects through `lance_namespace`, declares the table
(falling back to `create_empty_table` for older implementations), obtains a
location and storage options, and attempts to construct a public
`LanceNamespaceStorageOptionsProvider` on workers and the coordinator.
Namespace-returned options override conflicting caller options. Namespace
clients are cached per process; the default maximum cache size is 16 and can be
changed with `LANCE_RAY_NAMESPACE_CACHE_SIZE`.

That last provider step is the pylance 4 contract. With Lance 12, reuse the
cached namespace client itself and pass it with `table_id` to fragment writing,
dataset opening, and commit. Lance constructs the credential-refresh provider
internally. Without that change the example fails; upgrading only the package
pins is insufficient.

Limitations in this Ray baseline:

- namespace-backed writes support only `CREATE`;
- `path` is ignored when namespace parameters resolve a location; and
- `read_lance` has no namespace parameters, so reads need prior catalog
  resolution into a URI and storage options.

Lance 12 and `lance-namespace` must be version-compatible. Test declaration,
credential refresh, and commit on remote workers, not only connection on the
driver.

## 7. Indexed reads and online queries

### 7.1 Scalar-filter scans

Ray's fragment groups are disjoint, so concatenating their native filtered
scans is natural. Lance may use BTree, bitmap, label-list, zone-map, bloom,
NGram, inverted, spatial, or other applicable indexes without Ray knowing the
physical plan.

### 7.2 ANN through `scanner_options`

The passthrough accepts Lance scanner arguments, including `nearest`. With more
than one Ray read task, `k` is applied by each fragment-group scanner and the
Ray Dataset is the union of local candidates. `read_lance` does not apply one
final global top-k.

For single-query batch work, either:

- use `override_num_blocks=1` and validate that one native Lance scanner sees
  the intended fragments/indexes; or
- sort the candidate union by `_distance` and take `k`, accepting the added
  Ray reduction; or
- implement an explicit bounded top-k reducer.

For a batch of query vectors, reduce independently by Lance's `query_index`.
Do not apply one global limit across all query vectors.

### 7.3 Recommended online pattern

Use a long-lived actor or Ray Serve replica so the Lance `Session` and index
cache survive across requests:

```python
import lance
import ray

@ray.remote
class LanceSearchReplica:
    def __init__(self, uri, storage_options=None, version=None):
        self._uri = uri
        self._storage_options = storage_options
        self._ds = lance.dataset(
            uri, version=version, storage_options=storage_options
        )

    def checkout_latest(self):
        self._ds.checkout_latest()
        return self._ds.version

    def search(self, vector, *, k=10, tenant=None):
        expr = None if tenant is None else f"tenant = {int(tenant)}"
        return self._ds.scanner(
            nearest={
                "column": "embedding",
                "q": vector,
                "k": k,
                "minimum_nprobes": 1,
                "maximum_nprobes": 50,
                "refine_factor": 10,
            },
            filter=expr,
            prefilter=expr is not None,
        ).to_table()
```

Choose an explicit refresh policy. Pinning a version maximizes reproducibility;
calling `checkout_latest` changes the snapshot and should be coordinated with
cache warmup and request consistency requirements.

## 8. Distributed Lance 12 index build with Ray Core

### 8.1 Control flow

```text
 durable job record / coordinator
      |
      +─ dataset URI + numeric source version
      +─ field/dimension/metric/full index parameters
      +─ model artifacts + hashes
      +─ exact target fragment set
      +─ disjoint worker groups + attempt records
      |
      v
 ray.put(shared model artifacts)
      |
      ├─────────────── top-level ObjectRef dereference ───────────────┐
      v                                                              v
 Ray build task A                                              Ray build task N
 open source version                                           open source version
 create_index_uncommitted(group A)                             (... group N)
      |                                                              |
      v                                                              v
 picklable Lance Index metadata + durable `_indices/<uuid>/` outputs
      └────────────────────────────┬─────────────────────────────────┘
                                   v
 coordinator barrier validates exact coverage and provenance
                                   |
                         direct commit or compatible
                         one-level merge groups
                                   |
                                   v
 refreshed coordinator commits final segment set exactly once
```

### 8.2 Illustrative IVF-PQ skeleton

The following is deliberately a skeleton. A production system must persist the
job and attempts, use secure credential delivery, balance groups by rows/bytes,
and handle ambiguous commit outcomes.

```python
from dataclasses import dataclass

import lance
import ray
from lance.indices import IndicesBuilder


@dataclass(frozen=True)
class BuildSpec:
    uri: str
    source_version: int
    column: str
    index_name: str
    metric: str
    num_partitions: int
    num_subvectors: int


@ray.remote(max_retries=0)
def build_segment(spec, fragment_ids, models, storage_options=None):
    # Numeric version is mandatory: every task reads the same snapshot.
    dataset = lance.dataset(
        spec.uri,
        version=spec.source_version,
        storage_options=storage_options,
    )
    # Omit index_uuid so every attempt receives a fresh physical UUID.
    return dataset.create_index_uncommitted(
        column=spec.column,
        index_type="IVF_PQ",
        name=spec.index_name,
        metric=spec.metric,
        num_partitions=spec.num_partitions,
        num_sub_vectors=spec.num_subvectors,
        ivf_centroids=models["ivf_centroids"],
        pq_codebook=models["pq_codebook"],
        fragment_ids=list(fragment_ids),
    )


def partition_disjointly(fragment_ids, task_count):
    # Illustrative only. Production grouping should balance rows or bytes.
    ordered = sorted(fragment_ids)
    return [ordered[i::task_count] for i in range(task_count) if ordered[i::task_count]]


uri = "s3://bucket/catalog/items.lance"
storage_options = {"region": "us-east-1"}
source = lance.dataset(uri, storage_options=storage_options)
source_version = source.version
target_ids = {fragment.fragment_id for fragment in source.get_fragments()}

spec = BuildSpec(
    uri=uri,
    source_version=source_version,
    column="embedding",
    index_name="embedding_ivf_pq",
    metric="L2",
    num_partitions=4096,
    num_subvectors=32,
)

# Lance 12's helper trains centrally. It is not distributed k-means.
models = IndicesBuilder(source, spec.column).prepare_global_ivf_pq(
    num_partitions=spec.num_partitions,
    num_subvectors=spec.num_subvectors,
    distance_type=spec.metric.lower(),
)
models_ref = ray.put(models)
groups = partition_disjointly(target_ids, task_count=128)

segment_refs = [
    build_segment.remote(spec, group, models_ref, storage_options)
    for group in groups
]
segments = ray.get(segment_refs)

# External semantic barrier.
covered = set().union(*(set(segment.fragment_ids) for segment in segments))
assert covered == target_ids
assert sum(len(segment.fragment_ids) for segment in segments) == len(covered)
assert len({segment.uuid for segment in segments}) == len(segments)

# Refresh latest and enforce the application's concurrent-write policy.
coordinator = lance.dataset(uri, storage_options=storage_options)
live_ids = {fragment.fragment_id for fragment in coordinator.get_fragments()}
if not target_ids <= live_ids:
    raise RuntimeError("source fragments changed while the index was building")

# Fast publication option: keep worker outputs as physical segments.
coordinator.commit_existing_index_segments(
    spec.index_name,
    spec.column,
    segments,
)
```

Top-level Ray `ObjectRef` arguments are dereferenced before task execution, so
the shared model can be placed in the Ray object store once and reused. The
durable source of truth for those artifacts must still be external if the
driver/object owner can fail.

### 8.3 Optional merge topology

Directly committing all worker segments avoids rewrite I/O but multiplies query
fanout. A one-segment merge minimizes query fanout but can become a serial I/O
bottleneck. Hybrid grouping gives a bounded segment count and parallel merges.

```text
 128 worker segments
       |
       +-- direct commit --------------------------> 128 query segments
       |
       +-- one merge of all ----------------------->   1 query segment
       |
       +-- 8 parallel groups x 16 inputs each ---->   8 query segments
```

For IVF-PQ and IVF-RQ in Lance 12, each input can participate in only one
physical merge level. Do not feed a merged result into a second recursive
merge. Shared IVF/PQ/RQ models are required for physical compatibility.

An SQ merger in this baseline does not validate equal learned bounds; keep
independently trained IVF-SQ and IVF-HNSW-SQ outputs separate.

### 8.4 Worker resource design

Set task resources from measured peak usage, not only input bytes:

```text
 worker heap
 ├── source scan/decode buffers
 ├── model artifacts
 ├── quantized/shuffle buffers
 └── HNSW construction state (when applicable)

 local temporary disk
 └── index shuffle/intermediate files

 durable object storage
 └── final _indices/<attempt-uuid>/ files
```

Ray's object-store memory resource does not account for all Lance native heap
or local temporary disk. Use `memory=` scheduling hints, custom resources/node
labels for scratch capacity, and measured concurrency limits.

## 9. Correctness and recovery contract

### 9.1 Persisted job record

Do not keep the only job state in a driver dictionary, actor heap, or Ray
ObjectRefs. Persist at least:

```text
 build ID
 dataset URI / branch / numeric source version
 column / field ID / Arrow type / dimension / nullability
 metric and complete ordered index-stage parameters
 serialized shared model locations and hashes
 intended fragment IDs and disjoint groups
 group attempt -> UUID, returned Index metadata, state, timestamps
 selected successful attempt per group
 merge group inputs -> output UUID and state
 final commit intent UUID set
 committed dataset version, when known
```

### 9.2 Retry policy

Lance file creation is an external side effect. Prefer application-controlled
retries that record each attempt and allocate a fresh UUID. The skeleton sets
`max_retries=0` to make this explicit.

If Ray automatic retries are enabled, each retry must still produce a unique
segment UUID, and only one successful attempt per fragment group may cross the
barrier. Other outputs become unreferenced cleanup candidates. Never make two
concurrent attempts write the same UUID.

### 9.3 Barrier checks

Before merge or commit prove:

- final UUIDs are unique;
- fragment bitmaps are nonempty and pairwise disjoint;
- their union is exactly the intended target set;
- every segment targets the expected Lance field ID;
- source version and external build ID match;
- metric, vector dimension, stage family, and complete parameters match;
- shared artifact hashes match where merge compatibility is required; and
- every referenced segment directory is complete.

Lance repeats structural validation, but it cannot infer the external build ID,
intended coverage, or artifact hash.

### 9.4 Concurrent table changes

Define policy before launching workers:

- Append may be acceptable if new fragments intentionally remain an unindexed
  delta.
- Deletions are masked by Lance queries but still change the operational source
  state.
- Rewrite, compaction, overwrite, or schema changes can retire or alter source
  coverage. Exact-coverage jobs should normally abort and rebuild.
- New vector overlays cause affected rows to be masked from old ANN segments
  and re-evaluated through current values. Correctness is preserved, but the
  fallback may violate latency goals.

Refresh the coordinator just before commit and compare it with the recorded
policy. Do not let each worker independently open “latest.”

### 9.5 Ambiguous commit

A client timeout does not prove that `commit_existing_index_segments` failed.
Inspect the latest logical-index description and expected UUID set before
retrying. Commit all final segments in one call so readers observe the old set
or the complete new set.

### 9.6 Cleanup

Failed attempts, superseded retries, merge inputs, and merged-but-uncommitted
outputs remain under `_indices/`. Lance classifies never-published files as
unverified and preserves them by default. Delete unverified files only after a
retention period longer than the maximum build/recovery window and after the
external job registry proves no active attempt needs them.

## 10. Operational patterns

| Workload | Recommended Ray–Lance shape |
|---|---|
| Offline ETL/embedding generation | Ray Data pipeline -> Lance-12-ported `write_lance` -> separate index workflow |
| Training data input | Pinned `read_lance` version, explicit columns/filter, stream batches to Train |
| High-QPS point ANN | Ray Serve/actor replicas with long-lived native Lance sessions |
| Large initial vector index | Ray Core tasks over disjoint fragments, shared model, bounded final segment topology |
| Incremental index delta | Build only uncovered fragments; commit same logical name; consolidate before fanout budget is exceeded |
| Table/catalog credentials | Namespace/provider where supported; otherwise securely distribute short-lived storage options |

Monitor both systems:

- Ray task duration, retries, worker heap, object-store pressure, local scratch,
  scheduler backlog, and node failures;
- Lance rows/bytes per second, fragment skew, source/destination requests,
  index stages, final segment count, cache hits/misses, ANN recall, and
  cold/warm query latency.

## 11. Lance 12 compatibility verification

Create a clean virtual environment; do not install into system Python. Pin the
exact Ray build, `pylance==12.0.0`, `lance-namespace>=0.11.1,<0.12` and its
resolver-compatible generated client, plus exact PyArrow and storage SDK
versions. The Ray repository's `lance-namespace==0.6.1` development pin must be
changed or isolated for this matrix, and the write-call adaptation in section
5.1 must be present.

Run at least the following repository tests against that environment:

```bash
python -m pytest -q python/ray/data/tests/datasource/test_lance.py
python -m pytest -q \
  python/ray/data/tests/test_predicate_pushdown.py::test_pushdown_filter_lance
```

Add integration coverage not present in the current Ray tests:

1. Add a signature/smoke regression proving the sink passes no removed
   `storage_options_provider` keyword to Lance 12.
2. Assert a default/explicit Ray write produces the intended Lance `2.2`
   storage version and that append with `None` inherits an existing table's
   manifest default.
3. Test local and target object-store reads/writes with the real credential
   path.
4. Test namespace declare, credential vending/refresh, and create-only
   rejection from remote Ray workers.
5. Write/read `FixedSizeList<Float32, D>` vectors and nullable-vector edge
   cases.
6. Build every required Lance index type centrally and query it through native
   Lance.
7. Verify `scanner_options["nearest"]` behavior with one Ray task and with many
   tasks plus an explicit global reducer.
8. Run a multi-worker uncommitted-segment build, barrier, optional merge, and
   commit; assert exact fragment coverage and representative ANN recall.
9. Inject worker failure, duplicate attempt, merge failure, driver restart,
   and ambiguous commit.
10. Append data after index creation and verify delta correctness and latency.
11. Exercise cleanup without deleting active uncommitted outputs.

For contribution-quality validation, record the exact commands and results and
have the human submitter review every generated/changed line as required by the
repository's `AGENTS.md`.

## 12. Known gaps and sharp edges

| Gap | Consequence | Response |
|---|---|---|
| Ray locks `pylance==4.0.0` and namespace 0.6.1, while Lance 12 requires namespace 0.11.x | The repository development environment cannot represent the target as-is | Update/isolate the lock and test a clean pinned matrix |
| Ray passes removed `storage_options_provider=` keywords and imports a provider no longer exported in Python | All unmodified `write_lance` modes fail against Lance 12; namespace writes have an additional import failure | Port all three call sites to `namespace_client` + `table_id`, omit namespace kwargs for URI writes, then test |
| Read retry wrapper returns a lazy generator before scanner I/O | Native scan iteration errors bypass the intended `call_with_retry` boundary | Use task retries and add a test/fix before relying on fragment-level retries |
| Ray's write docstring says default legacy and Lance's fragment docstring says `2.0`, while the Lance 12 Rust default is `2.2` for new tables | Wrong on-disk assumption | Pin exact `2.2`; inspect the result; test existing-table inheritance |
| Fragment grouping is count-based | Read skew for unequal fragments | Compact/rebalance or implement size-aware grouping |
| Write retries materialize input bundles | Higher worker peak memory | Bound bundles/concurrency and measure |
| Namespace read API absent | Catalog resolution is asymmetric | Resolve before `read_lance` or add a dedicated datasource |
| Namespace writes create-only | No catalog-backed append/overwrite through this surface | Use native Lance/catalog workflow or extend/test connector |
| No Ray Data index-build operator | No automatic barrier/job state | Use Ray Core orchestration with external state |
| No global ANN reduction in `read_lance` | Multiple tasks return candidate union | One native scanner or explicit per-query top-k |
| Ray write does not maintain indexes | Appends create unindexed deltas | Schedule index append/optimization separately |
| Task retries leave durable attempts | Orphaned `_indices`/data objects | Unique UUIDs, attempt registry, conservative cleanup |
| Central shared-model training | Front-end bottleneck at large scale | Resource it separately; monitor Lance distributed-training work |
| Lance 12 PQ/RQ recursive merge unsupported | Cannot use arbitrary merge trees | One physical merge level only |
| Lance 12 SQ merge validation gap | Independent SQ code bounds may be misinterpreted | Do not physically merge independent SQ segments |

## 13. Source map

### Ray

- Public read API: [`python/ray/data/read_api.py`](../../python/ray/data/read_api.py)
- Public `write_lance`: [`python/ray/data/dataset.py`](../../python/ray/data/dataset.py)
- Lance datasource: [`python/ray/data/_internal/datasource/lance_datasource.py`](../../python/ray/data/_internal/datasource/lance_datasource.py)
- Lance datasink: [`python/ray/data/_internal/datasource/lance_datasink.py`](../../python/ray/data/_internal/datasource/lance_datasink.py)
- Namespace helpers: [`python/ray/data/_internal/datasource/lance_utils.py`](../../python/ray/data/_internal/datasource/lance_utils.py)
- Retry configuration: [`python/ray/data/context.py`](../../python/ray/data/context.py)
- Connector tests: [`python/ray/data/tests/datasource/test_lance.py`](../../python/ray/data/tests/datasource/test_lance.py)
- Predicate test: [`python/ray/data/tests/test_predicate_pushdown.py`](../../python/ray/data/tests/test_predicate_pushdown.py)
- Compiled requirements: [`python/requirements_compiled.txt`](../../python/requirements_compiled.txt)

### Lance 12

- Architecture overview: [`../../../lance-open-source/plans/docs/00-overview.md`](../../../lance-open-source/plans/docs/00-overview.md)
- Vector storage: [`../../../lance-open-source/plans/docs/01-vector-storage.md`](../../../lance-open-source/plans/docs/01-vector-storage.md)
- Vector algorithms: [`../../../lance-open-source/plans/docs/02-vector-indexes.md`](../../../lance-open-source/plans/docs/02-vector-indexes.md)
- Index files and query path: [`../../../lance-open-source/plans/docs/03-index-on-disk-and-search.md`](../../../lance-open-source/plans/docs/03-index-on-disk-and-search.md)
- Release/file-format versions: [`../../../lance-open-source/plans/docs/04-lance-versions.md`](../../../lance-open-source/plans/docs/04-lance-versions.md)
- Distributed index creation: [`../../../lance-open-source/plans/docs/05-distributed-vector-index-creation.md`](../../../lance-open-source/plans/docs/05-distributed-vector-index-creation.md)
- Python dependency declaration: [`../../../lance-open-source/python/pyproject.toml`](../../../lance-open-source/python/pyproject.toml)
- Fragment writer signature: [`../../../lance-open-source/python/python/lance/fragment.py`](../../../lance-open-source/python/python/lance/fragment.py)
- Dataset open/commit signatures: [`../../../lance-open-source/python/python/lance/dataset.py`](../../../lance-open-source/python/python/lance/dataset.py)
