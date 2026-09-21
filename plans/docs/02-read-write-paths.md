# Read and write paths

“Read” and “write” can refer either to Ray Core objects or to external data
through Ray Data. This document traces both and then expands the built-in Lance
connector.

## 1. Ray Core object write path

Ray creates immutable objects through `ray.put`, task returns, actor-method
returns, and streaming-generator yields.

```text
 producer process
      |
      | serialize value
      v
 size / representation decision
      |
      +-- small/direct value ──▶ owner memory store / task RPC payload
      |
      +-- large value ─────────▶ local shared-memory object store
                                      |
                                      +─ seal immutable object
                                      +─ register location/ownership metadata
                                      +─ spill later under memory pressure
      |
      v
 ObjectRef returned to caller; data remains out of the caller process
```

For a normal remote task, the caller creates the return IDs before execution
and owns their metadata, while the executor produces the values. A small result
can come back inline in the `PushTask` reply. A larger result is sealed in the
executor node's object store and the reply identifies it as an object-store
result.

Objects are immutable after publication. This permits caching and replication
without a coherence protocol for mutable values.

## 2. Ray Core object read path

`ray.get(ref)` first resolves availability and then retrieves the value:

```text
 consumer calls ray.get(ObjectRef)
      |
      v
 consumer CoreWorker asks owner/location metadata
      |
      +-- direct value available ──▶ deserialize inline bytes
      |
      +-- object is local ─────────▶ read local shared-memory buffer
      |
      +-- object is remote ────────▶ local object manager pulls from peer
      |                                  |
      |                                  v
      |                            local object store
      |
      +-- object was spilled ──────▶ restore into an object store
      |
      +-- all copies lost ─────────▶ try lineage reconstruction
                                         or raise object-loss error
```

NumPy arrays and Arrow-compatible buffers can be exposed zero-copy within a
node. General Python objects require deserialization. Top-level `ObjectRef`
task arguments are resolved before a task runs; nested refs are passed through
and are only fetched if user code calls `ray.get`.

The object store is a runtime cache, not durable application storage. Spilling
protects execution from shared-memory pressure; it does not give objects table
versions, transactions, or long-term retention.

## 3. Generic Ray Data read path

A Ray Data read has a planning phase and an execution phase.

```text
 public read_* API
      |
      v
 Datasource instance
      |
      | get_read_tasks(parallelism) for metadata/planning
      v
 logical Read operator ── lazy Dataset returned to application
      |
      | consumption: iter_batches / materialize / write / count / ...
      v
 optimize logical plan; push predicates/projections where supported
      |
      v
 plan_read_op
      |
      +─ datasource.get_read_tasks(detected_parallelism)
      +─ ray.put each ReadTask description
      +─ InputDataBuffer of task refs
      +─ TaskPoolMapOperator or ActorPoolMapOperator
      |
      v
 remote worker invokes ReadTask.read_fn()
      |
      v
 one or more Arrow/pandas blocks sealed in a Ray object store
      |
      v
 downstream operators consume block refs as a stream
```

The datasource decides how external work maps to read tasks. A task can emit
more than one block, so read-task count, file count, and final block count are
not necessarily equal. Ray's block-size shaping may further split oversized
outputs.

The current legacy datasource path calls `get_read_tasks` during API planning
and again during physical planning. Implementations should keep enumeration
side-effect-free and reasonably cheap.

## 4. Generic Ray Data write path

`Dataset.write_datasink` is a terminal operation. It builds a logical `Write`,
materializes it, waits for worker results, and invokes the sink's completion
callback on the driver.

```text
 upstream Dataset blocks
      |
      v
 logical Write(datasink)
      |
      v
 plan_write_op -> MapOperator("Write")
      |
      +─ optional bundling to Datasink.min_rows_per_write
      +─ remote task calls Datasink.write(block_iterator, TaskContext)
      +─ task reports rows, bytes, and an arbitrary write_return
      |
      v
 driver combines all WriteResult values
      |
      +-- success ──▶ Datasink.on_write_complete(combined_result)
      |
      +-- failure ──▶ Datasink.on_write_failed(error), then re-raise
```

The separation between task-local writes and `on_write_complete` supports
two-phase sink designs: workers create data files or staging objects, and one
driver-side action makes them visible. Whether this is truly transactional
depends on the external system and datasink implementation.

## 5. Built-in Lance read path

### 5.1 Planning

`ray.data.read_lance` creates a `LanceDatasource` and calls the generic
`read_datasource` path.

```text
 ray.data.read_lance(uri, version, columns, filter, scanner_options, ...)
      |
      v
 driver: LanceDatasource.__init__
      |
      +─ import `lance` (`pylance` distribution)
      +─ copy scanner options
      +─ add `columns` and deprecated source `filter`, if supplied
      +─ lance.dataset(uri, version=..., storage_options=...)
      +─ capture Lance retry policy from DataContext
      |
      v
 driver: LanceDatasource.get_read_tasks(parallelism)
      |
      +─ get selected scanner fragments or dataset.get_fragments()
      +─ split fragments into roughly equal-count groups with numpy.array_split
      +─ collect fragment IDs, row counts, and data-file paths
      +─ create one Ray ReadTask per nonempty group
```

Fragment groups are balanced by fragment count, not estimated bytes or rows.
Highly skewed Lance fragments can therefore produce skewed Ray tasks.

The datasource stores a Lance dataset handle opened at construction. Supplying
an integer version or tag pins the intended Lance snapshot. Omitting `version`
opens the latest version at that time; a long-lived Ray Dataset should not be
assumed to refresh itself after later commits.

### 5.2 Predicate and projection behavior

The explicit `columns=` argument is put into Lance scanner options. Ray
expression filters such as `ds.filter(expr=...)` can be absorbed into the
`Read` because `LanceDatasource.supports_predicate_pushdown()` returns true.
At task creation the expression is converted through PyArrow to a SQL-like
string and combined with the deprecated `read_lance(filter=...)` string using
`AND`.

```text
 read_lance(filter="tenant = 7")
       .filter(expr=col("score") > 0.8)

 task scanner filter:
     (<pushed PyArrow expression>) AND (tenant = 7)
```

The datasource does not advertise generic Ray projection pushdown in this
checkout, so use the `columns=` argument when physical Lance column pruning is
required.

### 5.3 Worker scan

```text
 Ray read task receives fragment IDs
      |
      v
 lance_ds.get_fragment(id) for each ID
      |
      v
 lance_ds.scanner(fragments=group, **scanner_options)
      |
      v
 Lance manifest/index planning -> object-store byte reads -> decode
      |
      v
 scanner.to_reader() yields Arrow RecordBatch values
      |
      v
 wrap each batch in pyarrow.Table
      |
      +─ reject Arrow pickled-object extension columns by default
      |
      v
 yield Ray Data blocks into the Ray object store
```

Only fragment IDs are captured in each read closure because serializing full
`LanceFragment` objects is expensive. The code passes fragment reads through a
`call_with_retry` wrapper configured for ten attempts, a maximum 32-second
backoff, and `LanceError(IO)` plus Ray Data's general retried-I/O substrings.
There is an important implementation caveat: `_read_fragments` is a generator,
so the wrapper returns successfully when it creates the generator, before the
scanner is consumed. Exceptions raised later by `scanner.to_reader()` iteration
aren't caught by that wrapper in the current implementation. Rely on Ray task
retry policy or fix/materialize the retry boundary before claiming per-scan
retry guarantees.

Pickled Python object columns are rejected before deserialization to prevent
untrusted-data code execution. The opt-in environment variable
`RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1` must be set on both driver and
workers only for trusted data.

## 6. Built-in Lance write path

Ray's Lance sink is a distributed-fragment, single-commit design. The diagram
and phases below describe the implementation in this Ray tree. They are the
right architectural shape for Lance 12, but the calls require an API adaptation
before they can execute against Lance 12.0.0: this Ray code always supplies the
old `storage_options_provider=` keyword, whereas Lance 12's
`write_fragments`, `LanceDataset` constructor, and `LanceDataset.commit` use
`namespace_client=` and `table_id=` instead. An ordinary URI write also passes
the obsolete keyword with value `None`, so this is not limited to namespace
writes.

```text
 Dataset.write_lance(...)
      |
      v
 driver: construct LanceDatasink
      |
      +─ optionally resolve namespace table -> URI + storage options
      +─ validate mode / inspect existing dataset
      |
      v
 Write MapOperator bundles upstream blocks
      |
      ├─────────────┬─────────────┬─────────────────┐
      v             v             v                 v
 worker 0        worker 1      worker 2          worker N
 convert blocks  convert       convert           convert
 to Arrow        to Arrow      to Arrow          to Arrow
      |             |             |                 |
 Lance `write_fragments` writes immutable data files and returns
 uncommitted FragmentMetadata values
      └─────────────┴─────────────┴─────────────────┘
                            |
                  pickled metadata results
                            v
 driver: unify worker schemas
                            |
             +--------------+---------------+
             |                              |
       CREATE/OVERWRITE                   APPEND
       LanceOperation.Overwrite           LanceOperation.Append
             |                              |
             +--------------+---------------+
                            v
 LanceDataset.commit(uri, operation, read_version=...)
                            |
                            v
 one new manifest version publishes all fragments atomically
```

### 6.1 Start phase

Supported modes are `SaveMode.CREATE`, `APPEND`, and `OVERWRITE`.

- `CREATE` opens the target to check that it does not already exist. If it can
  be opened, Ray raises and tells the caller to choose append or overwrite.
- `APPEND` opens the existing dataset, records its numeric version as
  `read_version`, and adopts its schema if no schema was supplied. The version
  participates in Lance's optimistic commit check.
- `OVERWRITE` prepares replacement fragments without reading the old schema.
- Namespace-backed writes currently allow only `CREATE`.

The existence helper intentionally treats any failed open as “not existing” so
that the subsequent native Lance call reports the real storage/credential
error. Consequently the preflight is not a substitute for Lance commit-time
concurrency semantics.

### 6.2 Worker phase

Each write task:

1. obtains a block iterator, normally bundled toward `min_rows_per_file`;
2. infers a schema from the first block if no schema is provided;
3. converts every block to Arrow and reorders columns to the target schema;
4. creates an Arrow `RecordBatchReader`;
5. calls `lance.fragment.write_fragments`; and
6. returns pickled `(FragmentMetadata, schema)` pairs to the driver.

`min_rows_per_file` is implemented as Ray's minimum rows per write bundle. It
does not force Lance to combine output from different Ray tasks into one file.
`max_rows_per_file` is passed to Lance and a single task can still return
multiple fragments/files.

With the default retry count greater than one, the sink materializes the task's
entire input block iterator into a list so it can replay the stream. This
improves retryability but increases each task's peak heap/object residency.
Account for it when sizing write bundles.

### 6.3 Commit phase

The driver unpickles all fragment metadata, validates/unifies the schemas, and
commits once. `CREATE` and `OVERWRITE` both use Lance's overwrite operation;
the separate create preflight supplies the no-clobber user behavior. `APPEND`
uses the recorded `read_version`.

If no fragments or schemas are returned, the sink skips the commit. If a task,
driver, or commit fails after durable objects were written, this implementation
does not delete those objects in `on_write_failed`; they can remain unreferenced
until a safe Lance cleanup policy removes them.

## 7. Namespace and credential path

For namespace-backed writes:

```text
 namespace_impl + namespace_properties + table_id
       |
       v
 cached lance_namespace client (per process, default cache size 16)
       |
       +─ declare_table(...)
       |     or fallback create_empty_table(...)
       v
 resolved table URI + namespace storage options
       |
       +─ namespace options override same-named caller options
       v
 Ray creates a public LanceNamespaceStorageOptionsProvider
 (pylance 4-era connector contract)
       |
       v
 native Lance I/O with credential vending/refresh
```

When namespace parameters are complete, the explicit `path` is ignored with a
warning. The read API has no parallel namespace arguments in this checkout; an
application must resolve a namespace table externally and pass the resolved URI
and storage options to `read_lance`.

Lance 12 no longer exports that Python provider as the connector expects. It
accepts the namespace client and table ID on its public Python calls and builds
the provider internally. A Lance 12 adapter therefore needs to pass the cached
client returned by `lance_namespace.connect`, not instantiate or pass the old
provider object.

## 8. Lance 12 storage-version behavior

Ray passes `data_storage_version` unchanged to Lance. Under Lance 12:

- for a new dataset, `None` resolves to Lance's default exact format, `2.2`;
- for an existing dataset, `None` inherits the exact format recorded in the
  manifest rather than silently migrating the table;
- `stable` resolves to exact file format `2.2`;
- `next` selects unstable `2.3`.

The current Ray `write_lance` docstring says the default is legacy v1, but the
implementation default is `None`; Lance 12's Rust implementation resolves a
new table to `2.2` and an existing table to its manifest default. (The Lance 12
Python fragment docstring also still says `2.0`, so it is stale too.) For a
reproducible Lance 12 deployment, set the desired exact/selector value
explicitly and validate the resulting dataset's `data_storage_version`. Do not
rely on either stale sentence.

## 9. Path comparison

| Property | Ray object | Ray Data block | Lance fragment/data file |
|---|---|---|---|
| Lifetime | Reference/lineage driven | Execution-plan driven | Manifest/version driven |
| Storage | Shared memory, replicas, spill | Ray object store between operators | Durable local/object storage |
| Mutability | Immutable | Immutable as a Ray object | Immutable file; new manifests/overlays represent change |
| Visibility | ObjectRef readiness | Downstream operator readiness | Atomic Lance manifest commit |
| Recovery | Copy or task lineage when possible | Re-execute plan/task when possible | Open an immutable committed version |
| Primary identifier | Object ID | Object ID plus block metadata | Fragment ID/file UUID and manifest version |

## 10. Primary source map

- Task lifecycle: [`doc/source/ray-core/internals/task-lifecycle.rst`](../../doc/source/ray-core/internals/task-lifecycle.rst)
- Object storage: [`doc/source/ray-core/objects.rst`](../../doc/source/ray-core/objects.rst)
- Generic read API: [`python/ray/data/read_api.py`](../../python/ray/data/read_api.py)
- Datasource/ReadTask contract: [`python/ray/data/datasource/datasource.py`](../../python/ray/data/datasource/datasource.py)
- Generic read planner: [`python/ray/data/_internal/planner/plan_read_op.py`](../../python/ray/data/_internal/planner/plan_read_op.py)
- Datasink contract: [`python/ray/data/datasource/datasink.py`](../../python/ray/data/datasource/datasink.py)
- Generic write planner: [`python/ray/data/_internal/planner/plan_write_op.py`](../../python/ray/data/_internal/planner/plan_write_op.py)
- Lance read implementation: [`python/ray/data/_internal/datasource/lance_datasource.py`](../../python/ray/data/_internal/datasource/lance_datasource.py)
- Lance write implementation: [`python/ray/data/_internal/datasource/lance_datasink.py`](../../python/ray/data/_internal/datasource/lance_datasink.py)
- Lance namespace helpers: [`python/ray/data/_internal/datasource/lance_utils.py`](../../python/ray/data/_internal/datasource/lance_utils.py)
- Lance 12 fragment writer: [`../../../lance-open-source/python/python/lance/fragment.py`](../../../lance-open-source/python/python/lance/fragment.py)
- Lance 12 dataset API: [`../../../lance-open-source/python/python/lance/dataset.py`](../../../lance-open-source/python/python/lance/dataset.py)
- Lance 12 vector storage: [`../../../lance-open-source/plans/docs/01-vector-storage.md`](../../../lance-open-source/plans/docs/01-vector-storage.md)
