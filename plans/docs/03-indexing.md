# Indexing functionality

## 1. The responsibility boundary

Ray does not expose a database-style secondary-index abstraction over a
`ray.data.Dataset`. Ray schedules compute and moves blocks; a persistent system
such as Lance owns durable indexes, index/query semantics, and transactional
publication.

The word “index” does appear in Ray for unrelated mechanisms:

| Mechanism | Purpose | Not equivalent to |
|---|---|---|
| Object IDs and owner/location metadata | Find immutable Ray objects across a running cluster | A durable row/value index |
| GCS tables/maps | Locate nodes, actors, jobs, and placement groups | A user-queryable table index |
| Ray Data `FileIndexer` | List/prune files for DataSource V2 planning | A secondary index over rows |
| Batch/task indices | Preserve order and identify execution units | Search acceleration |

The built-in Lance connector can benefit from indexes when the native Lance
scanner uses them. Index creation and maintenance remain Lance operations.

## 2. Lance 12 index model

A Lance dataset stores immutable data fragments and secondary-index payloads.
The manifest version atomically ties the two together.

```text
 dataset.lance/
 ├── data/
 │   └── <uuid>.lance             immutable fragment data files
 ├── _versions/
 │   └── <version>.manifest       fragments + index metadata snapshot
 └── _indices/
     ├── <segment-uuid-A>/        physical index segment
     │   ├── index.idx
     │   └── auxiliary.idx        vector families; exact files vary by type
     └── <segment-uuid-B>/
```

A user-visible logical index is identified by name. It can contain multiple
physical segments, each with a unique UUID and a bitmap of the fragments it
covers.

```text
 logical index "embedding_idx"
             |
             +── segment A, UUID=A, fragments {0,1,2}
             +── segment B, UUID=B, fragments {3,4}
             +── segment C, UUID=C, fragments {5}

 manifest fragments {0,1,2,3,4,5,6,7}
 indexed coverage  = union(A,B,C) = {0,1,2,3,4,5}
 unindexed delta                         {6,7}
```

Normal Lance queries search all compatible segments and combine them with a
flat scan of uncovered fragments. `fast_search=true` can skip that delta at the
cost of completeness.

## 3. Scalar, text, spatial, and system indexes

Lance 12's index registry contains several non-vector families. The most
important user-facing choices are:

| Family | Intended workload | Result behavior |
|---|---|---|
| BTree | Equality, `IN`, ranges, sorted access on higher-cardinality columns | Exact |
| Bitmap | Equality/small ranges on low-cardinality values | Exact |
| LabelList | `array_has`, `array_has_all`, and `array_has_any` over tags/labels | Exact |
| Inverted | Tokenized full-text search | Search/scoring semantics defined by Lance |
| NGram | Substring, infix `LIKE`, and regex candidate pruning | Candidate set is rechecked for exact predicate results |
| ZoneMap | Min/max/null summaries used to prune fixed-size zones | Inexact pruning followed by recheck; null bitmap can be exact |
| BloomFilter | Space-efficient equality/membership pruning | No false negatives; false-positive candidates are rechecked |
| RTree | Static two-dimensional bounding-box/rectangle pruning | Spatial candidate pruning |

Lance also has system-oriented index state such as the Fragment Reuse Index,
which remaps row addresses after deferred index remapping during compaction,
and MemWAL-related structures. These are not substitutes for application
secondary-index selection.

When a Ray expression filter is pushed into `LanceDatasource`, the native Lance
scanner decides whether a compatible scalar index can accelerate it. Ray does
not inspect or choose the index itself.

## 4. Lance vector-index taxonomy

Lance 12's exposed vector indexes all have an outer IVF partitioning stage.
They combine two inner choices: flat versus HNSW search and full-precision
versus quantized distance storage.

```text
                         vector rows
                              |
                         IVF centroids
                    assign row to partition
                              |
          ┌───────────────────┼───────────────────┐
          v                   v                   v
      partition 0        partition 1         partition N
          |                   |                   |
          +-- sub-index: Flat or HNSW graph ------+
          |                   |                   |
          +-- values: Flat / SQ / PQ / RaBitQ ----+
```

| Lance type | Composition | Main trade-off |
|---|---|---|
| `IVF_FLAT` | IVF + flat full-precision vectors | Highest within-probed-partition fidelity; largest scans/storage |
| `IVF_SQ` | IVF + flat scalar-quantized vectors | One byte per dimension in the current SQ8 implementation |
| `IVF_PQ` | IVF + flat product-quantized codes | Very compact codes and fast lookup-table distances; approximate |
| `IVF_RQ` | IVF + rotated bit quantization | Different recall/compute point; v12 defaults to five bits/dimension |
| `IVF_HNSW_FLAT` | IVF + HNSW + full-precision distances | Graph speed within large partitions; larger graph/storage cost |
| `IVF_HNSW_SQ` | IVF + HNSW + SQ | Graph traversal with compact scalar distances |
| `IVF_HNSW_PQ` | IVF + HNSW + PQ | Graph traversal with highly compact approximate codes |

There is no pure, top-level HNSW index without IVF in this Lance 12 baseline.

### Principal tuning dimensions

| Stage | Build-time knobs | Query-time knobs |
|---|---|---|
| IVF | partitions/target partition size, training sample, centroids | fixed/automatic probe count (`nprobes`) |
| PQ | subvector count, 4- or 8-bit codebooks | refine factor and probe count |
| SQ | currently operationally SQ8, learned bounds | refine factor and probe count |
| RaBitQ | 1–9 bits, rotation model; v12 default 5 | approximation mode, refine factor |
| HNSW | `m`, `ef_construction`, maximum level | `ef_search`, approximation mode |

The distance metric (`L2`, cosine, dot, and supported hamming paths) must match
the data and the model used to build the index.

## 5. Centralized vector-index build path

For a normal `lance.dataset(...).create_index(...)`, Lance performs:

```text
 committed vector column
      |
      v
 sample rows ──▶ train IVF centroids
      |
      +────────▶ train/load quantizer (PQ, SQ, or RaBitQ)
      |
      v
 assign every covered vector to an IVF partition
      |
      v
 shuffle encoded rows by partition
      |
      v
 build partition sub-index (flat storage or HNSW graph)
      |
      v
 write _indices/<new-uuid>/ index files
      |
      v
 commit IndexMetadata + exact fragment coverage in a new manifest
```

Data and index files are immutable. Rebuilds and optimization create new
segment UUIDs; a new manifest switches readers atomically. Older snapshots can
continue to reference older segments until cleanup.

## 6. Vector query path

```text
 nearest(column="embedding", q, k)
      |
      v
 load every compatible segment for the logical index
      |
      +─ rank that segment's IVF centroids for q
      +─ probe selected partitions
      +─ flat scan or HNSW traversal using Flat/SQ/PQ/RQ distance
      +─ retain segment-local candidates
      |
      v
 merge candidates across physical segments
      |
      +─ merge exact flat-KNN results from uncovered fragments
      +─ mask deletions, retired ownership, and stale overlay rows
      |
      +─ optional exact refine:
      |      fetch k * refine_factor raw vectors and rerank
      |
      +─ optional prefilter/postfilter plan
      v
 top-k rows + `_distance`
```

With `prefilter=true`, Lance evaluates the scalar predicate first and passes a
row-ID mask into vector search. With postfiltering, ANN runs first and the
predicate can remove candidates, possibly returning fewer than `k` rows. Lance
can use scalar indexes to construct a prefilter mask.

Segment count is part of query cost. Each segment has its own model and probe
work, so hundreds of small segments can dominate latency even though the
logical result is correct.

## 7. Distributed index creation protocol

Lance 12 supplies scheduler-neutral worker and coordinator APIs; it does not
supply a Ray/Spark/Dask scheduler. The external orchestrator owns the job.

```text
 coordinator: pin source version and index contract
          |
          +─ train and distribute shared model artifacts, if required
          +─ partition fragment IDs into disjoint nonempty groups
          |
      ┌───┴───────────┬───────────┴───┐
      v               v               v
 Ray task 0       Ray task 1       Ray task N
 Lance create_index_uncommitted(fragment_ids=group)
      |               |               |
      v               v               v
 segment A         segment B        segment N
      └───────────────┬───────────────┘
                      v
 external barrier validates UUIDs, coverage, field, source, and model hashes
                      |
             ┌────────┴─────────┐
             v                  v
       direct segment set   one-level merge groups
             └────────┬─────────┘
                      v
 commit_existing_index_segments(...) once
                      |
                      v
 one new manifest atomically publishes the logical index
```

The Ray implementation design is detailed in
[05-ray-lance-integration.md](05-ray-lance-integration.md).

### Shared versus independent model scope

- Shared IVF/model artifacts give partitions and codes consistent meaning
  across workers and are required when outputs will be physically merged.
- Independently trained segments can be committed together and queried
  correctly because each segment retains its own model. They must stay as
  separate physical segments unless the merger can prove compatibility.

The orchestrator must still enforce a single field, dimension, metric, stage
family, parameter contract, source version, and intended coverage. Direct
commit intentionally does not prove full model equality.

### Lance 12 merge constraints

- Fragment coverage must be disjoint and segment UUIDs unique.
- IVF/PQ/RQ/HNSW model compatibility is required for physical merge.
- Independently trained SQ segments must not be physically merged because the
  v12 merger does not validate equal learned SQ bounds.
- PQ and RQ distributed worker layouts undergo a one-way final transform during
  merge. Do not recursively merge already-merged PQ/RQ segments in v12; use at
  most one merge level and commit several merged groups if necessary.
- Merging HNSW segments rebuilds graphs and can be expensive.
- Uncommitted and superseded segment directories are not automatically rolled
  back and need an explicit safe cleanup policy.

## 8. How the built-in Ray connector uses indexes

### Filter scans

`ray.data.read_lance(...).filter(expr=...)` pushes supported expressions into
each Lance scanner. Lance may use scalar indexes, zone maps, or other native
planning. This is a supported and natural connector use: each fragment group
produces ordinary filtered rows and Ray concatenates the disjoint groups.

### Nearest-neighbor scans

`scanner_options` is passed through to `LanceDataset.scanner`, so an application
can technically supply a `nearest` dictionary. The current Ray datasource does
not, however, implement a global distributed top-k operator:

```text
 fragment group A -> Lance scanner -> local top-k candidates
 fragment group B -> Lance scanner -> local top-k candidates
 fragment group C -> Lance scanner -> local top-k candidates
                                  |
                                  v
                     Ray Dataset concatenates candidates
                     (no final top-k in read_lance itself)
```

The union contains candidates, not the final contract of a single native Lance
query. For a single query vector, an application can globally sort by
`_distance` and limit to `k`, but that adds a Ray shuffle/reduction and requires
care around filters and ties. Batch queries additionally need a top-k per
`query_index`.

For a normal global Lance ANN query, prefer one of these patterns:

1. Call Lance directly inside the client or a Ray Serve/actor replica. Lance
   already parallelizes its internal I/O and searches all index segments.
2. Force one Ray read task with `override_num_blocks=1`, then validate the
   resulting plan and performance for the target Lance version.
3. Deliberately use multiple Ray candidate tasks and implement/test an explicit
   global top-k merge.

The connector's test suite currently covers normal reads, projection via
`columns`, predicate pushdown, versions, and writes—not vector nearest-neighbor
result reduction.

## 9. Index maintenance after Ray writes

`Dataset.write_lance` creates/appends/overwrites data fragments only. It does
not call `create_index`, append an existing index, or optimize index segments.

After an append:

```text
 old index coverage:   fragments {0..99}
 Ray append creates:   fragments {100..109}

 normal Lance query = ANN({0..99}) + flat KNN/filter scan({100..109})
```

This remains correct under normal Lance query semantics but may degrade
latency. Schedule Lance index append/optimization separately and establish a
segment-count/coverage service-level objective.

After overwrite, old index segments belong to older dataset versions and the
new snapshot needs indexes appropriate to its new fragment set.

## 10. Lance 12 version notes relevant to indexing

- Library release `12.0.0`, data-file version, and index-layout version are
  separate axes.
- New datasets using `stable` write data-file format `2.2` in Lance 12.
- New IVF-RQ indexes default to five bits per dimension; set `num_bits=1`
  explicitly for binary RaBitQ.
- Logical indexes can be segment sets with a union of fragment coverage.
- Queries reconcile uncovered fragments and newer overlay values unless the
  caller explicitly chooses indexed-only behavior.
- The external orchestrator, not Lance, owns distributed job state, retries,
  model broadcast, and the semantic barrier before commit.

## 11. Primary source map

- Ray Lance datasource: [`python/ray/data/_internal/datasource/lance_datasource.py`](../../python/ray/data/_internal/datasource/lance_datasource.py)
- Ray predicate-pushdown tests: [`python/ray/data/tests/test_predicate_pushdown.py`](../../python/ray/data/tests/test_predicate_pushdown.py)
- Ray Lance tests: [`python/ray/data/tests/datasource/test_lance.py`](../../python/ray/data/tests/datasource/test_lance.py)
- Ray DataSource V2 file indexer: [`python/ray/data/_internal/datasource_v2/listing/file_indexer.py`](../../python/ray/data/_internal/datasource_v2/listing/file_indexer.py)
- Lance 12 overview: [`../../../lance-open-source/plans/docs/00-overview.md`](../../../lance-open-source/plans/docs/00-overview.md)
- Lance vector algorithms: [`../../../lance-open-source/plans/docs/02-vector-indexes.md`](../../../lance-open-source/plans/docs/02-vector-indexes.md)
- Lance index storage/query: [`../../../lance-open-source/plans/docs/03-index-on-disk-and-search.md`](../../../lance-open-source/plans/docs/03-index-on-disk-and-search.md)
- Lance distributed index protocol: [`../../../lance-open-source/plans/docs/05-distributed-vector-index-creation.md`](../../../lance-open-source/plans/docs/05-distributed-vector-index-creation.md)

