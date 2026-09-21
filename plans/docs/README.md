# Ray architecture and Lance 12 integration notes

This documentation set describes the checked-out Ray source tree, its runtime and
Ray Data architecture, the main read and write paths, and the boundary between
Ray and persistent indexing systems. Lance receives extra attention because the
target integration is Lance 12.0.

## Baselines

| Component | Baseline used here |
|---|---|
| Ray | `2.58.0`, branch `master-described`, commit `01b49e4fa77b30bc28c69ed702dfe523fd6421d8` (2026-08-21) |
| Lance | `12.0.0`, commit `cbeec97cb` (2026-09-17), documented in `../../../lance-open-source/plans/docs` |

The Ray checkout's compiled development requirements currently pin
`pylance==4.0.0` and `lance-namespace==0.6.1`. Lance 12's Python project
requires `lance-namespace>=0.11.1,<0.12`, so the repository development lock is
not a Lance 12 environment and contains a concrete namespace-version conflict.
There is also a source-level write incompatibility: Ray passes the former
`storage_options_provider=` keyword to fragment write, dataset-open, and commit
calls, while Lance 12 accepts `namespace_client=` plus `table_id=` and creates
the provider internally. Because Ray passes the old keyword even when its value
is `None`, the unmodified built-in `write_lance` path is not Lance 12 compatible.
The read signatures inspected here do align, subject to runtime verification.

The integration analysis below therefore distinguishes:

- behavior present in this Ray checkout;
- behavior supplied by Lance 12;
- orchestration that an application must add; and
- compatibility claims that still require an end-to-end test with the exact
  Ray, `pylance`, `lance-namespace`, Arrow, and object-store packages deployed.

The write-path chapters describe both the connector's intended two-phase
architecture and the small API adaptation required before using it with Lance
12; they do not imply that the two checked-out versions work together
unchanged.

## Documents

| File | Contents |
|---|---|
| [00-project-overview.md](00-project-overview.md) | What Ray is, its user-facing layers, core abstractions, repository layout, and responsibility boundaries |
| [01-architecture.md](01-architecture.md) | Cluster, runtime, and Ray Data architecture with ASCII diagrams |
| [02-read-write-paths.md](02-read-write-paths.md) | Ray Core object movement, generic Ray Data reads/writes, and the Lance connector's concrete data paths |
| [03-indexing.md](03-indexing.md) | What “indexing” means in Ray, Lance 12 scalar/vector indexes, build/search paths, and distributed-index boundaries |
| [04-integrations.md](04-integrations.md) | How Ray integrates with storage, table formats, ML frameworks, serving systems, and other execution engines |
| [05-ray-lance-integration.md](05-ray-lance-integration.md) | Focused Ray–Lance 12 API map, connector behavior, operational caveats, and a Ray Core distributed-index design |

Suggested reading order is the table order. Readers interested only in the
Ray–Lance boundary can start with
[05-ray-lance-integration.md](05-ray-lance-integration.md) and follow its links
back to the data-path and indexing details.

## Terminology

Several similarly named concepts belong to different systems:

| Term | Owner | Meaning |
|---|---|---|
| Ray object store | Ray Core | Per-node shared memory for immutable task/actor values; spillable and not a durable database |
| Object storage | External infrastructure | S3, GCS, Azure, or compatible durable storage holding files and Lance objects |
| Ray Data block | Ray Data | In-memory Arrow table or pandas DataFrame used as the unit of distributed processing |
| Lance fragment | Lance | Immutable logical row group referencing one or more durable Lance data files |
| Lance index segment | Lance | Immutable physical secondary-index payload with explicit fragment coverage |
| Ray task | Ray Core | Stateless remote function invocation |
| Ray actor | Ray Core | Stateful worker process whose methods execute remotely |

## Scope and non-goals

These notes explain implementation and integration behavior; they are not a
replacement for Ray's public user guide or Lance's format specification. In
particular:

- Ray is a compute runtime, not a durable table or vector database.
- Lance owns dataset manifests, versions, fragments, indexes, and query
  semantics.
- The built-in Ray Data Lance connector is designed to own parallel batch
  ingestion and extraction; its write calls need the documented Lance 12 API
  port for this target. It does not own distributed index construction or a
  global ANN result-reduction operator.
- Examples that orchestrate Lance index builds with Ray Core are an application
  design derived from both public APIs, not a new API already implemented in
  `ray.data`.
