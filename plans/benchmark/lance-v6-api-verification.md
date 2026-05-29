# Lance 6.0 Python API verification (sharded-benchmark prerequisites)

> **Note:** The Ray-owned `benchmarks/lance_hybrid_cache/` subtree
> referenced throughout this document has been removed from this
> repository. The distributed IVF cache actor / coordinator /
> example now lives in the `lance-ray` project under
> `lance_ray/distributed_cache/` and
> `examples/distributed_ivf_cache.py` (commits `17140fe..d0d09ab`).
> The verification commands below remain useful when bumping the
> pylance pin in that project; the actor-side `hasattr` gating
> described under *Decision* now lives in `lance-ray`, not in this
> tree.

This note records the verification of the Python (`pylance`) API surface
required by the v6 sharded-mode benchmark port described in
[`lance-distributed-cache-6.0.md`](./lance-distributed-cache-6.0.md).

## Lance build under verification

- Source tree: `../lance-open-source`
- Branch: `private-cache-6.0-ver-1`
- Diff range examined: `b3d546a64..9ebfe4de0`
- Pinned commit for the benchmark: **`9ebfe4de0`** (HEAD of the range at
  the time of the v6 plan write-up).
- Wheel expectation: no published wheel; consumers build in-place via
  `pip install -e ../lance-open-source/python` (maturin PEP 517/660, release
  profile by default).

## API surface results

Evidence comes from `python/python/lance/lance/__init__.pyi` on the pinned
commit, plus exhaustive greps of the entire `python/` subtree at the same
commit.

| Required API | Status | Evidence |
|---|---|---|
| `Session.with_distributed_cache(l2_dir, index_metadata_l1_capacity_bytes, *, moka_l1_partition_bytes=None) -> _Session` | **Present** | Declared in `__init__.pyi`; PyO3 bridge documented in the v6 plan ("v6 API surface used by this benchmark"). |
| `Session.size_bytes() -> int` | **Present** | Declared in `__init__.pyi`; sole surviving session-stats accessor (`index_cache_stats()` was removed). |
| `Dataset.prewarm_index(name, *, with_position=False, partition_ids=None)` | **Present** | Declared in `__init__.pyi`. Strict path raises `LanceError` on L2 write failure / mid-prewarm generation change. |
| `Dataset.compute_partition_ids(...)` | **MISSING from Python** | Rust-only on this commit: `rust/lance/src/index/vector/ivf/v2.rs`, `lance-index/src/vector.rs`, `rust/lance/src/io/exec/knn.rs`. Grep of the entire `python/` tree at `9ebfe4de0` returns zero hits. |
| `Dataset.search_partitions(...)` | **MISSING from Python** | Same as above. No PyO3 wrapper exists at `9ebfe4de0`. |

## Decision

**Ray side now actor-gated.** The original verification at commit
`9ebfe4de0` found both `compute_partition_ids` and `search_partitions`
Rust-only. The Ray-side port (issue #7) has since landed and made the
sharded methods (`HybridSearchActor.measure_sharded`,
`HybridSearchActor.search_partitions`, `CoordinatorActor.__init__`)
gate each Python API via ``hasattr``: a build that ships both runs
end-to-end, a build that does not raises a clear `RuntimeError` on
first use. The driver-level pre-block was lifted so the verified-API
contract from issue #7 is honoured without preventing operators from
running against newer pylance builds that already ship the wrappers.

Per the parent issue (#3) directive recorded on this child (#4):

> If `compute_partition_ids` or `search_partitions` are still missing,
> mark this child blocked and do not start the sharded-mode port unless
> a human explicitly accepts a replicated-only fallback.

The sharded-mode port is **started and actor-gated**; operators must
re-run the verification (below) against their pylance build before a
sharded run to confirm both APIs are bound.

## Unblock paths (choose one, human approval required)

1. **Wait for the Lance side.** File a Lance-repo issue to add PyO3
   wrappers on `_Dataset` matching the v4 fork's Python signatures:
   - `compute_partition_ids(name: str, query, nprobes: int) -> list[int]`
   - `search_partitions(name: str, query, partition_ids: list[int], k: int) -> RecordBatch`

   Each is mechanically small (wraps an existing Rust call). Once the
   patch lands and the benchmark's pinned commit is bumped, re-run this
   verification and unblock #4.

2. **Reduced-scope fallback (replicated-only).** Drop `--mode sharded`
   from the v6 benchmark; only support `--mode replicated`. Loses the
   partition-sharded full-recall coordinator topology -- which is the
   most interesting distributed-cache workload. Requires explicit human
   sign-off because it permanently narrows the benchmark's scope versus
   the v4 baseline.

## Re-verification command

When the Lance commit advances, re-run these greps against the new
Lance tree (substitute the new commit) and update the table above:

```bash
cd ../lance-open-source
git checkout <new-commit>
# Each should print at least one hit if the wrapper landed:
grep -rn 'compute_partition_ids' python/
grep -rn 'search_partitions'     python/
# Spot-check the type stub:
grep -n -E 'with_distributed_cache|size_bytes|prewarm_index|compute_partition_ids|search_partitions' \
  python/python/lance/lance/__init__.pyi
```

A non-empty result for both `compute_partition_ids` and
`search_partitions` confirms the actor-side `hasattr` gates will pass
at runtime; an empty result means the sharded codepaths will raise
`RuntimeError` on first call and the run must fall back to
`--mode replicated --prewarm forced`.
