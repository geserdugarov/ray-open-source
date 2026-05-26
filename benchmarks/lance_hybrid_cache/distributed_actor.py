"""Per-shard Ray actor for the distributed Lance hybrid-cache bench.

Each actor owns one Lance Session (and therefore one cache, with its
own L2 subdirectory under the shared NVMe parent). The driver picks
how queries are routed across actors; the actor itself is dumb and
runs whatever it's handed.

Why a distributed actor instead of `ScenarioActor`:

* We need a per-actor `prewarm_index` entrypoint that drives Lance's
  forced prewarm (`dataset.prewarm_index(name)`), which `ScenarioActor`
  does not expose.
* We split queries across actors at the driver, so the actor takes
  query slices (lists), not a full warmup/measure pair.
* `ScenarioActor.run` is a single-shot end-to-end method; this actor
  is split into prewarm/warmup/measure/close so the driver can time
  each phase independently.

This module also defines :class:`CoordinatorActor` for the partition-
sharded full-recall topology: one coordinator owns the IVF centroid
step, scatters per-query partition lists to the workers that own each
shard via ``search_partitions``, and merges per-worker partial top-K
into a global top-K.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple

import numpy as np
import ray


def _merge_top_k(
    partials: List[Tuple[np.ndarray, np.ndarray]], k: int
) -> Tuple[np.ndarray, np.ndarray]:
    """Merge per-worker (distances, row_ids) into a global top-K.

    Each ``partials[i]`` is ``(distances_f32, row_ids_u64)`` for one
    worker's owned slice. Returns the k smallest distances overall and
    their row ids, sorted ascending. Empty input yields empty arrays.
    """
    if not partials:
        return (
            np.empty(0, dtype=np.float32),
            np.empty(0, dtype=np.uint64),
        )
    dist = np.concatenate([p[0] for p in partials])
    rows = np.concatenate([p[1] for p in partials])
    if dist.size == 0:
        return dist, rows
    if dist.size <= k:
        order = np.argsort(dist, kind="stable")
    else:
        # argpartition to k, then sort the k smallest.
        idx = np.argpartition(dist, k - 1)[:k]
        order = idx[np.argsort(dist[idx], kind="stable")]
    return dist[order], rows[order]


@ray.remote
class HybridSearchActor:
    """One shard of a distributed Lance read workload."""

    def __init__(
        self,
        actor_id: int,
        spec: Dict[str, Any],
        uri: str,
        endpoint_url: str,
        nprobes: int,
    ):
        # Imports live in __init__ because each actor runs in its own
        # Python process; the driver's import of these modules does not
        # transfer. ray.init(runtime_env={"working_dir": ...}) ships this
        # benchmark directory to the worker so _hybrid_cache_helpers resolves.
        import os as _os

        import lance
        from _hybrid_cache_helpers import (
            build_session,
            minio_storage_options,
            size_bytes_stats,
        )

        # Keep the helper reachable from non-__init__ methods.
        self._size_bytes_stats = size_bytes_stats

        self._actor_id = actor_id
        self._nprobes = nprobes
        # The v6 distributed-cache session takes an exclusive lock on
        # `{l2_dir}/lance-distributed.lock` and rejects a missing dir.
        # Create the per-actor dir in-process — driver-side mkdir would
        # only see the head-node filesystem in a multi-node cluster.
        # Stashed on the actor so the sharded-prewarm L2 file-count
        # validation can walk it without the driver re-sending the path.
        self._l2_dir: str | None = None
        if spec.get("kind") == "distributed":
            self._l2_dir = str(spec["l2_dir"])
            _os.makedirs(self._l2_dir, exist_ok=True)
        self._sess = build_session(spec)
        self._ds = lance.dataset(
            uri,
            session=self._sess,
            storage_options=minio_storage_options(endpoint_url),
        )
        # Set by prewarm_partitions and consumed by measure_sharded so the
        # sharded measure path knows which partitions this replica owns
        # without the driver re-sending them per query.
        self._owned_partitions: set[int] = set()
        self._sharded_index_name: str | None = None
        # Counts coord-driven search_partitions calls so the bench can
        # report fan-out balance per actor without timing them inside
        # the worker (the coord owns per-query latency).
        self._n_searches_handled: int = 0

    def _validate_l2_partition_files(self, expected_ids: List[int]) -> Dict[str, Any]:
        """Walk the actor-local L2 dir to verify sharded prewarm placement.

        v6's ``dataset.prewarm_index(name, partition_ids=...)`` returns
        ``None`` and either succeeds for every requested partition or
        raises ``LanceError``; the on-disk ``part-ivf-{id}.bin`` files
        under ``{l2_dir}/v1/{sanitize(prefix)}/`` are the only
        Python-visible placement signal. A mismatch here flags a
        backend regression, a stale-prefix collision (two live
        ``v1/{prefix}/`` subdirs), or a write that silently dropped
        despite the strict path.

        Returns ``{}`` for non-distributed sessions (no L2 tier to
        walk). For distributed sessions the dict carries the observed
        file count, on-disk apparent bytes, the count of partitions
        missing from disk (``missing_count``), unexpected partitions
        found (``extra_count``), and capped previews of the missing /
        extra id sets so a non-zero count is investigable from the
        driver log without a follow-up RPC.
        """
        if self._l2_dir is None:
            return {}
        # Local import: keeps the helper module out of the actor's
        # eager-import path for moka / no-cache scenarios that don't
        # touch the L2 tier.
        from check_l2_residency import walk_l2_partition_ids

        found_ids, file_count, apparent_bytes, prefix_dirs = walk_l2_partition_ids(
            self._l2_dir
        )
        found_set = set(found_ids)
        expected_set = {int(p) for p in expected_ids}
        missing = sorted(expected_set - found_set)
        extra = sorted(found_set - expected_set)
        return {
            "l2_file_count": int(file_count),
            "l2_apparent_bytes": int(apparent_bytes),
            "expected_count": len(expected_set),
            "missing_count": len(missing),
            "extra_count": len(extra),
            # Cap the printed sets — full lists clutter the ray log on
            # large indexes (3000 partitions per actor is common). The
            # full diff can be reconstructed from a snapshot_l2_dir
            # call if a non-zero count fires.
            "missing": missing[:32],
            "extra": extra[:32],
            "l2_prefix_dirs": prefix_dirs,
        }

    def prewarm_index(self, index_name: str) -> Dict[str, Any]:
        """Force every partition of the named index into the cache.

        With a hybrid Session this places every vector partition into
        the foyer L2 (NVMe) tier; subsequent queries hit L2 and decoded
        partitions are promoted into volatile L1 on first read. Vector
        partition L1 is intentionally left cold by the underlying
        no-writeback policy — there is no L1→L2 eviction path for
        vector partitions, so query-driven L1 churn does not produce
        further L2 writes. Returns post-prewarm stats so the driver
        can verify the cache actually filled.
        """
        if not hasattr(self._ds, "prewarm_index"):
            raise RuntimeError(
                "this pylance build does not expose dataset.prewarm_index; "
                "use --prewarm natural instead"
            )
        t0 = time.time()
        self._ds.prewarm_index(index_name)
        return {
            "actor_id": self._actor_id,
            "duration_s": time.time() - t0,
            "stats_post_prewarm": self._size_bytes_stats(self._sess),
        }

    def prewarm_partitions(
        self, index_name: str, partition_ids: List[int]
    ) -> Dict[str, Any]:
        """Force only this actor's slice of partitions into the cache.

        Counterpart to ``prewarm_index`` for partition-sharded topologies.
        Driver assigns each actor a disjoint subset of partition ids
        (e.g. ``partition_id % num_actors``); the actor stashes them so
        ``measure_sharded`` can intersect routed partitions against its
        own slice without the driver re-sending them per query.

        Lance 6.0 port: calls ``dataset.prewarm_index(name,
        partition_ids=...)`` — the strict v6 path that writes one
        ``part-ivf-<id>.bin`` per partition under ``{l2_dir}/v1/...``
        and raises ``LanceError`` on any L2 write failure / mid-prewarm
        generation change. The v4 ``policy='normal'`` knob is gone in
        v6; the distributed cache controls placement itself.

        Returns an ``l2_validation`` block (``{}`` for non-distributed
        sessions) that walks the actor-local L2 dir post-prewarm and
        reports ``l2_file_count`` / ``missing_count`` / ``extra_count``
        against the expected partition set. The driver should hard-fail
        on ``missing_count != 0`` — the v6 strict path either persisted
        every requested partition or raised ``LanceError``, so a
        survivor with missing files is a backend regression.
        """
        if not hasattr(self._ds, "prewarm_index"):
            raise RuntimeError(
                "this pylance build does not expose dataset.prewarm_index; "
                "needs the Lance 6.0 distributed-cache prewarm primitive"
            )
        t0 = time.time()
        # Dedupe to keep behavior stable; the v6 strict path tolerates
        # duplicates but a unique list keeps the L2 file-count check
        # cleaner downstream.
        unique_ids = list(dict.fromkeys(int(p) for p in partition_ids))
        self._ds.prewarm_index(index_name, partition_ids=unique_ids)
        self._owned_partitions = set(unique_ids)
        self._sharded_index_name = index_name
        return {
            "actor_id": self._actor_id,
            "n_partitions": len(self._owned_partitions),
            "duration_s": time.time() - t0,
            "stats_post_prewarm": self._size_bytes_stats(self._sess),
            "l2_validation": self._validate_l2_partition_files(unique_ids),
        }

    def prewarm_partitions_deterministic(
        self,
        index_name: str,
        partition_ids: List[int],
        policy: str = "",
        ram_bytes: int = 0,
        wait_for_disk: bool = True,
    ) -> Dict[str, Any]:
        """Deterministic forced prewarm of an IVF partition slice (Lance 6.0).

        Ports the v4 ``dataset.prewarm_vector_cache(name, ids, policy=...)``
        call site to ``dataset.prewarm_index(name, partition_ids=ids)`` —
        the v6 strict path that writes one ``part-ivf-<id>.bin`` per
        partition under ``{l2_dir}/v1/...``, raises ``LanceError`` on
        any L2 write failure / mid-prewarm generation change, and
        produces no Python-visible counters.

        The v4 ``policy`` / ``ram_bytes`` / ``wait_for_disk`` arguments
        are accepted for caller signature compatibility but ignored
        under v6: the distributed cache controls placement itself, has
        no L1 capacity-bookkeeping knob, and persists each partition
        atomically (rename + fsync) so there is no separate
        "wait for disk" gate. The returned ``prewarm_stats`` dict is
        empty (no v6 counter binding); the ``l2_validation`` block
        (populated for distributed sessions only) is the v6 analog of
        the v4 counter check: it walks the actor-local L2 dir and
        verifies that ``part-ivf-{id}.bin`` exists for every requested
        partition. ``missing_count != 0`` is a hard-fail signal for the
        driver. Non-distributed sessions return an empty dict (no L2
        tier).
        """
        if not hasattr(self._ds, "prewarm_index"):
            raise RuntimeError(
                "this pylance build does not expose dataset.prewarm_index; "
                "needs the Lance 6.0 distributed-cache prewarm primitive"
            )
        t0 = time.time()
        unique_ids = list(dict.fromkeys(int(p) for p in partition_ids))
        self._ds.prewarm_index(index_name, partition_ids=unique_ids)
        self._owned_partitions = set(unique_ids)
        self._sharded_index_name = index_name
        return {
            "actor_id": self._actor_id,
            "n_partitions": len(self._owned_partitions),
            "duration_s": time.time() - t0,
            "stats_post_prewarm": self._size_bytes_stats(self._sess),
            "prewarm_stats": {},
            "policy": policy,
            "ram_bytes_budget": int(ram_bytes),
            "l2_validation": self._validate_l2_partition_files(unique_ids),
        }

    def set_owned_partitions(
        self, index_name: str, partition_ids: List[int]
    ) -> Dict[str, Any]:
        """Record an owned-partition slice without any cache I/O.

        Lets the no-cache scenario participate in sharded routing
        topologies — the coord still needs ``measure_sharded`` and
        ``search_partitions`` to know which slice this actor owns, but
        forcing partitions through a no-op cache only burns MinIO
        traffic. Returns a stats-shaped dict for log uniformity with
        :meth:`prewarm_partitions_deterministic`.
        """
        self._owned_partitions = {int(p) for p in partition_ids}
        self._sharded_index_name = index_name
        return {
            "actor_id": self._actor_id,
            "n_partitions": len(self._owned_partitions),
            "duration_s": 0.0,
            "stats_post_prewarm": self._size_bytes_stats(self._sess),
        }

    def snapshot_l2_dir(self, l2_dir: str) -> Dict[str, Any]:
        """Return the actor-local L2 directory footprint.

        Polled by the driver after deterministic hybrid prewarm to
        report how much foyer wrote to NVMe per actor. Lives on the
        actor process because in real-cluster mode the L2 directory
        is on the actor node's local disk, unreachable from the driver
        node. Drops the per-file list — the driver only needs totals.
        """
        from l2_inspect import snapshot_l2_dir as _snapshot

        snap = _snapshot(l2_dir)
        snap.pop("files", None)
        snap["actor_id"] = self._actor_id
        return snap

    def check_l2_residency(
        self,
        l2_dir: "str | None",
        owned_partitions: List[int],
        label: str = "residency",
    ) -> Dict[str, Any]:
        """Walk the actor-local L2 dir and report per-actor residency.

        Runs on the actor process so the directory walk hits the actor
        node's local NVMe — driver-side walks would see an empty path
        in any real-cluster topology where the L2 dirs are not on the
        head node's filesystem. Returns the v6 aggregate-only schema
        defined in ``check_l2_residency.compute_l2_residency`` (see
        that module's docstring for the per-row field list).

        ``l2_dir`` may be ``None`` for non-hybrid scenarios; the row
        then carries zero L2 bytes / files and every owned partition
        lands in ``missing``. ``l1_size_bytes_at_probe`` is filled from
        ``Session.size_bytes()`` regardless.
        """
        from check_l2_residency import compute_l2_residency

        l1_size = int(self._size_bytes_stats(self._sess).get("size_bytes", -1))
        return compute_l2_residency(
            actor_id=self._actor_id,
            label=label,
            owned_partitions=owned_partitions,
            l2_dir=l2_dir,
            l1_size_bytes=l1_size,
        )

    def warmup_natural(self, queries: List[List[float]], k: int = 10) -> Dict[str, Any]:
        """Run a slice of representative queries to populate the cache.

        Equivalent to `_hybrid_cache_helpers.warmup` but takes
        a pre-sliced query list so the driver controls the partition.
        """
        from _hybrid_cache_helpers import run_query

        t0 = time.time()
        for q in queries:
            run_query(
                self._ds,
                np.asarray(q, dtype=np.float32),
                k=k,
                nprobes=self._nprobes,
            )
        return {
            "actor_id": self._actor_id,
            "n_queries": len(queries),
            "duration_s": time.time() - t0,
        }

    def measure(
        self,
        queries: List[List[float]],
        k_list: List[int],
    ) -> Dict[str, Any]:
        """Run measure queries serially. Same shape as measure()."""
        from _hybrid_cache_helpers import run_query

        results: Dict[int, List[float]] = {k: [] for k in k_list}
        t0 = time.time()
        for q in queries:
            qa = np.asarray(q, dtype=np.float32)
            for k in k_list:
                lat = run_query(self._ds, qa, k=k, nprobes=self._nprobes)
                results[k].append(lat)
        return {
            "actor_id": self._actor_id,
            "latencies_by_k": {int(k): v for k, v in results.items()},
            "stats_post": self._size_bytes_stats(self._sess),
            "duration_s": time.time() - t0,
            "n_queries": len(queries),
        }

    def measure_sharded(
        self,
        queries: List[List[float]],
        k_list: List[int],
    ) -> Dict[str, Any]:
        """Measure latencies on a partition-sharded actor.

        Per query: route via ``compute_partition_ids`` (centroid-only),
        intersect with the partition slice this actor was prewarmed for,
        then call ``search_partitions`` over the intersection. The result
        is a partial top-K — only this actor's slice of the routed
        partitions is searched, no cross-actor merge happens here. To
        recover full recall a coordinator would need to merge top-K
        across actors per query; the bench reports per-actor latency to
        keep the comparison apples-to-apples on per-actor work.

        Requires ``prewarm_partitions`` to have run first (sets the
        owned-partition slice). Errors otherwise rather than silently
        scanning everything.
        """
        if self._sharded_index_name is None:
            raise RuntimeError(
                "measure_sharded called before prewarm_partitions; "
                "the actor has no owned-partition slice to search"
            )
        # Both APIs are required: routing calls compute_partition_ids
        # (centroid-only) and the per-query partial search calls
        # search_partitions. Gate them together so a pylance build that
        # ships only one of the two fails before the measure loop starts,
        # rather than partway through with a misleading AttributeError on
        # the second call.
        missing_apis = [
            name
            for name in ("compute_partition_ids", "search_partitions")
            if not hasattr(self._ds, name)
        ]
        if missing_apis:
            raise RuntimeError(
                "this pylance build does not expose dataset."
                f"{' / dataset.'.join(missing_apis)}; needs the partition-"
                "sharded IVF primitives (lance commit 4078a83b or later)"
            )

        results: Dict[int, List[float]] = {k: [] for k in k_list}
        n_owned_routed: List[int] = []
        t0 = time.time()
        for q in queries:
            qa = np.asarray(q, dtype=np.float32)
            routed = self._ds.compute_partition_ids(
                self._sharded_index_name, qa, self._nprobes
            )
            owned_routed = [int(p) for p in routed if int(p) in self._owned_partitions]
            n_owned_routed.append(len(owned_routed))
            for k in k_list:
                t1 = time.perf_counter()
                self._ds.search_partitions(
                    self._sharded_index_name, qa, owned_routed, k
                )
                results[k].append(time.perf_counter() - t1)
        return {
            "actor_id": self._actor_id,
            "latencies_by_k": {int(k): v for k, v in results.items()},
            "stats_post": self._size_bytes_stats(self._sess),
            "duration_s": time.time() - t0,
            "n_queries": len(queries),
            "owned_partitions": len(self._owned_partitions),
            "mean_owned_routed_per_query": (
                float(np.mean(n_owned_routed)) if n_owned_routed else 0.0
            ),
        }

    def search_partitions(
        self,
        query: List[float],
        partition_ids: List[int],
        k: int,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Run a partial top-K search over the supplied partition slice.

        Coordinator-driven counterpart to ``measure_sharded``: the coord
        has already done the centroid step, has already filtered the
        nprobes ids by ownership, and just wants this replica's partial
        result so it can merge across workers. Returns the per-partition
        candidates as numpy arrays so the coord's merge is a cheap
        ``np.argpartition`` over numpy concatenation rather than a
        cross-process pyarrow round-trip.

        Requires :meth:`prewarm_partitions` to have run first. Errors
        otherwise rather than silently scanning empty cache.
        """
        if self._sharded_index_name is None:
            raise RuntimeError(
                "search_partitions called before prewarm_partitions; "
                "the actor has no sharded index registered"
            )
        if not hasattr(self._ds, "search_partitions"):
            raise RuntimeError(
                "this pylance build does not expose dataset.search_partitions; "
                "needs the partition-sharded IVF primitives "
                "(lance commit 4078a83b or later)"
            )
        if not partition_ids:
            return (
                np.empty(0, dtype=np.float32),
                np.empty(0, dtype=np.uint64),
            )
        qa = np.asarray(query, dtype=np.float32)
        rb = self._ds.search_partitions(
            self._sharded_index_name, qa, list(partition_ids), k
        )
        self._n_searches_handled += 1
        # Materialise into numpy now so the over-the-wire payload to the
        # coord is two flat arrays, not an arrow batch.
        distances = np.asarray(rb.column("_distance"), dtype=np.float32)
        row_ids = np.asarray(rb.column("_rowid"), dtype=np.uint64)
        return distances, row_ids

    def cache_stats(self) -> Dict[str, Any]:
        """Return current cache stats and the worker's coord-driven counter.

        Used in coordinator mode to gather per-actor hit/miss rates
        post-measure (the coord owns per-query timing; per-actor
        stats are only available by polling the worker).
        """
        return {
            "actor_id": self._actor_id,
            "stats_post": self._size_bytes_stats(self._sess),
            "n_searches_handled": int(self._n_searches_handled),
            "owned_partitions": len(self._owned_partitions),
        }

    def close(self) -> None:
        """Release session resources and the L2 directory flock.

        Foyer's NVMe L2 region files are held under an exclusive
        ``{l2_dir}/lance-hybrid.lock``; close drops it so a subsequent
        bench run on the same ``l2_dir`` can attach. Vector partition
        durability does not depend on close — under the
        no-vector-L1-writeback policy partitions are written to L2 at
        deterministic prewarm time (``wait_for_disk=True`` blocks until
        foyer's storage flusher confirms the write) and never flow back
        from L1 to L2 during normal query traffic. Idempotent for
        default (Moka) sessions.
        """
        self._sess.close()


@ray.remote
class CoordinatorActor:
    """Routing actor for the partition-sharded full-recall topology.

    Owns the IVF centroid step (cheap, metadata-only) and the
    partition→actor mapping. Per query it groups the nprobes routed
    partitions by owning worker, scatter-gathers ``search_partitions``
    in parallel across workers, and merges per-worker partial top-K
    into a final top-K. Workers must already be alive and registered
    by name (``hybrid-search-actor-<i>``) when the coordinator
    initialises.

    The coordinator never loads partition data and never uses an L2
    NVMe tier — it only opens the dataset for the centroid metadata,
    which lives in the small per-Session metadata cache.
    """

    def __init__(
        self,
        dataset_uri: str,
        endpoint_url: str,
        index_name: str,
        nprobes: int,
        num_actors: int,
        worker_names: List[str],
        metadata_bytes: int | None = None,
    ):
        import lance
        from _hybrid_cache_helpers import minio_storage_options

        if len(worker_names) != num_actors:
            raise ValueError(
                f"worker_names ({len(worker_names)}) does not match "
                f"num_actors ({num_actors})"
            )

        self._index_name = index_name
        self._nprobes = nprobes
        self._num_actors = num_actors

        # DRAM-only Session: routing only needs the centroid metadata.
        # Sized small so the coord never contends with workers for NVMe.
        sess_kwargs: Dict[str, Any] = {"index_cache_size_bytes": 64 * 1024 * 1024}
        if metadata_bytes is not None:
            sess_kwargs["metadata_cache_size_bytes"] = int(metadata_bytes)
        self._sess = lance.Session(**sess_kwargs)
        self._ds = lance.dataset(
            dataset_uri,
            session=self._sess,
            storage_options=minio_storage_options(endpoint_url),
        )
        if not hasattr(self._ds, "compute_partition_ids"):
            raise RuntimeError(
                "this pylance build does not expose dataset.compute_partition_ids; "
                "needs the partition-sharded IVF primitives "
                "(lance commit 4078a83b or later)"
            )
        # Resolve worker handles by name. Workers must already be alive
        # — the driver creates them before the coordinator.
        self._workers = [ray.get_actor(name) for name in worker_names]

    def ready(self) -> bool:
        # Readiness barrier for the driver: actor creation is async, so
        # ray.get(coord.ready.remote()) is the only way to confirm
        # __init__ (which opens the dataset over MinIO) has finished
        # before the measure timer starts.
        return True

    def warmup_routing(self, sample_query: List[float]) -> Dict[str, Any]:
        """Force-open the top-level vector index and load IVF centroids.

        ``__init__`` opens the dataset but does not open the vector
        index — the first ``compute_partition_ids`` call lazily reads
        the index file, deserialises the IVF model, and inserts the
        top-level vector index object into the coordinator's metadata
        cache. Run that once on a throwaway query so the first measured
        query in ``search_batch`` does not pay the index-open cost.
        """
        qa = np.asarray(sample_query, dtype=np.float32)
        t0 = time.time()
        self._ds.compute_partition_ids(self._index_name, qa, self._nprobes)
        return {"duration_s": time.time() - t0}

    def search_batch(
        self,
        queries: List[List[float]],
        k_list: List[int],
    ) -> Dict[str, Any]:
        """Run a batch of full-recall sharded searches.

        For each ``(query, k)`` pair: centroid-route to ``nprobes``
        partition ids, group by ``id % num_actors``, fan out partial
        searches to non-empty buckets only (skips one RPC per zero-bucket
        actor), and merge to top-K. Returns aggregated per-k latency
        lists matching the shape of :meth:`HybridSearchActor.measure`,
        plus coord-side instrumentation (centroid overhead, fan-out
        breadth) so the bench can break down where wall-time goes.
        """
        results: Dict[int, List[float]] = {k: [] for k in k_list}
        centroid_latencies: List[float] = []
        scatter_latencies: List[float] = []
        merge_latencies: List[float] = []
        n_workers_invoked: List[int] = []
        n_owned_routed: List[int] = []

        t_start = time.time()
        for q in queries:
            qa = np.asarray(q, dtype=np.float32)
            for k in k_list:
                t0 = time.perf_counter()
                # 1. Centroid-only routing (no I/O beyond metadata cache).
                partition_ids = self._ds.compute_partition_ids(
                    self._index_name, qa, self._nprobes
                )
                buckets: List[List[int]] = [[] for _ in range(self._num_actors)]
                for pid in partition_ids:
                    buckets[int(pid) % self._num_actors].append(int(pid))
                t_centroid = time.perf_counter() - t0

                # 2. Scatter to non-empty buckets in parallel.
                t1 = time.perf_counter()
                non_empty = [(i, ids) for i, ids in enumerate(buckets) if ids]
                futures = [
                    self._workers[i].search_partitions.remote(qa, ids, k)
                    for i, ids in non_empty
                ]
                partials = ray.get(futures) if futures else []
                t_scatter = time.perf_counter() - t1

                # 3. Merge partial top-Ks → global top-K.
                t2 = time.perf_counter()
                _merge_top_k(partials, k)
                t_merge = time.perf_counter() - t2

                results[k].append(t_centroid + t_scatter + t_merge)
                centroid_latencies.append(t_centroid)
                scatter_latencies.append(t_scatter)
                merge_latencies.append(t_merge)
                n_workers_invoked.append(len(non_empty))
                n_owned_routed.append(sum(len(ids) for _, ids in non_empty))

        return {
            "latencies_by_k": {int(k): v for k, v in results.items()},
            "duration_s": time.time() - t_start,
            "n_queries": len(queries),
            "centroid_s_mean": (
                float(np.mean(centroid_latencies)) if centroid_latencies else 0.0
            ),
            "scatter_s_mean": (
                float(np.mean(scatter_latencies)) if scatter_latencies else 0.0
            ),
            "merge_s_mean": (
                float(np.mean(merge_latencies)) if merge_latencies else 0.0
            ),
            "mean_workers_invoked_per_query": (
                float(np.mean(n_workers_invoked)) if n_workers_invoked else 0.0
            ),
            "mean_routed_partitions_per_query": (
                float(np.mean(n_owned_routed)) if n_owned_routed else 0.0
            ),
        }
