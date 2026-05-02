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
        import lance
        from _hybrid_cache_helpers import build_session, minio_storage_options

        self._actor_id = actor_id
        self._nprobes = nprobes
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
            "stats_post_prewarm": dict(self._sess.index_cache_stats()),
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

        Calls ``prewarm_vector_cache`` with ``policy="normal"`` — the
        policy-aware path the deprecated ``dataset.prewarm_partitions``
        wrapper itself now delegates to. Going through the policy-aware
        API directly avoids the ``DeprecationWarning`` Lance emits from
        the wrapper.
        """
        if not hasattr(self._ds, "prewarm_vector_cache"):
            raise RuntimeError(
                "this pylance build does not expose dataset.prewarm_vector_cache; "
                "needs the deterministic vector cache prewarm primitives "
                "(lance commit 14f9e2862 or later); rebuild pylance"
            )
        t0 = time.time()
        # Dedupe to match the historical contract; the policy-aware path
        # rejects duplicate partition ids.
        unique_ids = list(dict.fromkeys(int(p) for p in partition_ids))
        self._ds.prewarm_vector_cache(
            index_name,
            unique_ids,
            policy="normal",
        )
        self._owned_partitions = set(unique_ids)
        self._sharded_index_name = index_name
        return {
            "actor_id": self._actor_id,
            "n_partitions": len(self._owned_partitions),
            "duration_s": time.time() - t0,
            "stats_post_prewarm": dict(self._sess.index_cache_stats()),
        }

    def prewarm_partitions_deterministic(
        self,
        index_name: str,
        partition_ids: List[int],
        policy: str,
        ram_bytes: int,
        wait_for_disk: bool = True,
    ) -> Dict[str, Any]:
        """Deterministic forced prewarm of an IVF partition slice.

        Replaces random-query warmup as the main prewarm mechanism for
        sharded runs. Wraps ``dataset.prewarm_vector_cache`` so the
        driver can pick a placement policy per scenario:

        * ``hybrid_tiered`` — place every requested partition into the
          foyer L2 (NVMe) tier and intentionally leave L1 cold.
          ``ram_bytes`` is accepted for source compatibility but ignored
          (no L1 admissions happen during hybrid prewarm). Ordinary
          query traffic later promotes decoded partitions out of L2
          into volatile L1; vector partition L1 entries evicted by
          subsequent queries are dropped from RAM and not written back
          to L2 (the L2 entry from prewarm already exists). Requires a
          hybrid Session.
        * ``moka_ram_cap`` — load partitions in order until ``ram_bytes``
          of decoded ``DeepSizeOf`` are resident, then stop. Avoids
          churning Moka past its capacity.
        * ``normal`` — existing behavior (every requested partition runs
          through the default cache insert).

        Returns the per-policy prewarm counters
        (``loaded_to_ram`` — always 0 under ``hybrid_tiered``,
        ``loaded_to_disk``, ``skipped_existing``,
        ``stopped_before``, ``ram_bytes_deep_size`` — always 0 under
        ``hybrid_tiered``, ``disk_bytes_serialized``,
        ``disk_bytes_unknown_spills`` — always 0 under ``hybrid_tiered``)
        so the driver can verify the cache filled in the expected
        shape.
        """
        if not hasattr(self._ds, "prewarm_vector_cache"):
            raise RuntimeError(
                "this pylance build does not expose dataset.prewarm_vector_cache; "
                "needs the deterministic vector cache prewarm primitives "
                "(lance commit 14f9e2862 or later); rebuild pylance"
            )
        t0 = time.time()
        prewarm_stats = self._ds.prewarm_vector_cache(
            index_name,
            partition_ids,
            policy=policy,
            ram_bytes=ram_bytes,
            wait_for_disk=wait_for_disk,
        )
        self._owned_partitions = {int(p) for p in partition_ids}
        self._sharded_index_name = index_name
        return {
            "actor_id": self._actor_id,
            "n_partitions": len(self._owned_partitions),
            "duration_s": time.time() - t0,
            "stats_post_prewarm": dict(self._sess.index_cache_stats()),
            "prewarm_stats": dict(prewarm_stats),
            "policy": policy,
            "ram_bytes_budget": int(ram_bytes),
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
            "stats_post_prewarm": dict(self._sess.index_cache_stats()),
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

    def warmup_natural(
        self, queries: List[List[float]], k: int = 10
    ) -> Dict[str, Any]:
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
            "stats_post": dict(self._sess.index_cache_stats()),
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
        if not hasattr(self._ds, "search_partitions"):
            raise RuntimeError(
                "this pylance build does not expose dataset.search_partitions; "
                "needs the partition-sharded IVF primitives "
                "(lance commit 4078a83b or later)"
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
            "stats_post": dict(self._sess.index_cache_stats()),
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
            "stats_post": dict(self._sess.index_cache_stats()),
            "n_searches_handled": int(self._n_searches_handled),
            "owned_partitions": len(self._owned_partitions),
        }

    def check_partition_residency(
        self,
        index_name: str,
        partition_ids: List[int] | None = None,
        l2_dir: str | None = None,
    ) -> Dict[str, Any]:
        """Probe owned partitions for L1 (and, when bound, L2) residency
        without side effects.

        L1 probe — ``IvfIndexSearcher::partition_is_cached`` is not yet
        bound in pylance, so we use ``prewarm_vector_cache`` with
        ``policy='moka_ram_cap', ram_bytes=0`` as a no-load equivalent:
        pass 1 counts every DRAM-resident partition in
        ``skipped_existing``, pass 2 short-circuits on
        ``ram_bytes_deep_size >= ram_bytes`` (i.e. ``0 >= 0``) and never
        loads anything. Single-partition probes therefore tell us, for
        each id, whether it is currently in the foyer L1 (DRAM) tier.

        L2 probe — ``partition_is_in_l2`` exists in the Rust IVF
        searcher but is not yet bound to Python. Without it the result
        defers L2 status to the actor's own bookkeeping: when the
        actor has just been hybrid-prewarmed with
        ``policy='hybrid_tiered', wait_for_disk=True`` every owned
        partition is on L2 by construction, so ``in_l2`` is filled
        with ``self._owned_partitions`` for hybrid sessions. The L2
        directory snapshot is included as a coarse cross-check (file
        count + on-disk bytes) — see ``l2_inspect.py`` for why this
        only catches *visible* growth and file-count churn, not silent
        block overwrites. When per-partition L2 residency is later
        bound this method should switch ``in_l2`` and ``missing`` to
        the bound probe; the field shape is stable today.

        Result fields:

        * ``in_l1`` — partition ids confirmed L1-resident.
        * ``not_in_l1`` — owned partitions not currently in L1 (would
          incur an L2 read on next query if hybrid, else MinIO).
        * ``in_l2`` — partitions known L2-resident. For hybrid sessions
          with a pylance build that lacks ``partition_is_in_l2`` this
          defaults to every owned partition (post-prewarm assumption);
          for non-hybrid actors it is empty.
        * ``missing`` — partitions neither L1- nor L2-resident. Without
          a bound L2 probe this is always empty for hybrid actors
          (rely on the L2 directory snapshot + prewarm validation to
          catch missing L2 placement). For no-L2 sessions (Moka /
          no-cache) anything not in L1 is missing from cache entirely,
          so this equals ``not_in_l1``.
        * ``l2_probe_supported`` — false until the Python binding
          lands, so downstream tooling can demote ``in_l2`` /
          ``missing`` to inferred-only values.
        * ``l2_residency_source`` — how ``in_l2`` / ``missing`` were
          derived. Today this is ``prewarm_validated_owned_set`` for
          hybrid actors (prewarm counters validate ownership, the L2 dir
          snapshot cross-checks filesystem footprint) or ``no_l2_tier``
          for Moka / no-cache actors. Switch to ``index_probe`` when a
          pylance L2 binding lands.

        Backwards compatibility: ``in_ram`` / ``not_in_ram`` are kept as
        aliases for ``in_l1`` / ``not_in_l1`` for callers that still
        consume the old field names. Remove once no caller references
        them.

        Parameters
        ----------
        index_name : str
            The IVF index name whose partitions are probed.
        partition_ids : list of int, optional
            Partitions to probe. Defaults to ``self._owned_partitions``
            (set by ``prewarm_partitions_deterministic`` /
            ``set_owned_partitions``). The probe is no-side-effect on
            DRAM residency but still iterates per id, so callers can
            restrict the probe range when ownership is large.
        l2_dir : str, optional
            Actor-local L2 directory to snapshot. When omitted the L2
            footprint section of the result is empty (Moka actors have
            no L2 dir).
        """
        if partition_ids is None:
            partition_ids = sorted(self._owned_partitions)
        else:
            partition_ids = [int(p) for p in partition_ids]

        in_l1: List[int] = []
        not_in_l1: List[int] = []
        t0 = time.time()
        for p in partition_ids:
            stats = self._ds.prewarm_vector_cache(
                index_name,
                [int(p)],
                policy="moka_ram_cap",
                ram_bytes=0,
                wait_for_disk=False,
            )
            if int(stats.get("skipped_existing", 0)) >= 1:
                in_l1.append(int(p))
            else:
                not_in_l1.append(int(p))

        # Per-partition L2 probe is not bound to Python yet. For hybrid
        # actors, assume every owned partition is L2-resident — the
        # post-prewarm L2 directory snapshot and the prewarm-time
        # loaded_to_disk + skipped_existing validation in the driver
        # are the real disk-placement check. Moka / no-cache actors
        # have no L2 tier, so anything not in L1 is missing from cache
        # entirely (e.g. partitions dropped once moka_ram_cap fills).
        l2_probe_supported = False
        if l2_dir is not None:
            in_l2 = [int(p) for p in partition_ids]
            missing: List[int] = []
            l2_residency_source = "prewarm_validated_owned_set"
        else:
            in_l2 = []
            missing = list(not_in_l1)
            l2_residency_source = "no_l2_tier"

        result: Dict[str, Any] = {
            "actor_id": self._actor_id,
            "index_name": index_name,
            "n_probed": len(partition_ids),
            "in_l1": in_l1,
            "not_in_l1": not_in_l1,
            "in_l2": in_l2,
            "missing": missing,
            "l2_probe_supported": l2_probe_supported,
            "l2_residency_source": l2_residency_source,
            # Aliases retained for backward compatibility with older
            # callers; remove once everything reads in_l1/not_in_l1.
            "in_ram": in_l1,
            "not_in_ram": not_in_l1,
            "session_stats": dict(self._sess.index_cache_stats()),
            "probe_duration_s": time.time() - t0,
        }
        if l2_dir is not None:
            from l2_inspect import snapshot_l2_dir as _snapshot

            snap = _snapshot(l2_dir)
            snap.pop("files", None)
            result["l2_dir"] = snap
        return result

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
                non_empty = [
                    (i, ids) for i, ids in enumerate(buckets) if ids
                ]
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
                n_owned_routed.append(
                    sum(len(ids) for _, ids in non_empty)
                )

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
