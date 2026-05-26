"""Verify per-actor IVF partition L2 residency via the Lance v6 layout.

Doubles as a library (called by ``run_distributed_bench.py`` after the
prewarm phase and again after the measure phase) and a standalone tool
that can attach to a live Ray cluster whose ``hybrid-search-actor-<i>``
actors are still alive.

What this checks
----------------

Lance 6.0's distributed cache writes one ``part-ivf-{id}.bin`` per
prewarmed partition under ``{l2_dir}/v1/{sanitize(prefix)}/``. File
presence one-to-one maps to L2 residency, so a directory walk is an
*exact* probe -- the v4 ``partition_is_in_l2`` ambiguity goes away. The
report compares the on-disk partition ids against the actor's expected
owned slice and lists the difference.

The v4 per-partition L1 probe
(``prewarm_vector_cache(name, [pid], policy='moka_ram_cap', ram_bytes=0)``
returning ``skipped_existing == 1``) has no v6 equivalent: the strict
``prewarm_index`` path has no no-load shortcut. The aggregate-only
replacement records ``l1_size_bytes_at_probe`` from
``Session.size_bytes()`` instead -- a coarse "how full is L1" readout
rather than per-partition L1 identity. The v4 inferred ``in_ram`` /
``not_in_ram`` aliases are dropped (they were inference-only anyway).

Output shape
------------

The library returns one dict per actor with the schema also written to
``partition_residency.jsonl``::

    {
      "actor_id": 0,
      "label": "post-prewarm" | "post-measure",
      "owned_count": 1500,
      "in_l2": [0, 2, 4, ...],
      "missing": [],
      "l2_size_bytes_total": 5234567890,
      "l2_file_count": 1500,
      "l1_size_bytes_at_probe": 0,
      "probe_duration_s": 0.05
    }

The standalone CLI prints a per-actor summary to stdout and (when
``--out`` is set) appends one JSON line per actor for downstream
analysis.

Usage as a library
------------------

``run_l2_residency_check(actors, partitions_for_actor, nvme_dir,
label="...", log=print)``

* ``actors`` -- list of ``HybridSearchActor`` handles (or ``None`` to
  skip ``l1_size_bytes_at_probe`` collection, e.g. when the cluster
  is no longer alive).
* ``partitions_for_actor`` -- parallel list, ``partitions_for_actor[i]``
  is the set of partition ids actor ``i`` was prewarmed for.
* ``nvme_dir`` -- parent of the per-actor L2 subdirs; each actor reads
  ``<nvme_dir>/actor-<i>``. Pass ``None`` for non-hybrid scenarios
  (the residency row then reports zero L2 files / bytes).
* ``label`` -- short string ("post-prewarm" / "post-measure") embedded
  in each row and used in the printed header so a single bench log can
  carry both checks.

Standalone usage
----------------

When the bench's actors are still alive, attach to the cluster and run
the same check from another shell::

    python check_l2_residency.py \\
        --num-actors 2 --num-partitions 3000 \\
        --nvme-dir /mnt/nvme/lance-l2/distributed \\
        --label adhoc

The actor names follow ``run_distributed_bench.py``'s convention
(``hybrid-search-actor-<i>``); override with ``--actor-name-prefix`` if
your driver renamed them. Pass ``--no-attach`` to skip Ray and report
only the on-disk inventory (``l1_size_bytes_at_probe`` will be ``-1``).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from l2_inspect import (
    V1_SUBDIR,
    format_bytes,
    parse_partition_id,
)

# ``ray`` is imported lazily inside the attach paths (the actor-RPC
# branch of ``run_l2_residency_check`` and the CLI's non-``--no-attach``
# mode). Keeping it out of module load lets ``--help`` and ``--no-attach``
# work in environments without ray installed.

HERE = Path(__file__).resolve().parent

# Shared Ray namespace for the bench actors. Ray uses per-job anonymous
# namespaces by default, so the driver and any external attach process
# would land in different namespaces and ``ray.get_actor()`` from the
# attach side would not see the driver's named actors. Pinning both to
# the same constant restores the documented "attach from another shell"
# workflow. ``run_distributed_bench.py`` imports this so the two stay in
# sync; the standalone CLI exposes ``--namespace`` for the rare case
# where a user renames it.
DEFAULT_NAMESPACE = "lance-hybrid-bench"

# Sentinel: ``l1_size_bytes_at_probe`` is unknown (no live session to
# query, e.g. CLI run with --no-attach or after the cluster has been
# torn down). Distinct from 0, which is a valid empty-cache size.
L1_SIZE_UNKNOWN = -1


def walk_l2_partition_ids(
    l2_dir: str,
    *,
    active_prefix: Optional[str] = None,
) -> Tuple[List[int], int, int, List[str]]:
    """Walk ``{l2_dir}/v1/{prefix}/part-ivf-{id}.bin`` for the active prefix.

    Returns ``(sorted_unique_partition_ids, file_count, apparent_bytes_total,
    live_prefix_dirs)`` where ``live_prefix_dirs`` is the sorted list of
    non-deleting subdirectory names found under ``v1/`` (useful for
    diagnosis when more than one is present).

    The v6 layout is prefix-scoped: ``v1/{sanitize(dataset_uri,
    index_name)}/`` -- one subdir per ``(dataset, index)`` pair. The
    residency probe must answer "what's on disk for *this* dataset+index",
    not the union across stale or unrelated prefixes (which would mask
    an empty current prefix when an old prefix happens to hold matching
    partition ids).

    Prefix selection rules:

    * ``active_prefix`` set: walk exactly that subdir. The directory
      may legitimately be absent (no prewarm yet) -- the function
      returns ``([], 0, 0, live)`` in that case.
    * ``active_prefix=None`` with one live prefix dir: walk it.
    * ``active_prefix=None`` with zero live prefix dirs: empty.
    * ``active_prefix=None`` with two or more live prefix dirs: refuse
      to claim residency -- return ``([], 0, 0, live)`` so the caller
      sees the ambiguity in ``live_prefix_dirs`` and exposes it in the
      row. The aggregate-union path is intentionally not exposed: it
      is what caused the stale-prefix masking the v6 plan calls out.

    Skips ``.{prefix}.deleting-{nonce}`` background-removal sentinel
    directories: a partition file there is being torn down by an
    in-flight invalidation and is not a stable L2 entry.
    """
    p = Path(l2_dir)
    v1 = p / V1_SUBDIR
    if not v1.is_dir():
        return [], 0, 0, []

    live_prefix_dirs = sorted(
        child.name
        for child in v1.iterdir()
        if child.is_dir()
        and not (child.name.startswith(".") and ".deleting-" in child.name)
    )

    if active_prefix is not None:
        target = active_prefix
    elif len(live_prefix_dirs) == 1:
        target = live_prefix_dirs[0]
    else:
        # Zero live (nothing on disk yet) or 2+ live (ambiguous): refuse
        # to claim residency; the caller surfaces ``live_prefix_dirs``.
        return [], 0, 0, live_prefix_dirs

    pfx = v1 / target
    if not pfx.is_dir():
        # ``active_prefix`` was passed but the directory is not yet
        # populated -- consistent with "owned partitions are missing".
        return [], 0, 0, live_prefix_dirs

    ids: set[int] = set()
    file_count = 0
    apparent_total = 0
    for fp in pfx.iterdir():
        if not fp.is_file():
            continue
        pid = parse_partition_id(fp.name)
        if pid is None:
            continue
        try:
            st = fp.stat()
        except OSError:
            continue
        ids.add(pid)
        file_count += 1
        apparent_total += int(st.st_size)
    return sorted(ids), file_count, apparent_total, live_prefix_dirs


def compute_l2_residency(
    *,
    actor_id: int,
    label: str,
    owned_partitions: Sequence[int],
    l2_dir: Optional[str],
    l1_size_bytes: int = L1_SIZE_UNKNOWN,
    active_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the per-actor residency dict by walking ``l2_dir``.

    Pure function (modulo filesystem reads); safe to call without a
    live Ray session. When ``l2_dir`` is ``None`` the row reports zero
    L2 files / bytes and every owned partition lands in ``missing`` --
    appropriate for moka / no-cache scenarios that have no L2 tier.

    Pass ``active_prefix`` when the caller knows the
    ``sanitize(dataset+index)`` subdirectory for this session. When
    omitted, the walk uses the single live prefix under ``v1/`` if
    exactly one exists; if multiple coexist, ``in_l2`` is reported
    empty (residency claim refused) and ``l2_prefix_dirs`` lists them
    so the operator can investigate.
    """
    owned_sorted = sorted({int(p) for p in owned_partitions})
    owned_set = set(owned_sorted)

    t0 = time.time()
    if l2_dir is None:
        in_l2_all: List[int] = []
        l2_file_count = 0
        l2_size_bytes_total = 0
        l2_prefix_dirs: List[str] = []
    else:
        in_l2_all, l2_file_count, l2_size_bytes_total, l2_prefix_dirs = (
            walk_l2_partition_ids(l2_dir, active_prefix=active_prefix)
        )

    # Owned partitions confirmed on disk vs. missing from disk. Files
    # on disk for partitions outside the owned set are not reported
    # individually (they would be a benchmark bug -- the driver's
    # round-robin assignment is the source of truth -- and are caught
    # upstream by the file-count vs. owned_count comparison).
    in_l2_owned = [pid for pid in in_l2_all if pid in owned_set]
    missing = [pid for pid in owned_sorted if pid not in set(in_l2_all)]

    return {
        "actor_id": int(actor_id),
        "label": label,
        "owned_count": len(owned_sorted),
        "in_l2": in_l2_owned,
        "missing": missing,
        "l2_size_bytes_total": int(l2_size_bytes_total),
        "l2_file_count": int(l2_file_count),
        "l2_prefix_dirs": l2_prefix_dirs,
        "l1_size_bytes_at_probe": int(l1_size_bytes),
        "probe_duration_s": time.time() - t0,
    }


def run_l2_residency_check(
    actors: Optional[Sequence[Any]],
    partitions_for_actor: Sequence[Sequence[int]],
    nvme_dir: Optional[str] = None,
    label: str = "residency",
    log=print,
) -> List[Dict[str, Any]]:
    """Build a residency row per actor.

    Default path (``actors`` is not ``None``): the walk happens on the
    actor via ``HybridSearchActor.check_l2_residency.remote(...)``. The
    actor RPC is required for real-cluster topologies where the L2
    directories live on the actor node's local NVMe and are not
    reachable from the driver. The actor also fills
    ``l1_size_bytes_at_probe`` from its live session, so this single
    RPC carries both halves of the row.

    Fallback path (``actors`` is ``None``): the driver walks
    ``nvme_dir/actor-<i>`` directly and reports
    ``l1_size_bytes_at_probe = L1_SIZE_UNKNOWN`` (-1). Useful only for
    the standalone ``--no-attach`` CLI mode on a cluster that has
    already torn down, on a host where the L2 dirs are still mounted.
    """
    log(f"\n=== L2 residency check: {label} ===")
    n_actors = len(partitions_for_actor)

    if actors is not None and len(actors) != n_actors:
        raise ValueError(
            f"actors ({len(actors)}) does not match "
            f"partitions_for_actor ({n_actors})"
        )

    def _actor_l2(i: int) -> Optional[str]:
        return os.path.join(nvme_dir, f"actor-{i}") if nvme_dir is not None else None

    if actors is not None:
        # Lazy ray import: only the actor-RPC path needs it. The CLI's
        # --no-attach mode and the library's actors=None fallback work
        # without ray installed.
        import ray  # noqa: PLC0415

        futures = [
            actors[i].check_l2_residency.remote(
                l2_dir=_actor_l2(i),
                owned_partitions=[int(p) for p in partitions_for_actor[i]],
                label=label,
            )
            for i in range(n_actors)
        ]
        results: List[Dict[str, Any]] = list(ray.get(futures))
    else:
        results = [
            compute_l2_residency(
                actor_id=i,
                label=label,
                owned_partitions=partitions_for_actor[i],
                l2_dir=_actor_l2(i),
                l1_size_bytes=L1_SIZE_UNKNOWN,
            )
            for i in range(n_actors)
        ]

    total_owned = sum(r["owned_count"] for r in results)
    total_in_l2 = sum(len(r["in_l2"]) for r in results)
    total_missing = sum(len(r["missing"]) for r in results)
    total_l2_bytes = sum(int(r["l2_size_bytes_total"]) for r in results)
    log(
        f"  aggregate: actors={n_actors} owned={total_owned} "
        f"in_l2={total_in_l2} missing={total_missing} "
        f"l2_bytes={format_bytes(total_l2_bytes)} "
        f"({(total_in_l2 / total_owned * 100) if total_owned else 0.0:.1f}% on disk)"
    )

    for r in results:
        l1 = r["l1_size_bytes_at_probe"]
        l1_str = "?" if l1 == L1_SIZE_UNKNOWN else format_bytes(l1)
        log(
            f"  actor-{r['actor_id']:<3} owned={r['owned_count']:<5} "
            f"in_l2={len(r['in_l2']):<5} missing={len(r['missing']):<5} "
            f"l2_files={r['l2_file_count']:<5} "
            f"l2_bytes={format_bytes(r['l2_size_bytes_total'])} "
            f"l1_bytes={l1_str} "
            f"probe={r['probe_duration_s']:5.2f}s"
        )
    return results


def write_residency_jsonl(
    results: Sequence[Dict[str, Any]],
    out_path: str,
    label: Optional[str] = None,
) -> None:
    """Append one JSON line per actor to ``out_path``.

    The row already carries its ``label`` field (set by
    ``compute_l2_residency``); the ``label`` parameter is kept for
    callers that want to override it without re-running the probe and
    is otherwise a no-op when it matches what's already in the row.
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "a") as f:
        for r in results:
            payload = dict(r)
            if label is not None:
                payload["label"] = label
            f.write(json.dumps(payload) + "\n")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--ray-address",
        type=str,
        default=os.environ.get("RAY_ADDRESS", "auto"),
        help="Ray cluster address; 'auto' attaches to the running cluster.",
    )
    p.add_argument(
        "--num-actors",
        type=int,
        required=True,
        help="Number of actors to resolve (must match the driver's --num-actors).",
    )
    p.add_argument(
        "--num-partitions",
        type=int,
        required=True,
        help=(
            "Total partitions in the IVF index; used to reconstruct each "
            "actor's round-robin slice (id %% num_actors)."
        ),
    )
    p.add_argument(
        "--nvme-dir",
        type=str,
        default=None,
        help=(
            "Parent of per-actor L2 subdirs. Pass for hybrid scenarios so "
            "the L2 footprint shows up in the per-actor summary."
        ),
    )
    p.add_argument(
        "--actor-name-prefix",
        type=str,
        default="hybrid-search-actor",
        help="Prefix for named actors (the driver uses 'hybrid-search-actor').",
    )
    p.add_argument(
        "--namespace",
        type=str,
        default=DEFAULT_NAMESPACE,
        help=(
            "Ray namespace the driver's named actors live in. Must match "
            f"the driver's namespace (default {DEFAULT_NAMESPACE!r}); "
            "ray.get_actor() only resolves actors in the same namespace."
        ),
    )
    p.add_argument(
        "--label",
        type=str,
        default="adhoc",
        help="Tag attached to the printed header and the JSONL output.",
    )
    p.add_argument(
        "--out",
        type=str,
        default=None,
        help="Optional JSONL path; one line per actor with the full residency dict.",
    )
    p.add_argument(
        "--no-attach",
        action="store_true",
        help=(
            "Skip Ray attachment and report only the on-disk inventory; "
            "l1_size_bytes_at_probe is reported as -1."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    partitions_for_actor = [
        list(range(i, args.num_partitions, args.num_actors))
        for i in range(args.num_actors)
    ]

    actors: Optional[List[Any]] = None
    if not args.no_attach:
        # Lazy ray import: --no-attach and --help must work in
        # environments without ray installed.
        try:
            import ray  # noqa: PLC0415
        except ImportError as e:
            print(
                f"[check] ray import failed ({e}); pass --no-attach for an "
                "L2-walk-only report (l1_size_bytes_at_probe reported as -1).",
                file=sys.stderr,
            )
            return 2
        ray.init(
            address=args.ray_address,
            namespace=args.namespace,
            ignore_reinit_error=True,
        )
        actors = []
        for i in range(args.num_actors):
            name = f"{args.actor_name_prefix}-{i}"
            try:
                actors.append(ray.get_actor(name, namespace=args.namespace))
            except ValueError as e:
                print(
                    f"[check] failed to resolve actor {name!r} in namespace "
                    f"{args.namespace!r}: {e}; is the bench driver still running "
                    "with these actors alive, and does its namespace match?",
                    file=sys.stderr,
                )
                return 2

    results = run_l2_residency_check(
        actors=actors,
        partitions_for_actor=partitions_for_actor,
        nvme_dir=args.nvme_dir,
        label=args.label,
    )

    if args.out is not None:
        write_residency_jsonl(results, args.out, args.label)
        print(f"[check] wrote {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
