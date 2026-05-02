"""Verify per-actor IVF partition cache residency.

Doubles as a library (called by ``run_distributed_bench.py`` after the
prewarm phase and again after the measure phase) and a standalone tool
that can attach to a live Ray cluster whose ``hybrid-search-actor-<i>``
actors are still alive.

What this checks
----------------

For every owned partition on every actor, this calls Lance's
``prewarm_vector_cache(name, [partition], policy='moka_ram_cap',
ram_bytes=0)`` API. With ``ram_bytes=0`` the algorithm walks pass 1
(counting any DRAM-resident partition in ``skipped_existing``) and
short-circuits pass 2 immediately on ``ram_bytes_deep_size >= ram_bytes``
(i.e. ``0 >= 0``) without loading anything from storage. Each probe
therefore yields a clean DRAM yes/no without disturbing residency,
which makes the same call safe to repeat at "post-prewarm, before
measure" *and* at "post-measure, after queries" to observe how the
cache shifted under load.

Lance does not (yet) surface ``partition_is_in_l2`` through the Python
bindings, so per-partition L2 status is not directly observable. To
cover the "or disk" half of the verification, each actor's report
includes the L2 directory snapshot (file count + on-disk bytes) and
the session's aggregate ``index_cache_stats``. Cross-referencing
``len(not_in_ram)`` against the most recent prewarm's
``loaded_to_disk`` count tells the operator whether queries have
promoted L2 partitions into RAM or evicted RAM-resident ones back to
disk.

Output shape
------------

The library returns one dict per actor with keys
``actor_id``, ``in_ram`` (sorted partition ids), ``not_in_ram`` (sorted
partition ids), ``session_stats``, ``l2_dir``. The standalone CLI
prints a per-actor summary table to stdout and (when ``--out`` is set)
writes the full dict-per-actor as JSON Lines for downstream analysis.

Usage as a library
------------------

``run_residency_check(actors, partitions_for_actor, index_name,
nvme_dir=None, scenario=None, log=print, label="...")``

* ``actors`` — list of ``HybridSearchActor`` handles.
* ``partitions_for_actor`` — parallel list, ``partitions_for_actor[i]``
  is the set of partition ids actor ``i`` was prewarmed for. Driver
  has this from its round-robin assignment, but passing ``None`` falls
  back to whatever each actor remembers via
  ``_owned_partitions``.
* ``index_name`` — IVF index name (same one passed to prewarm).
* ``nvme_dir`` — parent of the per-actor L2 subdir; each actor reads
  ``<nvme_dir>/actor-<i>``. Pass ``None`` for non-hybrid scenarios.
* ``label`` — short string ("post-prewarm" / "post-measure") used in
  the printed header so a single bench log can carry both checks.

Standalone usage
----------------

When the bench's actors are still alive, attach to the cluster and run
the same check from another shell::

    python check_partition_residency.py \\
        --num-actors 2 --num-partitions 3000 \\
        --index-name vector_idx \\
        --nvme-dir /mnt/nvme/lance-l2/distributed \\
        --label adhoc

The actor names follow ``run_distributed_bench.py``'s convention
(``hybrid-search-actor-<i>``); override with ``--actor-name-prefix`` if
your driver renamed them.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import ray

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

# Importing the actor module is only needed for the standalone CLI;
# when the bench driver imports this module the actor handles already
# exist as ray remote refs so ``HybridSearchActor`` does not have to
# be in scope here.

try:
    from l2_inspect import format_bytes  # type: ignore[import-not-found]
except ImportError:
    def format_bytes(n: int) -> str:
        sign = "-" if n < 0 else ""
        n = abs(int(n))
        for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
            if n < 1024 or unit == "TiB":
                return f"{sign}{n:.1f} {unit}" if unit != "B" else f"{sign}{n} {unit}"
            n /= 1024
        return f"{sign}{n} TiB"


def _format_partition_range_sample(ids: Sequence[int], max_shown: int = 8) -> str:
    """Compact preview of an id list: ``[0, 2, 4, …, 2996, 2998]``."""
    n = len(ids)
    if n == 0:
        return "[]"
    if n <= max_shown:
        return "[" + ", ".join(str(i) for i in ids) + "]"
    head = ", ".join(str(i) for i in ids[: max_shown // 2])
    tail = ", ".join(str(i) for i in ids[-max_shown // 2 :])
    return f"[{head}, …, {tail}]"


def run_residency_check(
    actors: Sequence["ray.actor.ActorHandle"],
    partitions_for_actor: Optional[Sequence[Sequence[int]]],
    index_name: str,
    nvme_dir: Optional[str] = None,
    label: str = "residency",
    log=print,
) -> List[Dict[str, Any]]:
    """Poll every actor's ``check_partition_residency`` in parallel.

    The driver already knows the partition mapping (round-robin mod
    ``num_actors``), so we pass it explicitly to keep the probe range
    aligned with what each actor was *supposed* to own — if the
    benchmark sharded scenario was skipped (e.g. ``--scenario no-cache``
    with ``set_owned_partitions``), the actor still has the right
    ``_owned_partitions`` set and can be passed ``None``.
    """
    log(f"\n=== Partition residency check: {label} ===")
    futures = []
    for i, actor in enumerate(actors):
        owned: Optional[List[int]] = None
        if partitions_for_actor is not None:
            owned = [int(p) for p in partitions_for_actor[i]]
        actor_l2 = None
        if nvme_dir is not None:
            actor_l2 = os.path.join(nvme_dir, f"actor-{i}")
        futures.append(
            actor.check_partition_residency.remote(
                index_name=index_name,
                partition_ids=owned,
                l2_dir=actor_l2,
            )
        )
    results: List[Dict[str, Any]] = ray.get(futures)

    # Sort the per-actor partition lists so the printed preview and any
    # downstream JSON are stable across runs even if the actor visited
    # them in a different order.
    for r in results:
        r["in_ram"] = sorted(int(x) for x in r.get("in_ram", []))
        r["not_in_ram"] = sorted(int(x) for x in r.get("not_in_ram", []))

    n_actors = len(results)
    total_owned = sum(r["n_probed"] for r in results)
    total_in_ram = sum(len(r["in_ram"]) for r in results)
    total_not_in_ram = sum(len(r["not_in_ram"]) for r in results)
    log(
        f"  aggregate: actors={n_actors} probed={total_owned} "
        f"in_ram={total_in_ram} not_in_ram={total_not_in_ram} "
        f"({(total_in_ram / total_owned * 100) if total_owned else 0.0:.1f}% in RAM)"
    )

    for r in results:
        s = r.get("session_stats", {})
        bytes_str = (
            f"{int(s.get('size_bytes', 0)):,}" if s else "?"
        )
        l2 = r.get("l2_dir")
        if l2 and l2.get("exists"):
            l2_str = (
                f"  L2 dir: files={l2['file_count']} "
                f"disk={format_bytes(int(l2.get('disk_bytes', 0)))} "
                f"apparent={format_bytes(int(l2.get('apparent_bytes', 0)))}"
            )
        elif l2:
            l2_str = f"  L2 dir absent ({l2.get('path')})"
        else:
            l2_str = ""
        log(
            f"  actor-{r['actor_id']:<3} probed={r['n_probed']:<5} "
            f"in_ram={len(r['in_ram']):<5} not_in_ram={len(r['not_in_ram']):<5} "
            f"probe={r.get('probe_duration_s', 0.0):5.2f}s "
            f"cache_entries={s.get('num_entries', '?')} "
            f"cache_bytes={bytes_str}"
            + l2_str
        )
        log(
            f"    in_ram sample: {_format_partition_range_sample(r['in_ram'])}  "
            f"not_in_ram sample: {_format_partition_range_sample(r['not_in_ram'])}"
        )
    return results


def write_residency_jsonl(
    results: Sequence[Dict[str, Any]],
    out_path: str,
    label: str,
) -> None:
    """Append one JSON line per actor; reader can demux by ``label``."""
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "a") as f:
        for r in results:
            payload = dict(r)
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
        "--index-name",
        type=str,
        default="vector_idx",
        help="IVF index name (must match the driver's --index-name).",
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
    return p.parse_args()


def main() -> int:
    args = _parse_args()

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

    partitions_for_actor = [
        list(range(i, args.num_partitions, args.num_actors))
        for i in range(args.num_actors)
    ]

    results = run_residency_check(
        actors=actors,
        partitions_for_actor=partitions_for_actor,
        index_name=args.index_name,
        nvme_dir=args.nvme_dir,
        label=args.label,
    )

    if args.out is not None:
        write_residency_jsonl(results, args.out, args.label)
        print(f"[check] wrote {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
