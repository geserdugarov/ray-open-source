import atexit
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import numpy as np

from ray._common.retry import call_with_retry
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)


# Cache of per-worker hybrid-cache Sessions, keyed by the worker-local
# resolved L2 directory. Populated lazily inside read tasks (driver and
# workers are different processes, so the driver's dict is empty in
# workers — that's intentional).
_WORKER_HYBRID_SESSIONS: Dict[str, Any] = {}
_WORKER_ATEXIT_REGISTERED = False


class LanceDatasource(Datasource):
    """Lance datasource, for reading Lance dataset."""

    # Errors to retry when reading Lance fragments.
    READ_FRAGMENTS_ERRORS_TO_RETRY = ["LanceError(IO)"]
    # Maximum number of attempts to read Lance fragments.
    READ_FRAGMENTS_MAX_ATTEMPTS = 10
    # Maximum backoff seconds between attempts to read Lance fragments.
    READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS = 32

    def __init__(
        self,
        uri: str,
        version: Optional[Union[int, str]] = None,
        columns: Optional[List[str]] = None,
        filter: Optional[str] = None,
        storage_options: Optional[Dict[str, str]] = None,
        scanner_options: Optional[Dict[str, Any]] = None,
        hybrid_cache_l1_bytes: Optional[int] = None,
        hybrid_cache_l2_dir: Optional[str] = None,
        hybrid_cache_l2_bytes: Optional[int] = None,
        hybrid_cache_codecless_bytes: Optional[int] = None,
        hybrid_cache_l2_block_size_bytes: Optional[int] = None,
    ):
        _check_import(self, module="lance", package="pylance")

        import lance

        self.uri = uri
        self.version = version
        self.scanner_options = scanner_options or {}
        if columns is not None:
            self.scanner_options["columns"] = columns
        if filter is not None:
            self.scanner_options["filter"] = filter
        self.storage_options = storage_options
        self.hybrid_cache_config = _validate_hybrid_cache_config(
            l1_bytes=hybrid_cache_l1_bytes,
            l2_dir=hybrid_cache_l2_dir,
            l2_bytes=hybrid_cache_l2_bytes,
            codecless_bytes=hybrid_cache_codecless_bytes,
            l2_block_size_bytes=hybrid_cache_l2_block_size_bytes,
        )
        if self.hybrid_cache_config is not None:
            session_cls = getattr(lance, "Session", None)
            if not hasattr(session_cls, "with_hybrid_cache"):
                raise RuntimeError(
                    "hybrid_cache_* requires a pylance build that exposes "
                    "`lance.Session.with_hybrid_cache`; installed pylance "
                    f"{getattr(lance, '__version__', 'unknown')} does not. "
                    "Upgrade to a pylance release with hybrid-cache bindings."
                )
            if self.hybrid_cache_config.get("advanced") and not hasattr(
                session_cls, "with_hybrid_cache_advanced"
            ):
                raise RuntimeError(
                    "hybrid_cache_codecless_bytes / hybrid_cache_l2_block_size_bytes "
                    "require a pylance build that exposes "
                    "`lance.Session.with_hybrid_cache_advanced`; installed "
                    f"pylance {getattr(lance, '__version__', 'unknown')} does "
                    "not. Upgrade to a pylance release with the deferred-codec "
                    "hybrid-cache bindings."
                )
        # Driver opens with the default session: it only needs metadata
        # (fragments, schema). The hybrid cache is per-worker — building
        # one on the driver would either contend on the L2 lock with
        # workers or never see any traffic, neither of which is useful.
        self.lance_ds = lance.dataset(
            uri=uri, version=version, storage_options=storage_options
        )

        match = []
        match.extend(self.READ_FRAGMENTS_ERRORS_TO_RETRY)
        match.extend(DataContext.get_current().retried_io_errors)
        self._retry_params = {
            "description": "read lance fragments",
            "match": match,
            "max_attempts": self.READ_FRAGMENTS_MAX_ATTEMPTS,
            "max_backoff_s": self.READ_FRAGMENTS_RETRY_MAX_BACKOFF_SECONDS,
        }

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        read_tasks = []
        ds_fragments = self.scanner_options.get("fragments")
        if ds_fragments is None:
            ds_fragments = self.lance_ds.get_fragments()

        # Pin workers to the snapshot the driver planned against. Passing
        # the caller's `self.version` (which may be None or a movable tag)
        # would let workers race with concurrent commits and read a
        # different manifest than the one we computed `ds_fragments` from.
        open_dataset_kwargs = {
            "uri": self.uri,
            "version": self.lance_ds.version,
            "storage_options": self.storage_options,
        }

        for fragments in np.array_split(ds_fragments, parallelism):
            if len(fragments) <= 0:
                continue

            fragment_ids = [f.metadata.id for f in fragments]
            num_rows = sum(f.count_rows() for f in fragments)
            input_files = [
                data_file.path() for f in fragments for data_file in f.data_files()
            ]

            # TODO(chengsu): Take column projection into consideration for schema.
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                input_files=input_files,
                exec_stats=None,
            )
            scanner_options = self.scanner_options
            # When the worker reopens the dataset with a per-worker
            # hybrid-cache session, the driver's lance_ds is unused on
            # the worker — skip the pickle to save bandwidth.
            lance_ds = None if self.hybrid_cache_config else self.lance_ds
            retry_params = self._retry_params
            hybrid_cache_config = self.hybrid_cache_config

            read_task = ReadTask(
                lambda f=fragment_ids: _read_fragments_with_retry(
                    f,
                    lance_ds,
                    scanner_options,
                    retry_params,
                    open_dataset_kwargs,
                    hybrid_cache_config,
                ),
                metadata,
                schema=fragments[0].schema,
                per_task_row_limit=per_task_row_limit,
            )
            read_tasks.append(read_task)
        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(chengsu): Add memory size estimation to improve auto-tune of parallelism.
        return None


def _validate_hybrid_cache_config(
    l1_bytes: Optional[int],
    l2_dir: Optional[str],
    l2_bytes: Optional[int],
    codecless_bytes: Optional[int] = None,
    l2_block_size_bytes: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Reject partial hybrid-cache configuration at the driver. Returning
    None means "use the default in-memory session per worker".

    When ``codecless_bytes`` (and/or ``l2_block_size_bytes``) is set, the
    config switches to advanced mode: ``l1_bytes`` becomes the foyer DRAM
    tier (codec-bearing entries) specifically, ``codecless_bytes`` sizes
    the embedded Moka for codec-less entries, and the codec only runs on
    the L1↔L2 boundary in the foyer path. Otherwise lance derives both
    DRAM budgets from a single ``l1_bytes`` via its default 90/10 split
    (foyer L1 gets the bulk; codec-less Moka gets the 10% reserve).
    """
    provided = [
        ("hybrid_cache_l1_bytes", l1_bytes),
        ("hybrid_cache_l2_dir", l2_dir),
        ("hybrid_cache_l2_bytes", l2_bytes),
    ]
    set_names = [name for name, value in provided if value is not None]
    advanced_only_set = (
        codecless_bytes is not None or l2_block_size_bytes is not None
    )
    if not set_names and not advanced_only_set:
        return None
    if len(set_names) != len(provided):
        missing = [name for name, value in provided if value is None]
        raise ValueError(
            "hybrid cache requires all of hybrid_cache_l1_bytes, "
            f"hybrid_cache_l2_dir, hybrid_cache_l2_bytes — missing: {missing}"
        )
    config: Dict[str, Any] = {
        "l1_bytes": int(l1_bytes),
        "l2_dir_template": str(l2_dir),
        "l2_bytes": int(l2_bytes),
    }
    if not advanced_only_set:
        return config
    # Advanced mode: independent foyer L1 / codec-less Moka sizing, with
    # an optional L2 block size override. ``l1_bytes`` here is the foyer
    # DRAM tier; the codec-less Moka gets ``codecless_bytes`` separately.
    # ``with_hybrid_cache_advanced`` makes ``codecless_capacity_bytes``
    # required, so an l2-block-size override alone is not enough — either
    # both knobs are present or we stay on the convenience path.
    if codecless_bytes is None:
        raise ValueError(
            "hybrid_cache_l2_block_size_bytes requires "
            "hybrid_cache_codecless_bytes (advanced hybrid-cache mode); "
            "set both or unset both"
        )
    if int(codecless_bytes) <= 0:
        raise ValueError(
            "hybrid_cache_codecless_bytes must be > 0; "
            f"got {codecless_bytes}"
        )
    config["codecless_bytes"] = int(codecless_bytes)
    if l2_block_size_bytes is not None:
        if int(l2_block_size_bytes) <= 0:
            raise ValueError(
                "hybrid_cache_l2_block_size_bytes must be > 0; "
                f"got {l2_block_size_bytes}"
            )
        config["l2_block_size_bytes"] = int(l2_block_size_bytes)
    config["advanced"] = True
    return config


def _resolve_worker_l2_dir(l2_dir_template: str) -> str:
    """Pick a worker-local L2 subdirectory.

    Lance's HybridCacheBackend takes an exclusive flock on the L2
    directory, so two workers on the same node must not point at the
    same path. We suffix with the Ray worker id (stable across tasks on
    the same worker) and the OS pid (disambiguates if a worker forks).
    """
    try:
        import ray

        worker_id = ray.get_runtime_context().get_worker_id()
    except Exception:
        # Outside a Ray task (e.g. local test driver), fall back to pid
        # only — the directory is still unique per process.
        worker_id = "local"
    return os.path.join(l2_dir_template, f"worker-{worker_id}-{os.getpid()}")


def _close_worker_hybrid_sessions() -> None:
    """atexit hook: flush every cached hybrid session before the worker
    process exits. foyer runs WriteOnEviction, so without this hot DRAM
    entries never reach NVMe and a restart sees an empty L2."""
    for session in _WORKER_HYBRID_SESSIONS.values():
        try:
            session.close()
        except Exception as e:  # noqa: BLE001
            logger.warning("failed to close hybrid cache session: %s", e)
    _WORKER_HYBRID_SESSIONS.clear()


def _get_or_create_worker_session(hybrid_cache_config: Dict[str, Any]):
    """Build (or reuse) a per-worker hybrid-cache Session. Cached
    process-locally so multiple read tasks on the same worker share the
    same cache."""
    import lance

    l2_dir = _resolve_worker_l2_dir(hybrid_cache_config["l2_dir_template"])
    cached = _WORKER_HYBRID_SESSIONS.get(l2_dir)
    if cached is not None:
        return cached

    os.makedirs(l2_dir, exist_ok=True)
    if hybrid_cache_config.get("advanced"):
        # Defer the codec to the foyer L1/L2 boundary: foyer L1 holds
        # codec-bearing entries decoded, the embedded Moka holds
        # codec-less entries directly. The codec only runs on L2 promote
        # (encoded -> decoded) and L1 evict (decoded -> encoded).
        kwargs: Dict[str, Any] = dict(
            foyer_l1_capacity_bytes=hybrid_cache_config["l1_bytes"],
            codecless_capacity_bytes=hybrid_cache_config["codecless_bytes"],
            l2_dir=l2_dir,
            l2_capacity_bytes=hybrid_cache_config["l2_bytes"],
        )
        l2_block_size = hybrid_cache_config.get("l2_block_size_bytes")
        if l2_block_size is not None:
            kwargs["l2_block_size_bytes"] = l2_block_size
        session = lance.Session.with_hybrid_cache_advanced(**kwargs)
    else:
        session = lance.Session.with_hybrid_cache(
            l1_capacity_bytes=hybrid_cache_config["l1_bytes"],
            l2_dir=l2_dir,
            l2_capacity_bytes=hybrid_cache_config["l2_bytes"],
        )
    _WORKER_HYBRID_SESSIONS[l2_dir] = session

    global _WORKER_ATEXIT_REGISTERED
    if not _WORKER_ATEXIT_REGISTERED:
        atexit.register(_close_worker_hybrid_sessions)
        _WORKER_ATEXIT_REGISTERED = True
    return session


def _read_fragments_with_retry(
    fragment_ids,
    lance_ds,
    scanner_options,
    retry_params,
    open_dataset_kwargs,
    hybrid_cache_config,
) -> Iterator["pyarrow.Table"]:
    return call_with_retry(
        lambda: _read_fragments(
            fragment_ids,
            lance_ds,
            scanner_options,
            open_dataset_kwargs,
            hybrid_cache_config,
        ),
        **retry_params,
    )


def _read_fragments(
    fragment_ids,
    lance_ds,
    scanner_options,
    open_dataset_kwargs,
    hybrid_cache_config,
) -> Iterator["pyarrow.Table"]:
    """Read Lance fragments in batches.

    NOTE: Use fragment ids, instead of fragments as parameter, because pickling
    LanceFragment is expensive.
    """
    import lance
    import pyarrow

    if hybrid_cache_config is not None:
        session = _get_or_create_worker_session(hybrid_cache_config)
        lance_ds = lance.dataset(session=session, **open_dataset_kwargs)

    fragments = [lance_ds.get_fragment(id) for id in fragment_ids]
    scanner_options["fragments"] = fragments
    scanner = lance_ds.scanner(**scanner_options)
    for batch in scanner.to_reader():
        yield pyarrow.Table.from_batches([batch])
