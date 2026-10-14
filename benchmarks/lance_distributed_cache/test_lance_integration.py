"""Small integration check for the exact Lance cache API used by the benchmark."""

from __future__ import annotations

import gc
import inspect
import tempfile
import unittest
from pathlib import Path

try:
    import lance
    import numpy as np
    import pyarrow as pa

    _signature = inspect.signature(lance.Session.with_distributed_cache)
    _SUPPORTED = (
        "l1_partition_capacity_bytes" in _signature.parameters
        and "partition_ids"
        in inspect.signature(lance.LanceDataset.prewarm_index).parameters
        and hasattr(lance.Session, "invalidate_index_cache")
    )
    _SKIP_REASON = "installed pylance does not expose the Lance 12 cache API"
except Exception as error:  # An incompatible lance-namespace can fail during import.
    lance = None
    np = None
    pa = None
    _SUPPORTED = False
    _SKIP_REASON = f"pylance unavailable: {error!r}"


@unittest.skipUnless(_SUPPORTED, _SKIP_REASON)
class LanceDistributedCacheIntegrationTest(unittest.TestCase):
    def test_dataset_setup_is_idempotent_and_validates_shape(self):
        from benchmark_lib import DatasetSpec, ensure_dataset

        with tempfile.TemporaryDirectory(prefix="lance-cache-setup-") as temp_dir:
            dataset_uri = str(Path(temp_dir) / "dataset")
            spec = DatasetSpec(
                scale=320,
                dimension=16,
                num_partitions=4,
                num_bits=2,
            )
            ensure_dataset(
                spec,
                dataset_uri,
                storage_options={},
                batch_rows=128,
                log=lambda _message: None,
            )
            ensure_dataset(
                spec,
                dataset_uri,
                storage_options={},
                batch_rows=128,
                log=lambda _message: None,
            )
            self.assertEqual(lance.dataset(dataset_uri).count_rows(), spec.scale)

            mismatched = DatasetSpec(
                scale=spec.scale + 1,
                dimension=spec.dimension,
                num_partitions=spec.num_partitions,
                num_bits=spec.num_bits,
            )
            with self.assertRaisesRegex(RuntimeError, "expected 321"):
                ensure_dataset(
                    mismatched,
                    dataset_uri,
                    storage_options={},
                    log=lambda _message: None,
                )

    def test_strict_prewarm_restart_query_and_invalidate(self):
        from cache_inspect import invalidation_errors, snapshot_l2_dir

        with tempfile.TemporaryDirectory(prefix="lance-cache-integration-") as temp_dir:
            root = Path(temp_dir)
            vectors = (
                np.random.default_rng(42).standard_normal((512, 16)).astype(np.float32)
            )
            vector_array = pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.reshape(-1)), 16
            )
            dataset_uri = root / "dataset"
            dataset = lance.write_dataset(
                pa.table({"id": range(len(vectors)), "vector": vector_array}),
                dataset_uri,
                max_rows_per_file=256,
            )
            dataset = dataset.create_index(
                "vector",
                name="vector_idx",
                index_type="IVF_RQ",
                num_partitions=4,
                num_bits=8,
            )
            del dataset

            l2_dir = root / "l2"
            l2_dir.mkdir()
            session = lance.Session.with_distributed_cache(
                l2_dir,
                index_metadata_l1_capacity_bytes=1 << 20,
                l1_partition_capacity_bytes=None,
            )
            dataset = lance.dataset(dataset_uri, session=session)
            dataset.prewarm_index("vector_idx", partition_ids=[])
            first = snapshot_l2_dir(l2_dir)
            self.assertEqual(first["cache_entry_count"], 4)
            self.assertEqual(first["namespace_count"], 1)

            del dataset, session
            gc.collect()

            restarted_session = lance.Session.with_distributed_cache(
                l2_dir,
                index_metadata_l1_capacity_bytes=1 << 20,
                l1_partition_capacity_bytes=None,
            )
            restarted = lance.dataset(dataset_uri, session=restarted_session)
            restarted.prewarm_index("vector_idx", partition_ids=[])
            second = snapshot_l2_dir(l2_dir)
            self.assertEqual(second["cache_entry_count"], first["cache_entry_count"])

            result = restarted.to_table(
                columns=["id", "_distance"],
                nearest={
                    "column": "vector",
                    "q": vectors[0],
                    "k": 10,
                    "nprobes": 4,
                },
            )
            self.assertEqual(result.num_rows, 10)
            exact_ids = np.argsort(np.sum((vectors - vectors[0]) ** 2, axis=1))[:10]
            returned_ids = result["id"].to_numpy()
            recall = len(set(exact_ids) & set(returned_ids)) / 10
            self.assertGreaterEqual(recall, 0.5)
            uncached = lance.dataset(
                dataset_uri, session=lance.Session(index_cache_size_bytes=0)
            ).to_table(
                columns=["id", "_distance"],
                nearest={"column": "vector", "q": vectors[0], "k": 10, "nprobes": 4},
            )
            np.testing.assert_array_equal(returned_ids, uncached["id"].to_numpy())
            np.testing.assert_allclose(
                result["_distance"].to_numpy(), uncached["_distance"].to_numpy()
            )

            index_address = str(restarted.describe_indices()[0].segments[0].uuid)
            restarted_session.invalidate_index_cache(restarted.uri, index_address)
            invalidated = snapshot_l2_dir(l2_dir)
            self.assertEqual(invalidated["cache_entry_count"], 0)
            self.assertEqual(invalidation_errors(invalidated), [])


if __name__ == "__main__":
    unittest.main()
