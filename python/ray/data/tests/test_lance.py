import os

import lance
import pyarrow as pa
import pytest
from pkg_resources import parse_version
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.arrow_utils import get_pyarrow_version
from ray.data import Schema
from ray.data.datasource.path_util import _unwrap_protocol

_HYBRID_CACHE_AVAILABLE = hasattr(
    getattr(lance, "Session", None), "with_hybrid_cache"
)
_skip_without_hybrid_cache = pytest.mark.skipif(
    not _HYBRID_CACHE_AVAILABLE,
    reason="installed pylance does not expose lance.Session.with_hybrid_cache",
)
_HYBRID_CACHE_ADVANCED_AVAILABLE = hasattr(
    getattr(lance, "Session", None), "with_hybrid_cache_advanced"
)
_skip_without_hybrid_cache_advanced = pytest.mark.skipif(
    not _HYBRID_CACHE_ADVANCED_AVAILABLE,
    reason=(
        "installed pylance does not expose "
        "lance.Session.with_hybrid_cache_advanced (deferred-codec mode)"
    ),
)


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_space"),
            lazy_fixture("s3_path_with_space"),
        ),  # Path contains space.
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
    ],
)
@pytest.mark.parametrize(
    "batch_size",
    [None, 100],
)
def test_lance_read_basic(fs, data_path, batch_size):
    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    df1 = pa.table({"one": [2, 1, 3, 4, 6, 5], "two": ["b", "a", "c", "e", "g", "f"]})
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    lance.write_dataset(df1, path)

    ds_lance = lance.dataset(path)
    df2 = pa.table(
        {
            "one": [1, 2, 3, 4, 5, 6],
            "three": [4, 5, 8, 9, 12, 13],
            "four": ["u", "v", "w", "x", "y", "z"],
        }
    )
    ds_lance.merge(df2, "one")

    if batch_size is None:
        ds = ray.data.read_lance(path)
    else:
        ds = ray.data.read_lance(path, scanner_options={"batch_size": batch_size})

    # Test metadata-only ops.
    assert ds.count() == 6
    assert ds.schema() == Schema(
        pa.schema(
            {
                "one": pa.int64(),
                "two": pa.string(),
                "three": pa.int64(),
                "four": pa.string(),
            }
        )
    )

    # Test read.
    values = [[s["one"], s["two"]] for s in ds.take_all()]
    assert sorted(values) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]

    # Test column projection.
    ds = ray.data.read_lance(path, columns=["one"])
    values = [s["one"] for s in ds.take_all()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]
    assert ds.schema().names == ["one", "two", "three", "four"]


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_with_scanner_fragments(data_path):
    table = pa.table({"one": [2, 1, 3, 4, 6, 5], "two": ["b", "a", "c", "e", "g", "f"]})
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    dataset = lance.write_dataset(table, path, max_rows_per_file=2)

    fragments = dataset.get_fragments()
    ds = ray.data.read_lance(path, scanner_options={"fragments": fragments[:1]})
    values = [[s["one"], s["two"]] for s in ds.take_all()]
    assert values == [
        [2, "b"],
        [1, "a"],
    ]


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_many_files(data_path):
    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    num_rows = 1024
    data = pa.table({"id": pa.array(range(num_rows))})
    lance.write_dataset(data, path, max_rows_per_file=1)

    def test_lance():
        ds = ray.data.read_lance(path)
        return ds.count() == num_rows

    wait_for_condition(test_lance, timeout=10)


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_write(data_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema)

    ds = lance.dataset(data_path)
    ds.count_rows() == 10
    assert ds.schema.names == schema.names
    # The schema is platform-dependent, because numpy uses int32 on Windows.
    # So we observe the schema that is written and use that.
    schema = ds.schema

    tbl = ds.to_table()
    assert sorted(tbl["id"].to_pylist()) == list(range(10))
    assert set(tbl["str"].to_pylist()) == {f"str-{i}" for i in range(10)}

    ray.data.range(10).map(
        lambda x: {"id": x["id"] + 10, "str": f"str-{x['id'] + 10}"}
    ).write_lance(data_path, mode="append")

    ds = lance.dataset(data_path)
    ds.count_rows() == 20
    tbl = ds.to_table()
    assert sorted(tbl["id"].to_pylist()) == list(range(20))
    assert set(tbl["str"].to_pylist()) == {f"str-{i}" for i in range(20)}

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema, mode="overwrite")

    ds = lance.dataset(data_path)
    ds.count_rows() == 10
    assert ds.schema == schema


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_write_min_rows_per_file(data_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema, min_rows_per_file=100)

    ds = lance.dataset(data_path)
    assert ds.count_rows() == 10
    assert ds.schema == schema

    assert len(ds.get_fragments()) == 1


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_write_max_rows_per_file(data_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema, max_rows_per_file=1)

    ds = lance.dataset(data_path)
    assert ds.count_rows() == 10
    assert ds.schema == schema

    assert len(ds.get_fragments()) == 10


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_with_version(data_path):
    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    # Write an initial dataset (version 1)
    df1 = pa.table({"one": [2, 1, 3, 4, 6, 5], "two": ["b", "a", "c", "e", "g", "f"]})
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test_version.lance")
    lance.write_dataset(df1, path)

    # Merge new data to create a later version (latest)
    ds_lance = lance.dataset(path)
    # Get the initial version
    initial_version = ds_lance.version

    df2 = pa.table(
        {
            "one": [1, 2, 3, 4, 5, 6],
            "three": [4, 5, 8, 9, 12, 13],
            "four": ["u", "v", "w", "x", "y", "z"],
        }
    )
    ds_lance.merge(df2, "one")

    # Default read should return the latest (merged) dataset.
    ds_latest = ray.data.read_lance(path)

    assert ds_latest.count() == 6
    # Latest dataset should contain merged columns
    assert "three" in ds_latest.schema().names

    # Read the initial version and ensure it contains the original columns
    ds_prev = ray.data.read_lance(path, version=initial_version)
    assert ds_prev.count() == 6
    assert ds_prev.schema().names == ["one", "two"]

    values_prev = [[s["one"], s["two"]] for s in ds_prev.take_all()]
    assert sorted(values_prev) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(hybrid_cache_l1_bytes=4 * 1024 * 1024),
        dict(
            hybrid_cache_l1_bytes=4 * 1024 * 1024,
            hybrid_cache_l2_dir="/tmp/ignored",
        ),
        dict(hybrid_cache_l2_dir="/tmp/ignored"),
    ],
)
def test_lance_read_rejects_partial_hybrid_cache_args(kwargs, tmp_path):
    table = pa.table({"id": [1, 2, 3]})
    path = os.path.join(str(tmp_path), "test.lance")
    lance.write_dataset(table, path)
    with pytest.raises(ValueError, match="hybrid cache requires all of"):
        ray.data.read_lance(path, **kwargs)


@_skip_without_hybrid_cache
@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_with_hybrid_cache(data_path, tmp_path):
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    table = pa.table(
        {"one": list(range(64)), "two": [f"v{i}" for i in range(64)]}
    )
    lance.write_dataset(table, path, max_rows_per_file=8)

    l2_dir = tmp_path / "lance-cache"
    ds = ray.data.read_lance(
        path,
        hybrid_cache_l1_bytes=4 * 1024 * 1024,
        hybrid_cache_l2_dir=str(l2_dir),
        hybrid_cache_l2_bytes=1 << 30,
    )
    rows = ds.take_all()
    assert sorted([r["one"] for r in rows]) == list(range(64))
    assert sorted([r["two"] for r in rows]) == sorted(
        [f"v{i}" for i in range(64)]
    )

    # Each worker writes into its own ``worker-<id>-<pid>`` subdirectory
    # before opening the foyer device, so the only way these
    # subdirectories exist is if the per-worker session was actually
    # constructed and held the L2 lock.
    worker_dirs = [
        entry
        for entry in os.listdir(str(l2_dir))
        if entry.startswith("worker-")
    ]
    assert worker_dirs, (
        f"expected at least one worker-* subdirectory under {l2_dir}, "
        f"found: {os.listdir(str(l2_dir))}"
    )


@_skip_without_hybrid_cache
@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_hybrid_cache_pins_planned_version(data_path, tmp_path):
    """Workers must reopen the same manifest the driver planned against,
    even when ``version=None`` and a concurrent commit lands between
    planning and execution."""
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    table_v1 = pa.table({"id": list(range(8)), "tag": ["v1"] * 8})
    lance.write_dataset(table_v1, path, max_rows_per_file=4)
    planned_version = lance.dataset(path).version

    from ray.data._internal.datasource.lance_datasource import LanceDatasource

    ds = LanceDatasource(
        uri=path,
        hybrid_cache_l1_bytes=4 * 1024 * 1024,
        hybrid_cache_l2_dir=str(tmp_path / "lance-cache"),
        hybrid_cache_l2_bytes=1 << 30,
    )

    # Concurrent commit lands between planning and execution.
    lance.write_dataset(
        pa.table({"id": list(range(8)), "tag": ["v2"] * 8}),
        path,
        mode="overwrite",
    )
    assert lance.dataset(path).version > planned_version

    read_tasks = ds.get_read_tasks(parallelism=1)
    rows = []
    for task in read_tasks:
        for block in task():
            rows.extend(block.to_pylist())

    assert {r["tag"] for r in rows} == {"v1"}, (
        "workers resolved against the latest manifest instead of the "
        "snapshot the driver planned against"
    )


@pytest.mark.parametrize(
    "kwargs,match",
    [
        (
            dict(hybrid_cache_codecless_bytes=1 * 1024 * 1024),
            "hybrid cache requires all of",
        ),
        (
            dict(hybrid_cache_l2_block_size_bytes=4 * 1024 * 1024),
            "hybrid cache requires all of",
        ),
    ],
)
def test_lance_read_advanced_args_require_full_hybrid_config(
    kwargs, match, tmp_path
):
    """Setting an advanced-only knob without the base trio (l1/l2_dir/l2_bytes)
    is rejected at the driver — no point dispatching to the deferred-codec
    path with no L2 directory to write to."""
    table = pa.table({"id": [1, 2, 3]})
    path = os.path.join(str(tmp_path), "test.lance")
    lance.write_dataset(table, path)
    with pytest.raises(ValueError, match=match):
        ray.data.read_lance(path, **kwargs)


@pytest.mark.parametrize(
    "bad_value,field",
    [
        (0, "hybrid_cache_codecless_bytes"),
        (-1, "hybrid_cache_codecless_bytes"),
        (0, "hybrid_cache_l2_block_size_bytes"),
        (-1, "hybrid_cache_l2_block_size_bytes"),
    ],
)
def test_lance_read_rejects_nonpositive_advanced_args(bad_value, field, tmp_path):
    table = pa.table({"id": [1, 2, 3]})
    path = os.path.join(str(tmp_path), "test.lance")
    lance.write_dataset(table, path)
    kwargs = {
        "hybrid_cache_l1_bytes": 4 * 1024 * 1024,
        "hybrid_cache_l2_dir": str(tmp_path / "l2"),
        "hybrid_cache_l2_bytes": 1 << 30,
        field: bad_value,
    }
    if field == "hybrid_cache_l2_block_size_bytes":
        # The advanced path requires codecless_bytes too; supply a valid
        # one so we don't trip the prerequisite check before the >0 check.
        kwargs["hybrid_cache_codecless_bytes"] = 1 * 1024 * 1024
    with pytest.raises(ValueError, match=f"{field} must be > 0"):
        ray.data.read_lance(path, **kwargs)


def test_lance_read_l2_block_size_requires_codecless(tmp_path):
    """``with_hybrid_cache_advanced`` makes ``codecless_capacity_bytes`` a
    required positional. An l2-block-size override alone has no
    deferred-codec mode to dispatch to, so we reject it at the driver."""
    table = pa.table({"id": [1, 2, 3]})
    path = os.path.join(str(tmp_path), "test.lance")
    lance.write_dataset(table, path)
    with pytest.raises(
        ValueError,
        match="hybrid_cache_l2_block_size_bytes requires "
        "hybrid_cache_codecless_bytes",
    ):
        ray.data.read_lance(
            path,
            hybrid_cache_l1_bytes=4 * 1024 * 1024,
            hybrid_cache_l2_dir=str(tmp_path / "l2"),
            hybrid_cache_l2_bytes=1 << 30,
            hybrid_cache_l2_block_size_bytes=4 * 1024 * 1024,
        )


@_skip_without_hybrid_cache_advanced
@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_with_hybrid_cache_advanced(data_path, tmp_path):
    """Deferred-codec mode: foyer L1 (codec-bearing) and codec-less Moka are
    sized independently, with an L2 block size small enough to keep L2 capacity
    minimal for the test."""
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    table = pa.table(
        {"one": list(range(64)), "two": [f"v{i}" for i in range(64)]}
    )
    lance.write_dataset(table, path, max_rows_per_file=8)

    l2_dir = tmp_path / "lance-cache"
    ds = ray.data.read_lance(
        path,
        # Foyer L1 budget (codec-bearing entries) in advanced mode.
        hybrid_cache_l1_bytes=4 * 1024 * 1024,
        hybrid_cache_l2_dir=str(l2_dir),
        # 16 MiB L2 with a 4 MiB block size satisfies foyer's 4-block
        # minimum without requiring a 1 GiB allocation in tests.
        hybrid_cache_l2_bytes=16 * 1024 * 1024,
        hybrid_cache_l2_block_size_bytes=4 * 1024 * 1024,
        # Codec-less embedded Moka, sized independently from foyer L1.
        hybrid_cache_codecless_bytes=1 * 1024 * 1024,
    )
    rows = ds.take_all()
    assert sorted([r["one"] for r in rows]) == list(range(64))
    assert sorted([r["two"] for r in rows]) == sorted(
        [f"v{i}" for i in range(64)]
    )

    # Each worker writes into its own ``worker-<id>-<pid>`` subdirectory
    # before opening the foyer device — these only exist if the
    # ``with_hybrid_cache_advanced`` session was actually constructed
    # and held the L2 lock.
    worker_dirs = [
        entry
        for entry in os.listdir(str(l2_dir))
        if entry.startswith("worker-")
    ]
    assert worker_dirs, (
        f"expected at least one worker-* subdirectory under {l2_dir}, "
        f"found: {os.listdir(str(l2_dir))}"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
