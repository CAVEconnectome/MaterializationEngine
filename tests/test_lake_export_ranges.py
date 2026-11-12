"""lake_export ranges and publish on a frozen-database-shaped table in a throwaway PostGIS
database: id-range queries, streaming ranges into staging, publishing each range in one
commit, a failed attempt never reaching a lake, and the lakes optimized afterwards.
Needs the test PostGIS on localhost (as the rest of the suite); skipped without it."""

import uuid
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from materializationengine.workflows import deltalake_export as dx
from materializationengine.workflows.lake_export import optimize as opt
from materializationengine.workflows.lake_export import publish as pub
from materializationengine.workflows.lake_export import ranges, storage
from materializationengine.workflows.lake_export.sizing import SizingConfig, plan_optimize

TABLE, SEG = "synapses", "synapses__pcg"
ROWS = 20_000


@pytest.fixture(scope="module")
def frozen(mat_metadata):
    """A synapse table (ids with gaps, three PostGIS points) LEFT JOINed to a segmentation
    table that lacks some ids, so some root ids are null -- as in a frozen database."""
    url = make_url(mat_metadata["sql_uri"])
    base = f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}"
    name = f"lake_export_test_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(f"CREATE DATABASE {name}")
    except sa.exc.OperationalError as e:
        pytest.skip(f"no test Postgres: {e}")
    engine = sa.create_engine(f"{base}/{name}")
    try:
        with engine.begin() as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            conn.execute(f"""
                CREATE TABLE {TABLE} (id bigint PRIMARY KEY, created timestamp, deleted timestamp,
                    superceded_id bigint, valid boolean, pre_pt_position geometry(POINTZ),
                    ctr_pt_position geometry(POINTZ), post_pt_position geometry(POINTZ), size int);
                INSERT INTO {TABLE} SELECT g, now(), NULL, NULL, true,
                    ST_MakePoint(mod(g, 3000), mod(g, 2000), mod(g, 300)),
                    ST_MakePoint(mod(g, 3100), mod(g, 2100), mod(g, 310)),
                    ST_MakePoint(mod(g, 3200), mod(g, 2200), mod(g, 320)), mod(g * 7, 5000)
                FROM generate_series(1, {ROWS * 7 // 6}) g WHERE mod(g, 7) <> 0;
                CREATE INDEX ON {TABLE} USING gist (ctr_pt_position);
                CREATE TABLE {SEG} (id bigint PRIMARY KEY, pre_pt_supervoxel_id bigint, pre_pt_root_id bigint,
                    post_pt_supervoxel_id bigint, post_pt_root_id bigint);
                INSERT INTO {SEG} SELECT id, id * 10, mod(id * 7919, 4000), id * 10 + 1, mod(id * 104729, 4000)
                FROM {TABLE} WHERE mod(id, 11) <> 0;
                CREATE INDEX ON {SEG} (pre_pt_root_id);
                CREATE INDEX ON {SEG} (post_pt_root_id);
                ANALYZE;
            """)
        source = dx.TableSource(annotation_table=TABLE, segmentation_table=SEG)
        conn_str = f"{base}/{name}".replace("postgresql+psycopg2", "postgresql")
        specs = dx.discover_default_output_specs(source, engine)
        dx.apply_default_row_group_size(specs, 1000)
        rows = engine.execute(f"SELECT count(*) FROM {TABLE}").scalar()
        for spec in specs:
            spec.n_partitions = 8
        dx.resolve_all_bounds(specs, conn_str, TABLE)
        yield {
            "engine": engine, "conn_str": conn_str, "source": source, "rows": rows, "specs": specs,
            "columns": dx._resolve_select_columns(conn_str, source, dx._DEFAULT_DROP_COLUMNS),
            "geometry": dx._table_geometry_columns(conn_str, source, specs),
            "span": tuple(engine.execute(f"SELECT min(id), max(id) + 1 FROM {TABLE}").fetchone()),
        }
    finally:
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {name}")
        admin.dispose()


def id_bounds(frozen):
    return next(s.bounds for s in frozen["specs"] if s.partition_by == "id")


def export(frozen, out, lo, hi, i, flush_bytes=256 * 1024, stage=None):
    # small batches, so a range fills several buffers and flushes more than once
    return ranges.export_range(frozen["conn_str"], frozen["source"], frozen["columns"], frozen["specs"],
                               frozen["geometry"], out, i, lo, hi, flush_bytes, stage=stage, batch_bytes=64 * 1024)


def publish_all(frozen, out, result, job="job1"):
    for spec in frozen["specs"]:
        pub.publish(f"{out}/{spec.name}", result.stage, dx._spec_partition_by(spec), result.range_index, job)


class TestRangeQuery:
    def test_bounds_both_sides_of_the_join(self, frozen):
        lo, hi = frozen["span"][0], frozen["span"][0] + 500
        sql = ranges.range_query(frozen["source"], frozen["columns"], lo, hi)
        plan = "\n".join(r[0] for r in frozen["engine"].execute("EXPLAIN " + sql))
        # one condition on the annotation table and one on the segmentation table
        assert plan.count(f"(id >= {lo})") == 2, plan

    def test_no_segmentation_table(self, frozen):
        sql = ranges.range_query(dx.TableSource(annotation_table=TABLE), ["id", "size"], 5, 10)
        assert sql == f'SELECT "id", "size" FROM "{TABLE}" WHERE "{TABLE}".id >= 5 AND "{TABLE}".id < 10'

    def test_rejects_unsafe_identifiers(self, frozen):
        with pytest.raises(ValueError):
            ranges.range_query(frozen["source"], ['id"; DROP TABLE x; --'], 0, 1)


class TestCutRanges:
    def test_even_cover(self):
        r = ranges.cut_ranges(10, 110, 4)
        assert r == [(10, 35), (35, 60), (60, 85), (85, 110)]

    def test_cuts_just_after_partition_bounds(self):
        r = ranges.cut_ranges(0, 1000, 2, [100, 300, 500, 700])
        assert r == [(0, 501), (501, 1000)]

    def test_fewer_bounds_than_ranges_cuts_evenly(self):
        assert ranges.cut_ranges(0, 100, 4, [50]) == [(0, 25), (25, 50), (50, 75), (75, 100)]

    def test_degenerate(self):
        assert ranges.cut_ranges(0, 100, 1) == [(0, 100)]
        assert ranges.cut_ranges(5, 6, 4) == [(5, 6)]


class TestExportAndPublish:
    def test_every_range_one_commit_per_lake(self, frozen, tmp_path):
        from deltalake import DeltaTable

        out = str(tmp_path)
        parts = ranges.cut_ranges(*frozen["span"], 4, id_bounds(frozen))
        with ThreadPoolExecutor(4) as pool:
            results = list(pool.map(lambda a: export(frozen, out, a[1][0], a[1][1], a[0]), enumerate(parts)))
        assert sum(r.rows for r in results) == frozen["rows"]
        assert any(r.flushes > 1 for r in results)  # several staging commits per range
        for r in sorted(results, key=lambda r: r.range_index):
            publish_all(frozen, out, r)

        expected = frozen["engine"].execute(
            f"SELECT sum(a.id), count(*) FILTER (WHERE s.id IS NULL) FROM {TABLE} a LEFT JOIN {SEG} s USING (id)"
        ).fetchone()
        for spec in frozen["specs"]:
            uri = f"{out}/{spec.name}"
            table = DeltaTable(uri).to_pyarrow_table(columns=["id", "post_pt_root_id"])
            assert table.num_rows == frozen["rows"]
            assert table["id"].to_numpy().sum() == expected[0]
            assert table["post_pt_root_id"].null_count == expected[1]
            assert pub.published_ranges(uri, "job1") == {0, 1, 2, 3}
            assert pub.published_ranges(uri, "another-job") == set()
            assert pub.export_jobs(uri) == ["job1"]
            commits = [h for h in DeltaTable(uri).history() if h.get(pub.RANGE_KEY) is not None]
            assert len(commits) == 4
            # the staging logs are gone; the lake owns the staged files
            assert not [p for p in storage.list_paths(uri, "staging") if "/_delta_log/" in p]

    def test_failed_attempt_never_reaches_the_lake(self, frozen, tmp_path):
        from deltalake import DeltaTable

        out = str(tmp_path)
        lo, hi = ranges.cut_ranges(*frozen["span"], 2)[0]
        dead = export(frozen, out, lo, hi, 0)  # staged, then "lost" before publishing
        live = export(frozen, out, lo, hi, 0)
        assert dead.stage != live.stage
        publish_all(frozen, out, live)
        for spec in frozen["specs"]:
            uri = f"{out}/{spec.name}"
            assert DeltaTable(uri).to_pyarrow_table(columns=["id"]).num_rows == live.rows
            assert storage.list_paths(uri, dead.stage)
            opt.sweep(uri)
            assert storage.list_paths(uri, dead.stage) == []
            assert DeltaTable(uri).to_pyarrow_table(columns=["id"]).num_rows == live.rows

    def test_empty_range_publishes_nothing(self, frozen, tmp_path):
        from deltalake import DeltaTable

        out = str(tmp_path)
        result = export(frozen, out, -100, 0, 7)
        assert result.rows == 0 and result.flushes == 0
        publish_all(frozen, out, result)
        assert not DeltaTable.is_deltatable(f"{out}/id")

    def test_writer_failure_is_raised(self, frozen, tmp_path, monkeypatch):
        def boom(*a, **k):
            raise OSError("bucket gone")

        monkeypatch.setattr(dx, "_flush_buffer", boom)
        with pytest.raises(OSError, match="bucket gone"):
            export(frozen, str(tmp_path), *frozen["span"], 0)


class TestOptimizeExportedLakes:
    def test_optimize_every_lake_by_partition_groups(self, frozen, tmp_path):
        from deltalake import DeltaTable

        out = str(tmp_path)
        for i, (lo, hi) in enumerate(ranges.cut_ranges(*frozen["span"], 3, id_bounds(frozen))):
            publish_all(frozen, out, export(frozen, out, lo, hi, i))
        cfg = SizingConfig(optimize_group_bytes=64 * 1024, whole_lake_max_bytes=0)
        for spec in frozen["specs"]:
            uri = f"{out}/{spec.name}"
            column = (dx._spec_partition_by(spec) or [None])[0]
            stats = opt.partition_stats(uri, column)
            tasks = plan_optimize(stats, partitioned=column is not None, cfg=cfg)
            with ThreadPoolExecutor(4) as pool:
                list(pool.map(lambda t: opt.optimize_partitions(
                    uri, spec, column, None if t.values is None else list(t.values)), tasks))
            after = opt.partition_stats(uri, column)
            assert {s.value for s in after} == {s.value for s in stats}
            assert DeltaTable(uri).to_pyarrow_table(columns=["id"]).num_rows == frozen["rows"]
            # no staged file is left once every partition has been rewritten
            assert not [p for p in storage.list_paths(uri, "staging") if p.endswith(".parquet")]
            for file_uri in DeltaTable(uri).file_uris():
                meta = pq.ParquetFile(file_uri).metadata
                assert max(meta.row_group(i).num_rows for i in range(meta.num_row_groups)) <= 1000
                if spec.zorder_columns:
                    # ParquetFile, not read_table: no hive partitioning from the path
                    column = pq.ParquetFile(file_uri).read(columns=spec.zorder_columns)[0]
                    col = pc.drop_null(column).to_numpy()
                    assert (np.diff(col.astype(float)) >= 0).all()
